import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import jsonschema
from jsonschema import validate
from common import (
    connect_to_postgres,
    cleanup_empty_directories,
    archive_and_delete,
    log_processing_statistics,
    extract_data,
    load_data,
    extract_actual_date,
    extract_actual_hour,
)
import psycopg2
from typing import Any, Dict, List
import getpass

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DATA_PATH = "raw_data"
PROCESSED_DATA_PATH = "processed_data"
ARCHIVED_DATA_PATH = "archived_data"
INVALID_RECORDS_TABLE = "data.invalid_products"
PRODUCTS_SCHEMA_FILE = "products_schema.json"


with open(PRODUCTS_SCHEMA_FILE, "r") as schema_file:
    PRODUCTS_SCHEMA = json.load(schema_file)


def log_invalid_product(
    connection: Any, product: Dict[str, Any], error_message: str, date: str, hour: str
) -> None:
    """
    Log invalid product information into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        product (Dict[str, Any]): The invalid product information.
        error_message (str): The error message describing the validation error.
        date (str): The date of the data.
        hour (str): The hour of the data.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO data.invalid_products (record_date, record_hour, sku, name, price, category, popularity, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """,
            (
                actual_date,
                actual_hour,
                product.get("sku"),
                product.get("name"),
                product.get("price"),
                product.get("category"),
                product.get("popularity"),
                error_message,
            ),
        )
    connection.commit()


def transform_and_validate_products(
    connection: Any, products_data: List[Dict[str, Any]], date: str, hour: str
) -> List[Dict[str, Any]]:
    """
    Transform and validate products against the schema.

    Args:
        connection (Any): The PostgreSQL connection.
        products_data (List[Dict[str, Any]]): List of product data.
        date (str): The date of the data.
        hour (str): The hour of the data.

    Returns:
        List[Dict[str, Any]]: List of valid products.
    """
    schema = PRODUCTS_SCHEMA

    valid_products = []

    # Validate each product record against the schema
    for product in products_data:
        try:
            # Convert 'price' to a number before validation
            product["price"] = float(product["price"])
            validate(instance=product, schema=schema)
            valid_products.append(product)
        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            logger.error(f"Validation error for product: {e}")
            # Log the invalid record to the database
            log_invalid_product(connection, product, str(e), date, hour)
            continue

    # Update last_change timestamp
    for product in valid_products:
        product["last_change"] = datetime.utcnow().isoformat()

    return valid_products


def log_processed_products(
    connection: Any,
    date: str,
    hour: str,
    skus: List[str],
    names: List[str],
    prices: List[float],
    categories: List[str],
    popularities: List[int],
) -> None:
    """
    Log processed products into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        date (datetime): The date of the data.
        hour (int): The hour of the data.
        skus (List[str]): List of SKUs.
        names (List[str]): List of product names.
        prices (List[float]): List of product prices.
        categories (List[str]): List of product categories.
        popularities (List[int]): List of product popularities.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for sku, name, price, category, popularity in zip(
            skus, names, prices, categories, popularities
        ):
            # Check if the record already exists
            cursor.execute(
                """
                SELECT COUNT(*) FROM data.products
                WHERE record_date = %s AND record_hour = %s AND sku = %s;
            """,
                (actual_date, actual_hour, sku),
            )

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute(
                    """
                    INSERT INTO data.products (sku, name, price, category, popularity, record_date, record_hour)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                    (sku, name, price, category, popularity, actual_date, actual_hour),
                )
            else:
                # Record already exists, log or handle accordingly
                logger.info(
                    f"Record for sku {sku} at {actual_date} {actual_hour} already exists."
                )
    connection.commit()


def process_hourly_data(
    connection: Any, date: str, hour: str, available_datasets: List[str]
) -> None:
    """
    Process hourly data for products.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the data.
        hour (int): The hour of the data.
        available_datasets (List[str]): List of available datasets.
    """
    dataset_paths = {
        dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
        for dataset in available_datasets
    }
    logger.debug("Dataset Paths:", dataset_paths)

    # Record the start time
    start_time = datetime.now()

    # Extract raw_data
    products_data = extract_data(dataset_paths.get("products.json.gz", ""))

    # Transform and validate raw_data
    transformed_products = transform_and_validate_products(
        connection, products_data, date, hour
    )

    # Load processed raw_data
    load_data(transformed_products, "products.json.gz", date, hour, PROCESSED_DATA_PATH)

    # Log processed products
    skus = [product["sku"] for product in transformed_products]
    names = [product["name"] for product in transformed_products]
    prices = [product["price"] for product in transformed_products]
    categories = [product["category"] for product in transformed_products]
    popularities = [product["popularity"] for product in transformed_products]
    log_processed_products(
        connection, date, hour, skus, names, prices, categories, popularities
    )

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(
        connection,
        date,
        hour,
        "products.json.gz",
        len(transformed_products),
        processing_time,
    )

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data(connection: Any) -> None:
    """
    Process all available raw_data for products.

    Args:
        connection (Any): The PostgreSQL connection.
    """
    try:
        date_folders = os.listdir(RAW_DATA_PATH)
        date_folders.sort()
        # Process all available raw_data
        for date_folder in date_folders:
            date_path = os.path.join(RAW_DATA_PATH, date_folder)

            # Get a sorted list of hour folders
            hour_folders = os.listdir(date_path)
            hour_folders.sort()

            for hour_folder in hour_folders:
                hour_path = os.path.join(date_path, hour_folder)

                available_datasets = [
                    filename
                    for filename in os.listdir(hour_path)
                    if filename.startswith("products")
                    and filename.endswith((".json", ".json.gz"))
                ]

                if available_datasets:
                    process_hourly_data(
                        connection, date_folder, hour_folder, available_datasets
                    )
                else:
                    logger.warning(f"No datasets found for {date_folder}/{hour_folder}")

        # Clean up empty directories in raw_data after processing
        cleanup_empty_directories(RAW_DATA_PATH)

    except (psycopg2.Error, Exception):
        logger.exception("An error occurred while processing data")


def main() -> None:
    """
    Main function to execute data processing for products.
    """
    # Get the current user who is executing the script
    current_user = getpass.getuser()
    # Print out the user information along with your existing log messages
    print(f"The script was executed by user: {current_user}")
    try:
        with connect_to_postgres() as connection:
            process_all_data(connection)
    except psycopg2.Error:
        logger.exception("An error occurred while processing data")
    except Exception:
        logger.exception("An error occurred")


if __name__ == "__main__":
    main()