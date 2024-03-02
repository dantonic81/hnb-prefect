import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import jsonschema
from jsonschema import validate
from common import (
    load_data,
    archive_and_delete,
    extract_actual_date,
    extract_actual_hour,
    connect_to_postgres,
    extract_data,
    log_processing_statistics,
    cleanup_empty_directories,
)
import psycopg2
import cProfile
from typing import Any, List, Dict, Tuple


load_dotenv()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

RAW_DATA_PATH = "rawdata"
PROCESSED_DATA_PATH = "processed_data"
ARCHIVED_DATA_PATH = "archived_data"
INVALID_RECORDS_TABLE = "data.invalid_transactions"
TRANSACTIONS_SCHEMA_FILE = "transactions_schema.json"


# Load the JSON schema once and store it in a variable
with open(TRANSACTIONS_SCHEMA_FILE, "r") as schema_file:
    TRANSACTIONS_SCHEMA = json.load(schema_file)


def is_existing_customer(connection: Any, customer_id: str) -> bool:
    """
    Check if the customer with the given ID exists in the customer dataset.

    Args:
        connection (Any): The PostgreSQL connection.
        customer_id (str): The customer ID.

    Returns:
        bool: True if the customer exists, False otherwise.
    """
    with connection.cursor() as cursor:
        # Check if the customer_id exists in the customer dataset
        cursor.execute(
            """
            SELECT COUNT(*) FROM data.customers
            WHERE id = %s;
        """,
            (customer_id,),
        )

        count = cursor.fetchone()[0]

    return count > 0


def is_existing_product(connection: Any, sku: str) -> bool:
    """
    Check if the product with the given SKU exists in the product dataset.

    Args:
        connection (Any): The PostgreSQL connection.
        sku (str): The product SKU.

    Returns:
        bool: True if the product exists, False otherwise.
    """
    with connection.cursor() as cursor:
        # Check if the sku exists in the product dataset
        cursor.execute(
            """
            SELECT COUNT(*) FROM data.products
            WHERE sku = %s;
        """,
            (sku,),
        )

        count = cursor.fetchone()[0]

    return count > 0


def are_valid_product_skus(connection: Any, products: List[Dict[str, Any]]) -> bool:
    """
    Check if the product SKUs in the list exist in the product dataset.

    Args:
        connection (Any): The PostgreSQL connection.
        products (List[Dict[str, Any]]): List of products with 'sku' key.

    Returns:
        bool: True if all product SKUs are valid, False otherwise.
    """
    for product in products:
        sku = product.get("sku")
        if not is_existing_product(connection, sku):
            return False
    return True


def is_valid_total_cost(products: List[Dict[str, Any]], total_cost: str) -> bool:
    """
    Check if the total cost matches the sum of individual product costs.

    Args:
        products (List[Dict[str, Any]]): List of products with 'price' and 'quantity' keys.
        total_cost (str): The provided total cost.

    Returns:
        bool: True if the total cost is valid, False otherwise.
    """
    calculated_total_cost = sum(
        float(product.get("price", 0)) * float(product.get("quanitity", 0))
        for product in products
    )
    return round(calculated_total_cost, 2) == round(float(total_cost), 2)


def bulk_insert_invalid_transactions(
    connection: Any, invalid_transactions: List[Tuple[Dict[str, Any], str, str, str]]
) -> None:
    """
    Bulk insert invalid transactions into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        invalid_transactions (List[Tuple[Dict[str, Any], str, str, str]]): List of tuples containing invalid transaction details.
    """
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO data.invalid_transactions (record_date, record_hour, transaction_id, customer_id, error_message)
            VALUES (%s, %s, %s, %s, %s);
        """,
            [
                (
                    extract_actual_date(date),
                    extract_actual_hour(hour),
                    t.get("transaction_id"),
                    t.get("customer_id"),
                    error_message,
                )
                for t, error_message, date, hour in invalid_transactions
            ],
        )
    connection.commit()


def transform_and_validate_transactions(
    connection: Any, transactions_data: List[Dict[str, Any]], date: str, hour: str
) -> List[Dict[str, Any]]:
    """
    Transform and validate transactions data.

    Args:
        connection (Any): The PostgreSQL connection.
        transactions_data (List[Dict[str, Any]]): List of transaction records.
        date (str): The date of the transactions.
        hour (str): The hour of the transactions.

    Returns:
        List[Dict[str, Any]]: List of valid transactions.
    """
    schema = TRANSACTIONS_SCHEMA

    valid_transactions = []
    invalid_transactions = []
    unique_transaction_ids = set()

    # Validate each transaction record against the schema
    for transaction in transactions_data:
        try:
            validate(instance=transaction, schema=schema)

            # Check uniqueness of transaction_id
            transaction_id = transaction.get("transaction_id")
            if transaction_id in unique_transaction_ids:
                # Log or handle duplicate transaction_id
                logger.debug(f"Duplicate transaction_id found: {transaction_id}")
                invalid_transactions.append(
                    (transaction, "Duplicate transaction_id", date, hour)
                )
                continue

            unique_transaction_ids.add(transaction_id)

            # Check if customer_id refers to an existing customer
            customer_id = transaction.get("customer_id")
            if not is_existing_customer(connection, customer_id):
                # Log or handle invalid customer_id
                logger.debug(f"Invalid customer_id found: {customer_id}")
                invalid_transactions.append(
                    (transaction, f"Invalid customer_id: {customer_id}", date, hour)
                )
                continue

            # Check if product skus correspond to existing products
            if not are_valid_product_skus(
                connection, transaction.get("purchases", {}).get("products", [])
            ):
                # Log or handle invalid product skus
                logger.debug(
                    f"Invalid product skus found in transaction_id: {transaction_id}"
                )
                invalid_transactions.append(
                    (transaction, "Invalid product skus", date, hour)
                )
                continue

            # Check if total_cost matches the sum of individual product costs
            purchases = transaction.get("purchases", {})
            if not is_valid_total_cost(
                purchases.get("products", []), purchases.get("total_cost")
            ):
                # Log or handle invalid total_cost
                logger.debug(
                    f"Invalid total_cost found in transaction_id: {transaction_id}"
                )
                invalid_transactions.append(
                    (transaction, "Invalid total_cost", date, hour)
                )
                continue

            valid_transactions.append(transaction)

        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            logger.error(f"Validation error for transaction: {e}")
            invalid_transactions.append((transaction, str(e), date, hour))
            continue

    # Bulk insert invalid transactions
    bulk_insert_invalid_transactions(connection, invalid_transactions)

    return valid_transactions


def log_processed_transactions(
    connection: Any, date: str, hour: str, transactions: List[Dict[str, Any]]
) -> None:
    """
    Log processed transactions to the database.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the transactions.
        hour (str): The hour of the transactions.
        transactions (List[Dict[str, Any]]): List of processed transactions.
    """
    with connection.cursor() as cursor:
        for transaction in transactions:
            transaction_id = transaction.get("transaction_id")
            record_date = extract_actual_date(date)
            record_hour = extract_actual_hour(hour)

            # Check if the transaction_id already exists
            cursor.execute(
                """
                SELECT COUNT(*) FROM data.transactions
                WHERE transaction_id = %s;
            """,
                (transaction_id,),
            )

            if cursor.fetchone()[0] > 0:
                logger.debug(f"Duplicate transaction_id found: {transaction_id}")
                # Log or handle duplicate transaction_id
                continue

            # Insert transaction data
            cursor.execute(
                """
                INSERT INTO data.transactions (transaction_id, transaction_time, customer_id, record_date, record_hour)
                VALUES (%s, %s, %s, %s, %s);
            """,
                (
                    transaction_id,
                    transaction.get("transaction_time"),
                    transaction.get("customer_id"),
                    record_date,
                    record_hour,
                ),
            )

            # Insert delivery address data
            delivery_address = transaction.get("delivery_address")
            if delivery_address:
                cursor.execute(
                    """
                    INSERT INTO data.delivery_addresses (transaction_id, address, postcode, city, country)
                    VALUES (%s, %s, %s, %s, %s);
                """,
                    (
                        transaction_id,
                        delivery_address.get("address"),
                        delivery_address.get("postcode"),
                        delivery_address.get("city"),
                        delivery_address.get("country"),
                    ),
                )

            # Insert purchases data
            purchases = transaction.get("purchases", {}).get("products", [])
            for purchase in purchases:
                cursor.execute(
                    """
                    INSERT INTO data.purchases (transaction_id, product_sku, quantity, price, total)
                    VALUES (%s, %s, %s, %s, %s);
                """,
                    (
                        transaction_id,
                        purchase.get("sku"),
                        purchase.get("quanitity"),
                        purchase.get("price"),
                        purchase.get("total"),
                    ),
                )

    connection.commit()
    logger.debug(f"Data loaded successfully for transactions ({date}/{hour}).")


def process_hourly_data(
    connection: Any, date: str, hour: str, available_datasets: List[str]
) -> None:
    """
    Process hourly data for the specified date and hour.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the hourly data.
        hour (str): The hour of the hourly data.
        available_datasets (List[str]): List of available datasets for the given hour.
    """
    dataset_paths = {
        dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
        for dataset in available_datasets
    }
    logger.debug("Dataset Paths:", dataset_paths)

    # Record the start time
    start_time = datetime.now()

    # Extract rawdata
    transactions_data = extract_data(dataset_paths.get("transactions.json.gz", ""))

    # Transform and validate rawdata
    transformed_transactions = transform_and_validate_transactions(
        connection, transactions_data, date, hour
    )

    # Load processed rawdata
    load_data(
        transformed_transactions,
        "transactions.json.gz",
        date,
        hour,
        PROCESSED_DATA_PATH,
    )

    # Log processed transactions
    log_processed_transactions(connection, date, hour, transformed_transactions)

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(
        connection,
        date,
        hour,
        "transactions.json.gz",
        len(transformed_transactions),
        processing_time,
    )

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data(connection: Any) -> None:
    """
    Process all available rawdata.

    Args:
        connection (Any): The PostgreSQL connection.
    """
    # Get a sorted list of date folders
    try:
        date_folders = os.listdir(RAW_DATA_PATH)
        date_folders.sort()
        # Process all available rawdata
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
                    if filename.startswith("transactions")
                    and filename.endswith((".json", ".json.gz"))
                ]

                if available_datasets:
                    process_hourly_data(
                        connection, date_folder, hour_folder, available_datasets
                    )
                else:
                    logger.warning(f"No datasets found for {date_folder}/{hour_folder}")

        # Clean up empty directories in rawdata after processing
        cleanup_empty_directories(RAW_DATA_PATH)

    except (psycopg2.Error, Exception):
        logger.exception("An error occurred while processing data")


def main() -> None:
    """
    Main function to run the data processing pipeline.
    """
    try:
        with connect_to_postgres() as connection:
            process_all_data(connection)
    except psycopg2.Error:
        logger.exception("An error occurred while processing data")
    except Exception:
        logger.exception("An error occurred")


if __name__ == "__main__":
    cProfile.run("main()", sort="cumulative")
