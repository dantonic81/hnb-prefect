import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import jsonschema
from jsonschema import validate
from common import (
    extract_data,
    connect_to_postgres,
    cleanup_empty_directories,
    archive_and_delete,
    log_processing_statistics,
    extract_actual_date,
    extract_actual_hour,
    load_data,
)
import psycopg2
from typing import Any, Dict, List


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


RAW_DATA_PATH = "raw_data"
PROCESSED_DATA_PATH = "processed_data"
ARCHIVED_DATA_PATH = "archived_data"
INVALID_RECORDS_TABLE = "data.invalid_customers"
CUSTOMERS_SCHEMA_FILE = "customer_schema.json"


# Load the JSON schema once and store it in a variable
with open(CUSTOMERS_SCHEMA_FILE, "r") as schema_file:
    CUSTOMERS_SCHEMA = json.load(schema_file)


def log_invalid_customer(
    connection: Any, customer: Dict[str, Any], error_message: str, date: str, hour: str
) -> None:
    """
    Log invalid customer data into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        customer (Dict[str, Any]): The invalid customer data.
        error_message (str): The error message describing the validation error.
        date (str): The date of the data.
        hour (str): The hour of the data.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        # Check if the customer with the same id already exists
        cursor.execute(
            """
            SELECT id FROM data.invalid_customers WHERE id = %s;
        """,
            (customer.get("id"),),
        )

        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record if needed
            cursor.execute(
                """
                UPDATE data.invalid_customers
                SET error_message = %s
                WHERE id = %s;
            """,
                (error_message, customer.get("id")),
            )
        else:
            cursor.execute(
                """
                INSERT INTO data.invalid_customers (record_date, record_hour, id, first_name, last_name, email, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    actual_date,
                    actual_hour,
                    customer.get("id"),
                    customer.get("first_name"),
                    customer.get("last_name"),
                    customer.get("email"),
                    error_message,
                ),
            )
    connection.commit()


def transform_and_validate_customers(
    connection: Any, customers_data: List[Dict[str, Any]], date: str, hour: str
) -> List[Dict[str, Any]]:
    """
    Transform and validate customer data.

    Args:
        connection (Any): The PostgreSQL connection.
        customers_data (List[Dict[str, Any]]): List of customer records.
        date (str): The date of the data.
        hour (str): The hour of the data.

    Returns:
        List[Dict[str, Any]]: List of valid customer records.
    """
    # Load the JSON schema
    schema = CUSTOMERS_SCHEMA

    valid_customers = []

    # Keep track of unique ids
    unique_ids = set()

    # Validate each customer record against the schema
    for customer in customers_data:
        try:
            validate(instance=customer, schema=schema)

            # Convert 'id' to integer
            customer_id = int(customer["id"])

            # Check uniqueness of id
            if customer_id not in unique_ids:
                unique_ids.add(customer_id)
                valid_customers.append(customer)
            else:
                # Log or handle duplicate id
                logger.debug(f"Duplicate id found for customer: {customer['id']}")
                log_invalid_customer(connection, customer, "Duplicate id", date, hour)

        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            logger.error(f"Validation error for customer: {e}")
            log_invalid_customer(connection, customer, str(e), date, hour)
            continue

    # Update last_change timestamp
    for customer in valid_customers:
        customer["last_change"] = datetime.utcnow().isoformat()

    return valid_customers


def log_processed_customers(
    connection: Any,
    date: str,
    hour: str,
    customer_ids: List[str],
    first_names: List[str],
    last_names: List[str],
    emails: List[str],
) -> None:
    """
    Log processed customer data into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the data.
        hour (str): The hour of the data.
        customer_ids (List[str]): List of customer IDs.
        first_names (List[str]): List of customer first names.
        last_names (List[str]): List of customer last names.
        emails (List[str]): List of customer emails.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for customer_id, first_name, last_name, email in zip(
            customer_ids, first_names, last_names, emails
        ):
            # Check if the record already exists
            cursor.execute(
                """
                SELECT COUNT(*) FROM data.customers
                WHERE record_date = %s AND record_hour = %s AND id = %s;
            """,
                (actual_date, actual_hour, customer_id),
            )

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute(
                    """
                    INSERT INTO data.customers (record_date, record_hour, id, first_name, last_name, email)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """,
                    (
                        actual_date,
                        actual_hour,
                        customer_id,
                        first_name,
                        last_name,
                        email,
                    ),
                )
            else:
                # Record already exists, log or handle accordingly
                logger.info(
                    f"Record for customer_id {customer_id} at {actual_date} {actual_hour} already exists."
                )
    connection.commit()


def process_hourly_data(
    connection: Any, date: str, hour: str, available_datasets: List[str]
) -> None:
    """
    Process hourly customer data for the specified date and hour.

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

    # Extract raw_data
    customers_data = extract_data(dataset_paths.get("customers.json.gz", ""))

    # Transform and validate raw_data
    transformed_customers = transform_and_validate_customers(
        connection, customers_data, date, hour
    )

    # Load processed raw_data
    load_data(
        transformed_customers, "customers.json.gz", date, hour, PROCESSED_DATA_PATH
    )

    # Log processed customers
    customer_ids = [customer["id"] for customer in transformed_customers]
    first_names = [customer["first_name"] for customer in transformed_customers]
    last_names = [customer["last_name"] for customer in transformed_customers]
    emails = [customer["email"] for customer in transformed_customers]
    log_processed_customers(
        connection, date, hour, customer_ids, first_names, last_names, emails
    )

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(
        connection,
        date,
        hour,
        "customers.json.gz",
        len(transformed_customers),
        processing_time,
    )

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        logger.debug("Processing dataset:", dataset_type, "Path:", dataset_path)
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data(connection: Any) -> None:
    """
    Process all available customer data.

    Args:
        connection (Any): The PostgreSQL connection.
    """
    # Get a sorted list of date folders
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
                    if filename.startswith("customers")
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
    Main function to run the customer data processing pipeline.
    """
    try:
        with connect_to_postgres() as connection:
            process_all_data(connection)
    except psycopg2.Error:
        logger.exception("An error occurred while processing data")
    except Exception:
        logger.exception("An error occurred")


if __name__ == "__main__":
    main()
