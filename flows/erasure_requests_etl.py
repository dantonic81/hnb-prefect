import gzip
import json
import hashlib
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
    extract_actual_date,
    extract_actual_hour,
    log_processing_statistics,
    extract_data,
)
import psycopg2
from typing import Any, Optional, Tuple, List, Dict

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


RAW_DATA_PATH = "rawdata"
PROCESSED_DATA_PATH = "processed_data"
ARCHIVED_DATA_PATH = "archived_data"
INVALID_RECORDS_TABLE = "data.invalid_customers"
ERASURE_REQUESTS_SCHEMA_FILE = "erasure_requests_schema.json"


with open(ERASURE_REQUESTS_SCHEMA_FILE, "r") as schema_file:
    ERASURE_REQUESTS_SCHEMA = json.load(schema_file)


# Query the customers table to get date and hour for a given customer_id
def get_date_and_hour_to_anonymize(
    connection: Any, customer_id: str
) -> Optional[Tuple[datetime, int]]:
    """
    Query the customers table to get date and hour for a given customer_id.

    Args:
        connection (Any): The PostgreSQL connection.
        customer_id (str): The customer ID.

    Returns:
        Optional[Tuple[datetime, int]]: A tuple containing date and hour if the customer is found, else None.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT record_date, record_hour FROM data.customers
            WHERE id = %s
        """,
            (customer_id,),
        )
        result = cursor.fetchone()
        if result:
            return result
    return None


def locate_processed_data_file(date: datetime, hour: int) -> Optional[str]:
    """
    Locate the processed data file based on date and hour.

    Args:
        date (datetime): The date of the data.
        hour (int): The hour of the data.

    Returns:
        Optional[str]: The path to the processed data file if found, else None.
    """
    formatted_date = format_date_for_file_system(date)
    formatted_hour = format_hour_for_file_system(hour)
    processed_data_path = os.path.join(
        PROCESSED_DATA_PATH, str(formatted_date), str(formatted_hour)
    )
    for filename in os.listdir(processed_data_path):
        if filename.endswith((".json", ".json.gz")) and filename.startswith(
            "customers"
        ):
            return os.path.join(processed_data_path, filename)
    return None


# Anonymize and update the data in the processed data file
def anonymize_and_update_data(
    file_path: str, customer_id: str, erasure_request: Dict[str, Any]
) -> None:
    """
    Anonymize and update the data in the processed data file.

    Args:
        file_path (str): The path to the processed data file.
        customer_id (str): The customer ID.
        erasure_request (Dict[str, Any]): The erasure request data.
    """
    email_to_anonymize = erasure_request.get("email")
    anonymized_email = hashlib.sha256(email_to_anonymize.encode()).hexdigest()
    logger.debug(f"email: {email_to_anonymize}, anonymized_email: {anonymized_email}")
    try:
        is_gzipped = file_path.endswith(".gz")

        with gzip.open(file_path, "rt") if is_gzipped else open(file_path, "r") as file:
            data = [json.loads(line) for line in file]

        for record in data:
            if record.get("id") == customer_id:
                # Anonymize the email in the record
                record["email"] = anonymized_email

        with gzip.open(file_path, "wt") if is_gzipped else open(file_path, "w") as file:
            for record in data:
                json.dump(record, file)
                file.write("\n")
    except Exception:
        logger.exception(f"An error occurred while updating file {file_path}")


def archive_updated_file(file_path: str, date: datetime, hour: int) -> None:
    """
    Archive the updated file.

    Args:
        file_path (str): The path to the file to be archived.
        date (str): The date of the data.
        hour (str): The hour of the data.
    """
    archive_path = os.path.join(ARCHIVED_DATA_PATH, str(date), str(hour))
    os.makedirs(archive_path, exist_ok=True)

    archive_file = os.path.basename(file_path)
    archive_file_path = os.path.join(archive_path, archive_file)

    os.rename(file_path, archive_file_path)
    logger.info(f"File archived: {archive_file_path}")


def process_erasure_requests(
    connection: Any, erasure_requests: List[Dict[str, Any]]
) -> None:
    """
    Process erasure requests by anonymizing and updating customer data.

    Args:
        connection (Any): The PostgreSQL connection.
        erasure_requests (List[Dict[str, Any]]): List of erasure requests.
    """
    for erasure_request in erasure_requests:
        customer_id = erasure_request.get("customer-id")
        if customer_id:
            # Step 1: Query the processed_data_log table to get date and hour
            result = get_date_and_hour_to_anonymize(connection, customer_id)
            if result:
                date, hour = result

                # Step 2: Locate the processed data file
                file_path = locate_processed_data_file(date, hour)
                if file_path:
                    # Step 3: Anonymize and update the data
                    anonymize_and_update_data(file_path, customer_id, erasure_request)

                    # Step 4: Archive the updated file
                    archive_updated_file(file_path, date, hour)


def format_date_for_file_system(actual_date: datetime) -> str:
    """
    Format the date for the file system.

    Args:
        actual_date (datetime): The actual date.

    Returns:
        str: Formatted date string.
    """
    return f"date={actual_date.strftime('%Y-%m-%d')}"


def format_hour_for_file_system(actual_hour: int) -> str:
    """
    Format the hour for the file system.

    Args:
        actual_hour (int): The actual hour.

    Returns:
        str: Formatted hour string.
    """
    return f"hour={actual_hour:02}"


def log_invalid_erasure_request(
    connection: Any,
    erasure_request: Dict[str, Any],
    error_message: str,
    date: str,
    hour: str,
) -> None:
    """
    Log invalid erasure requests into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        erasure_request (Dict[str, Any]): The invalid erasure request data.
        error_message (str): The error message describing the validation error.
        date (str): The date of the data.
        hour (str): The hour of the data.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        # Check if the customer with the same id already exists
        customer_id = erasure_request.get("customer-id")
        cursor.execute(
            """
            SELECT customer_id FROM data.invalid_erasure_requests WHERE customer_id = %s;
        """,
            (customer_id,),
        )

        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record if needed
            cursor.execute(
                """
                UPDATE data.invalid_erasure_requests
                SET error_message = %s
                WHERE customer_id = %s;
            """,
                (error_message, customer_id),
            )
        else:
            cursor.execute(
                """
                INSERT INTO data.invalid_erasure_requests (record_date, record_hour, customer_id, error_message)
                VALUES (%s, %s, %s, %s);
            """,
                (actual_date, actual_hour, customer_id, error_message),
            )
    connection.commit()


def transform_and_validate_erasure_requests(
    connection: Any, erasure_requests_data: List[Dict[str, Any]], date: str, hour: str
) -> List[Dict[str, Any]]:
    """
    Transform and validate erasure requests against the schema.

    Args:
        connection (Any): The PostgreSQL connection.
        erasure_requests_data (List[Dict[str, Any]]): List of erasure requests data.
        date (str): The date of the data.
        hour (str): The hour of the data.

    Returns:
        List[Dict[str, Any]]: List of valid erasure requests.
    """
    schema = ERASURE_REQUESTS_SCHEMA

    valid_erasure_requests = []

    # Keep track of unique customer-ids
    unique_customer_ids = set()

    # Validate each erasure request against the schema
    for erasure_request in erasure_requests_data:
        try:
            # Perform validation
            validate(instance=erasure_request, schema=schema)

            # Extract customer-id from the erasure request
            customer_id = erasure_request.get("customer-id")

            # Check uniqueness of customer-id
            if customer_id and customer_id not in unique_customer_ids:
                unique_customer_ids.add(customer_id)
                valid_erasure_requests.append(erasure_request)
            else:
                # Log or handle duplicate customer-id
                logger.debug(
                    f"Duplicate customer-id found for erasure request: {customer_id}"
                )
                log_invalid_erasure_request(
                    connection, erasure_request, "Duplicate customer-id", date, hour
                )

        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            logger.error(f"Validation error for erasure request: {e}")
            log_invalid_erasure_request(connection, erasure_request, str(e), date, hour)
            continue

    return valid_erasure_requests


def log_processed_erasure_requests(
    connection: Any, date: str, hour: str, customer_ids: List[str], emails: List[str]
) -> None:
    """
    Log processed erasure requests into the database.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the data.
        hour (str): The hour of the data.
        customer_ids (List[str]): List of customer IDs.
        emails (List[str]): List of emails.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for customer_id, email in zip(customer_ids, emails):
            # Check if the record already exists
            cursor.execute(
                """
                SELECT COUNT(*) FROM data.erasure_requests
                WHERE record_date = %s AND record_hour = %s AND customer_id = %s;
            """,
                (actual_date, actual_hour, customer_id),
            )

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute(
                    """
                    INSERT INTO data.erasure_requests (record_date, record_hour, customer_id, email)
                    VALUES (%s, %s, %s, %s);
                """,
                    (actual_date, actual_hour, customer_id, email),
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
    Process erasure requests for a given hour.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the data.
        hour (str): The hour of the data.
        available_datasets (List[str]): List of available datasets for the given hour.
    """
    dataset_paths = {
        dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
        for dataset in available_datasets
    }
    logger.debug("Dataset Paths:", dataset_paths)

    # Record the start time
    start_time = datetime.now()

    # Extract rawdata from both .json.gz and .json files
    erasure_requests_data = []
    for dataset_type in ["erasure-requests.json.gz", "erasure-requests.json"]:
        if dataset_type in dataset_paths:
            erasure_requests_data.extend(extract_data(dataset_paths[dataset_type]))

    transformed_and_validated_erasure_requests = (
        transform_and_validate_erasure_requests(
            connection, erasure_requests_data, date, hour
        )
    )

    process_erasure_requests(connection, erasure_requests_data)
    customer_ids = [
        request["customer-id"] for request in transformed_and_validated_erasure_requests
    ]
    emails = [
        request["email"] for request in transformed_and_validated_erasure_requests
    ]
    log_processed_erasure_requests(connection, date, hour, customer_ids, emails)

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(
        connection,
        date,
        hour,
        "erasure_requests.json.gz",
        len(erasure_requests_data),
        processing_time,
    )

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data(connection: Any) -> None:
    """
    Process all available erasure requests data.

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
                    if filename.startswith("erasure")
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
    Main function to run the erasure requests processing pipeline.
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
