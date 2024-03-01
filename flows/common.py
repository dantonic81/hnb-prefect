from contextlib import contextmanager
import os
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta
import logging
import gzip
import json
from typing import Any, Generator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_connection_pool() -> SimpleConnectionPool:
    """
    Create a PostgreSQL connection pool.

    Returns:
        SimpleConnectionPool: A PostgreSQL connection pool.
    """
    return SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


@contextmanager
def get_postgres_connection() -> Generator:
    """
    Context manager for acquiring and releasing a PostgreSQL connection.

    Yields:
        Any: A PostgreSQL connection.
    """
    connection_pool = create_connection_pool()
    connection = connection_pool.getconn()

    try:
        yield connection
    finally:
        connection_pool.putconn(connection)


def connect_to_postgres() -> Any:
    """
    Connect to PostgreSQL using the context manager.

    Returns:
        Any: A PostgreSQL connection.
    """
    with get_postgres_connection() as connection:
        return connection


def cleanup_empty_directories(directory: str) -> None:
    """
    Cleanup empty directories in the specified directory.

    Args:
        directory (str): The directory to cleanup.
    """
    for root, dirs, files in os.walk(directory, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                logger.debug(f"Empty directory deleted: {dir_path}")


def archive_and_delete(
    file_path: str, dataset_type: str, date: str, hour: str, archive_path: str
) -> None:
    """
    Archive and delete the specified file.

    Args:
        file_path (str): The path of the file to archive and delete.
        dataset_type (str): The type of the dataset.
        date (str): The date of the dataset.
        hour (str): The hour of the dataset.
        archive_path (str): The path where the file should be archived.
    """
    archive_file = dataset_type
    archive_file_path = os.path.join(archive_path, date, hour, archive_file)

    # Create the archive directory if it doesn't exist
    os.makedirs(os.path.dirname(archive_file_path), exist_ok=True)

    # Archive the file
    os.rename(file_path, archive_file_path)
    logger.debug(f"File archived: {archive_file_path}")


def extract_actual_date(date_str: str) -> datetime.date:
    """
    Extract the actual date from the formatted date string.

    Args:
        date_str (str): The formatted date string.

    Returns:
        datetime.date: The actual date.
    """
    return datetime.strptime(date_str.replace("date=", ""), "%Y-%m-%d").date()


# Extract the actual hour from the "hour=00" format
def extract_actual_hour(hour_str) -> int:
    """
    Extract the actual hour from the formatted hour string.

    Args:
        hour_str (str): The formatted hour string.

    Returns:
        int: The actual hour.
    """
    return int(hour_str.replace("hour=", ""))


def log_processing_statistics(
    connection: Any,
    date: str,
    hour: str,
    dataset_type: str,
    record_count: int,
    processing_time: timedelta,
) -> None:
    """
    Log processing statistics.

    Args:
        connection (Any): The PostgreSQL connection.
        date (str): The date of the processed data.
        hour (str): The hour of the processed data.
        dataset_type (str): The type of the processed dataset.
        record_count (int): The number of records processed.
        processing_time (datetime.timedelta): The time taken for processing.
    """
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO data.processing_statistics (record_date, record_hour, dataset_type, record_count, processing_time)
            VALUES (%s, %s, %s, %s, %s);
        """,
            (actual_date, actual_hour, dataset_type, record_count, processing_time),
        )
    connection.commit()


def extract_data(file_path: str) -> list:
    """
    Extract data from the specified file.

    Args:
        file_path (str): The path of the file.

    Returns:
        list: The extracted data.
    """
    if not file_path:
        return []

    _, file_extension = os.path.splitext(file_path)

    if file_extension == ".gz":
        # Extract raw_data from a gzipped JSON file
        with gzip.open(file_path, "rt") as file:
            data = [json.loads(line) for line in file]
    elif file_extension == ".json":
        # Extract raw_data from a plain JSON file
        with open(file_path, "r", encoding="utf-8") as file:
            data = [json.load(file)]
    else:
        logger.warning(f"Unsupported file format: {file_extension}")
        return []

    return data


def load_data(
    data: list, dataset_type: str, date: str, hour: str, processed_data_path: str
) -> None:
    """
    Load data to the specified location.

    Args:
        data (list): The data to be loaded.
        dataset_type (str): The type of the dataset.
        date (str): The date of the dataset.
        hour (str): The hour of the dataset.
        processed_data_path (str): The path where the data should be loaded.
    """
    logger.debug(f"Loading data {data} for {dataset_type} {date} {hour}")
    # Create the corresponding subdirectories in processed_data
    output_dir = os.path.join(processed_data_path, date, hour)
    os.makedirs(output_dir, exist_ok=True)

    # Determine the appropriate file extension based on dataset_type
    if dataset_type.endswith(".json.gz"):
        file_extension = ".json.gz"
    elif dataset_type.endswith(".json"):
        file_extension = ".json"
    else:
        raise ValueError(f"Unsupported file extension in dataset_type: {dataset_type}")

    # Remove the existing extension if present
    dataset_type_without_extension, _ = dataset_type.split(".", 1)

    # Load processed data to the new location only if the dataset is not empty
    if data:
        # Use gzip compression if the file extension is .json.gz
        open_func = gzip.open if file_extension == ".json.gz" else open
        # Construct the output path
        output_path = os.path.join(
            str(output_dir), f"{dataset_type_without_extension}{file_extension}"
        )

        with open_func(output_path, "wt") as file:
            for record in data:
                json.dump(record, file)
                file.write("\n")
    else:
        logger.debug(f"Skipping loading for empty dataset: {dataset_type}")