import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ..logger import setup_logger

logging = setup_logger("etl.extract.s3")


def get_storage_options(aws_conn_id: str):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    creds = s3_hook.get_credentials()

    storage_options = {
        "key": creds.access_key,
        "secret": creds.secret_key
    }

    return s3_hook, storage_options


def extract_data_from_s3(bucket: str,folder: str,aws_conn_id: str,file_type: str = "csv"):
    logging.info("Starting S3 extraction",extra={"bucket": bucket,"folder": folder,"file_type": file_type })

    s3_hook, storage_options = get_storage_options(aws_conn_id=aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=folder)

    if not keys:
        logging.error("No files found in S3 location",extra={"bucket": bucket, "folder": folder})
        raise AirflowException(f"No files found in bucket {bucket}/{folder}")

    matched_paths = [
        f"s3://{bucket}/{key}"
        for key in keys
        if key.lower().endswith(f".{file_type}")]

    if not matched_paths:
        logging.error(
            "No matching files found for file type",extra={"bucket": bucket,"folder": folder,"file_type": file_type})
        raise AirflowException(f"No {file_type} in bucket {bucket}/{folder}")

    logging.info("S3 extraction completed",extra={"files_count": len(matched_paths)})

    return matched_paths
