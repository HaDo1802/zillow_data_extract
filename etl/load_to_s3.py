from dotenv import load_dotenv
import boto3
import os
import sys
from datetime import datetime, timezone


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from logger import get_logger
from utils.config import config

load_dotenv()
logger = get_logger(__name__)


def load_to_s3(file_path, bucket_name, s3_key):
    """
    Uploads a file to an S3 bucket.

    Args:
        file_path (str): Local path to the file to be uploaded.
        bucket_name (str): Name of the target S3 bucket.
        s3_key (str): S3 object key (path in the bucket).
    """
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"File {file_path} uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to S3: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    running_env = config.ENV_TYPE
    etl_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    bucket_name = "real-estate-scraped-data"
    if running_env == "local":
        raw_file_path = "data/raw/raw_latest.csv"
        transformed_file_path = "data/transformed/transformed_latest.csv"
    else:
        raw_file_path = "/opt/airflow/data/raw/raw_latest.csv"
        transformed_file_path = "/opt/airflow/data/transformed/transformed_latest.csv"
    raw_s3_key = f"raw/raw_{snapshot_date}_{etl_run_id}.csv"
    transformed_s3_key = f"transformed/transformed_{snapshot_date}_{etl_run_id}.csv"

    load_to_s3(raw_file_path, bucket_name, raw_s3_key)
    load_to_s3(transformed_file_path, bucket_name, transformed_s3_key)
