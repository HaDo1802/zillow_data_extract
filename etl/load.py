import os
import sys
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import boto3
from dotenv import load_dotenv

from logger import get_logger
from utils.config import config

# Initialize logger for this module
logger = get_logger(__name__)
load_dotenv()

DEFAULT_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "transformed",
        "transformed_latest.csv",
    )
)

DEFAULT_BUCKET = os.getenv("S3_BUCKET", "real-estate-scraped-data")
DEFAULT_TRANSFORMED_PREFIX = os.getenv("S3_TRANSFORMED_PREFIX", "transformed")
DEFAULT_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw")
DEFAULT_RAW_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "raw",
        "raw_latest.csv",
    )
)


def load_to_s3(file_path: str, bucket_name: str, s3_key: str) -> None:
    """Uploads a file to an S3 bucket."""
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        logger.info("File %s uploaded to s3://%s/%s", file_path, bucket_name, s3_key)
    except Exception as e:
        logger.error("Failed to upload %s to S3: %s", file_path, e)
        raise


def _build_s3_key(prefix: str, basename: str, snapshot_date: str, etl_run_id: str) -> str:
    # Include snapshot_date + etl_run_id so multiple same-day runs are traceable for audit/backfills.
    return f"{prefix}/{basename}_{snapshot_date}_{etl_run_id}.csv"


def load_csv(
    csv_file=DEFAULT_FILE,
    bucket_name: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_TRANSFORMED_PREFIX,
):
    """
    Upload transformed CSV data to S3.

    Postgres loading is handled downstream, so this step only stages the file in S3.
    """
    logger.info("STARTING DATA LOAD TO S3 (POSTGRES LOAD DISABLED IN THIS PROJECT)")
    logger.info(f"Loading file: {csv_file}")

    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    etl_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    s3_key = _build_s3_key(
        prefix=prefix,
        basename="transformed",
        snapshot_date=snapshot_date,
        etl_run_id=etl_run_id,
    )

    load_to_s3(csv_file, bucket_name, s3_key)

    logger.info("S3 upload completed successfully")
    logger.info(f"S3 destination: s3://{bucket_name}/{s3_key}")
    return {"file_path": csv_file, "bucket": bucket_name, "s3_key": s3_key}


def load_files_to_s3(
    raw_file: str = DEFAULT_RAW_FILE,
    transformed_file: str = DEFAULT_FILE,
    bucket_name: str = DEFAULT_BUCKET,
    raw_prefix: str = DEFAULT_RAW_PREFIX,
    transformed_prefix: str = DEFAULT_TRANSFORMED_PREFIX,
):
    """Upload raw and transformed CSV files to S3 in one step."""
    logger.info("STARTING DATA LOAD TO S3 (RAW + TRANSFORMED)")

    etl_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    results = {}

    if raw_file and os.path.exists(raw_file):
        raw_key = _build_s3_key(
            prefix=raw_prefix,
            basename="raw",
            snapshot_date=snapshot_date,
            etl_run_id=etl_run_id,
        )
        load_to_s3(raw_file, bucket_name, raw_key)
        results["raw"] = {"file_path": raw_file, "bucket": bucket_name, "s3_key": raw_key}
    else:
        logger.warning("Raw file not found or not provided, skipping: %s", raw_file)

    if transformed_file and os.path.exists(transformed_file):
        transformed_key = _build_s3_key(
            prefix=transformed_prefix,
            basename="transformed",
            snapshot_date=snapshot_date,
            etl_run_id=etl_run_id,
        )
        load_to_s3(transformed_file, bucket_name, transformed_key)
        results["transformed"] = {
            "file_path": transformed_file,
            "bucket": bucket_name,
            "s3_key": transformed_key,
        }
    else:
        logger.warning("Transformed file not found or not provided, skipping: %s", transformed_file)

    if not results:
        raise FileNotFoundError("No files uploaded. Provide valid raw/transformed file paths.")

    logger.info("S3 uploads completed successfully")
    return results


if __name__ == "__main__":
    logger.info(f"RUNNING IN {config.ENV_TYPE.upper()} ENVIRONMENT")
    logger.info("\nConfiguration:")
    logger.info(f"  Raw file: {DEFAULT_RAW_FILE}")
    logger.info(f"  Transformed file: {DEFAULT_FILE}")
    logger.info(f"  S3 bucket: {DEFAULT_BUCKET}")
    logger.info(f"  S3 raw prefix: {DEFAULT_RAW_PREFIX}")
    logger.info(f"  S3 transformed prefix: {DEFAULT_TRANSFORMED_PREFIX}")
    logger.info("  Postgres load: disabled (handled downstream)")

    try:
        start_time = datetime.now(timezone.utc)
        load_files_to_s3()
        duration = datetime.now(timezone.utc) - start_time

        logger.info("DATA LOAD COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration}")

    except Exception as e:
        logger.error("DATA LOAD FAILED")
        logger.error(f"Error: {str(e)}")
        exit(1)
