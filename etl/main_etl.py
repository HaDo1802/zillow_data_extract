import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from email_notifier import EmailNotifier  # noqa: E402
from extract import fetch_all_locations  # noqa: E402
from load import load_csv  # noqa: E402
from logger import get_logger  # noqa: E402
from transform import main_transform  # noqa: E402
from load_to_s3 import load_to_s3  # noqa: E402

logger = get_logger(__name__)

DEFAULT_LOCATIONS = ["Las Vegas, NV"]
DEFAULT_MAX_PAGES = 2
DEFAULT_S3_BUCKET = "real-estate-scraped-data"


def get_base_paths(base_dir: Optional[str] = None) -> Tuple[str, str, str]:
    if base_dir:
        selected_base_dir = base_dir
    elif os.path.exists("/opt/airflow"):
        selected_base_dir = "/opt/airflow"
    else:
        selected_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    raw_dir = os.path.join(selected_base_dir, "data", "raw")
    transformed_dir = os.path.join(selected_base_dir, "data", "transformed")

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(transformed_dir, exist_ok=True)

    return selected_base_dir, raw_dir, transformed_dir


def run_etl_pipeline(
    locations: Optional[List[str]] = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    s3_bucket: str = DEFAULT_S3_BUCKET,
    base_dir: Optional[str] = None,
) -> Tuple[bool, Dict[str, Any]]:
    logger.info("STARTING REAL ESTATE ETL PIPELINE")

    target_locations = locations or DEFAULT_LOCATIONS

    start_time = datetime.now()
    etl_run_id = start_time.strftime("%Y%m%d_%H%M")
    resolved_base_dir, raw_dir, transformed_dir = get_base_paths(base_dir=base_dir)
    env_type = "Docker/Airflow" if os.path.exists("/opt/airflow") else "Local"

    logger.info(f"Environment: {env_type} | ETL Run ID: {etl_run_id}")

    details = {
        "etl_run_id": etl_run_id,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "environment": env_type,
        "locations": target_locations,
        "max_pages": max_pages,
        "s3_bucket": s3_bucket,
        "base_dir": resolved_base_dir,
    }

    try:
        logger.info("STAGE 1: EXTRACT")
        df_extracted = fetch_all_locations(target_locations, max_pages)
        if df_extracted.empty:
            details["error"] = "No data extracted from API"
            details["failed_step"] = "EXTRACT"
            return False, details

        details["properties_extracted"] = len(df_extracted)
        logger.info(f"EXTRACT COMPLETED: {len(df_extracted)} properties")

        logger.info("STAGE 2: TRANSFORM")
        raw_latest = os.path.join(raw_dir, "raw_latest.csv")
        df_transformed, timestamped_file, latest_file = main_transform(
            input_file=raw_latest, output_dir=transformed_dir
        )

        if df_transformed is None or df_transformed.empty:
            details["error"] = "No data after transformation"
            details["failed_step"] = "TRANSFORM"
            return False, details

        details["records_transformed"] = len(df_transformed)
        logger.info("TRANSFORM COMPLETED")

        logger.info("STAGE 3: LOAD")
        load_csv(csv_file=latest_file)
        details["records_loaded"] = len(df_transformed)
        logger.info("LOAD COMPLETED\n")

        logger.info("Load to S3")
        snapshot_date = start_time.strftime("%Y%m%d")
        raw_s3_key = f"raw/raw_{snapshot_date}_{etl_run_id}.csv"
        transformed_s3_key = f"transformed/transformed_{snapshot_date}_{etl_run_id}.csv"

        load_to_s3(
            file_path=raw_latest,
            bucket_name=s3_bucket,
            s3_key=raw_s3_key,
        )
        load_to_s3(
            file_path=latest_file,
            bucket_name=s3_bucket,
            s3_key=transformed_s3_key,
        )
        logger.info("LOAD TO S3 COMPLETED\n")

        end_time = datetime.now()
        duration = end_time - start_time

        details["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        details["duration"] = str(duration).split(".")[0]
        quality_rate = (len(df_transformed) / len(df_extracted)) * 100
        details["quality_rate"] = f"{quality_rate:.1f}%"
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(
            f"Duration: {details['duration']} | Quality: {details['quality_rate']}"
        )
        return True, details

    except Exception as e:
        logger.error(f"ETL PIPELINE FAILED: {str(e)}", exc_info=True)
        details["error"] = str(e)
        details["failed_step"] = details.get("failed_step", "UNKNOWN")
        return False, details


if __name__ == "__main__":
    logger.info("Real Estate ETL Pipeline - Main Entry Point\n")

    email_notifier = EmailNotifier()
    pipeline_start = datetime.now()

    success, details = run_etl_pipeline()

    details["total_execution_time"] = str(datetime.now() - pipeline_start).split(".")[0]

    if success:
        logger.info("Sending success email...")
        email_notifier.send_notification(success=True, details=details)
    else:
        logger.error(f"Total execution time: {details['total_execution_time']}")
        logger.error(f"Failed at: {details.get('failed_step', 'UNKNOWN')}")
        logger.error("Sending failure email...")
        email_notifier.send_notification(success=False, details=details)
        sys.exit(1)
