import os
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
import requests

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

DEFAULT_SUPABASE_STORAGE_BUCKET = os.getenv(
    "SUPABASE_STORAGE_BUCKET", "real-estate-data"
)
DEFAULT_SUPABASE_RAW_PREFIX = os.getenv("SUPABASE_RAW_PREFIX", "raw")
DEFAULT_SUPABASE_TRANSFORMED_PREFIX = os.getenv(
    "SUPABASE_TRANSFORMED_PREFIX", "transformed"
)
DEFAULT_RAW_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "raw",
        "raw_latest.csv",
    )
)


def _env(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value.strip()
    return default


def _get_supabase_storage_config() -> Dict[str, str]:
    url = _env("SUPABASE_URL")
    service_key = _env("SUPABASE_SERVICE_ROLE_KEY")  # service_role key

    if not url or not service_key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    return {"url": url, "service_key": service_key}


def _build_object_key(
    prefix: str, basename: str, snapshot_date: str, etl_run_id: str
) -> str:
    return f"{prefix}/{basename}_{snapshot_date}_{etl_run_id}.csv"


def load_to_supabase_storage(
    csv_file: str, bucket_name: str, object_key: str
) -> Dict[str, str]:
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    storage = _get_supabase_storage_config()
    url = storage["url"].rstrip("/")
    upload_url = f"{url}/storage/v1/object/{bucket_name}/{object_key}"
    headers = {
        "apikey": storage["service_key"],
        "Authorization": f"Bearer {storage['service_key']}",
        "Content-Type": "text/csv",
        "x-upsert": "true",
    }

    logger.info(
        "Uploading %s to Supabase Storage: %s/%s", csv_file, bucket_name, object_key
    )
    with open(csv_file, "rb") as f:
        response = requests.post(
            upload_url,
            headers=headers,
            data=f,
            timeout=120,
        )

    if response.status_code >= 400:
        logger.error(
            "Supabase Storage upload failed (%s): %s",
            response.status_code,
            response.text,
        )
        response.raise_for_status()

    logger.info("Supabase Storage upload successful for %s", object_key)
    return {"file_path": csv_file, "bucket": bucket_name, "object_key": object_key}


def load_json_to_supabase_storage(
    payload: Dict[str, str], bucket_name: str, object_key: str
) -> Dict[str, str]:
    storage = _get_supabase_storage_config()
    url = storage["url"].rstrip("/")
    upload_url = f"{url}/storage/v1/object/{bucket_name}/{object_key}"
    headers = {
        "apikey": storage["service_key"],
        "Authorization": f"Bearer {storage['service_key']}",
        "Content-Type": "application/json",
        "x-upsert": "true",
    }

    logger.info(
        "Uploading JSON manifest to Supabase Storage: %s/%s", bucket_name, object_key
    )
    response = requests.post(
        upload_url,
        headers=headers,
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        timeout=120,
    )

    if response.status_code >= 400:
        logger.error(
            "Supabase Storage JSON upload failed (%s): %s",
            response.status_code,
            response.text,
        )
        response.raise_for_status()

    logger.info("Supabase Storage JSON upload successful for %s", object_key)
    return {"bucket": bucket_name, "object_key": object_key}


def load_csv(
    csv_file: str = DEFAULT_FILE,
    bucket_name: str = DEFAULT_SUPABASE_STORAGE_BUCKET,
    prefix: str = DEFAULT_SUPABASE_TRANSFORMED_PREFIX,
):
    """Upload transformed CSV data to Supabase Storage."""
    logger.info("STARTING DATA LOAD TO SUPABASE STORAGE (TRANSFORMED)")
    etl_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    object_key = _build_object_key(
        prefix=prefix,
        basename="transformed",
        snapshot_date=snapshot_date,
        etl_run_id=etl_run_id,
    )
    return load_to_supabase_storage(
        csv_file=csv_file, bucket_name=bucket_name, object_key=object_key
    )


def load_files_to_supabase(
    raw_file: str = DEFAULT_RAW_FILE,
    transformed_file: str = DEFAULT_FILE,
    bucket_name: str = DEFAULT_SUPABASE_STORAGE_BUCKET,
    raw_prefix: str = DEFAULT_SUPABASE_RAW_PREFIX,
    transformed_prefix: str = DEFAULT_SUPABASE_TRANSFORMED_PREFIX,
    etl_run_id: Optional[str] = None,
    snapshot_date: Optional[str] = None,
):
    """Upload raw and transformed CSV files into Supabase Storage."""
    logger.info("STARTING DATA LOAD TO SUPABASE STORAGE (RAW + TRANSFORMED)")
    results = {}
    etl_run_id = etl_run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    snapshot_date = snapshot_date or datetime.now(timezone.utc).strftime("%Y%m%d")

    if raw_file and os.path.exists(raw_file):
        raw_key = _build_object_key(
            prefix=raw_prefix,
            basename="raw",
            snapshot_date=snapshot_date,
            etl_run_id=etl_run_id,
        )
        results["raw"] = load_to_supabase_storage(
            csv_file=raw_file,
            bucket_name=bucket_name,
            object_key=raw_key,
        )
        latest_manifest_key = f"{raw_prefix}/_latest.json"
        latest_manifest_payload = {
            "path": raw_key,
            "run_id": etl_run_id,
            "snapshot_date": snapshot_date,
        }
        results["raw_latest_manifest"] = load_json_to_supabase_storage(
            payload=latest_manifest_payload,
            bucket_name=bucket_name,
            object_key=latest_manifest_key,
        )
    else:
        logger.warning("Raw file not found or not provided, skipping: %s", raw_file)

    if transformed_file and os.path.exists(transformed_file):
        transformed_key = _build_object_key(
            prefix=transformed_prefix,
            basename="transformed",
            snapshot_date=snapshot_date,
            etl_run_id=etl_run_id,
        )
        results["transformed"] = load_to_supabase_storage(
            csv_file=transformed_file,
            bucket_name=bucket_name,
            object_key=transformed_key,
        )
    else:
        logger.warning(
            "Transformed file not found or not provided, skipping: %s", transformed_file
        )

    if not results:
        raise FileNotFoundError(
            "No files loaded. Provide valid raw/transformed file paths."
        )

    logger.info("Supabase Storage uploads completed successfully")
    return results


if __name__ == "__main__":
    logger.info(f"RUNNING IN {config.ENV_TYPE.upper()} ENVIRONMENT")
    logger.info("\nConfiguration:")
    logger.info(f"  Raw file: {DEFAULT_RAW_FILE}")
    logger.info(f"  Transformed file: {DEFAULT_FILE}")
    logger.info(f"  Supabase storage bucket: {DEFAULT_SUPABASE_STORAGE_BUCKET}")
    logger.info(f"  Supabase raw prefix: {DEFAULT_SUPABASE_RAW_PREFIX}")
    logger.info(f"  Supabase transformed prefix: {DEFAULT_SUPABASE_TRANSFORMED_PREFIX}")

    try:
        start_time = datetime.now(timezone.utc)
        load_files_to_supabase()
        duration = datetime.now(timezone.utc) - start_time

        logger.info("DATA LOAD COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration}")

    except Exception as e:
        logger.error("DATA LOAD FAILED")
        logger.error(f"Error: {str(e)}")
        exit(1)
