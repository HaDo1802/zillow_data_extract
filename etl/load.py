import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from logger import get_logger

from utils.config import config

logger = get_logger(__name__)
load_dotenv()
UPLOAD_TIMEOUT_SECONDS = 120

DEFAULT_TRANSFORMED_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "transformed",
        "transformed_latest.csv",
    )
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

DEFAULT_SUPABASE_STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "real-estate-data")
DEFAULT_SUPABASE_RAW_PREFIX = os.getenv("SUPABASE_RAW_PREFIX", "raw")
DEFAULT_SUPABASE_TRANSFORMED_PREFIX = os.getenv("SUPABASE_TRANSFORMED_PREFIX", "transformed")


def _resolve_run_metadata(etl_run_id: Optional[str] = None, snapshot_date: Optional[str] = None) -> Dict[str, str]:
    """
    Resolve ETL run metadata for the current execution.
    """
    if etl_run_id and snapshot_date:
        return {"etl_run_id": etl_run_id, "snapshot_date": snapshot_date}

    now = datetime.now(timezone.utc)
    return {
        "etl_run_id": etl_run_id or now.strftime("%Y%m%d"),
        "snapshot_date": snapshot_date or now.strftime("%Y%m%d"),
    }


def _build_manifest_payload(
    *,
    bucket_name: str,
    snapshot_date: str,
    etl_run_id: str,
    raw_object_key: Optional[str],
    transformed_object_key: Optional[str],
) -> bytes:
    """Build the JSON payload containing metadata about the latest ETL run and uploaded files for downstream jobs."""
    return json.dumps(
        {
            "bucket": bucket_name,
            "latest_scraped_date": snapshot_date,
            "etl_run_id": etl_run_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "raw_file": raw_object_key,
            "transformed_file": transformed_object_key,
        },
        indent=2,
    ).encode("utf-8")


def _upload_object(
    *,
    upload_url: str,
    object_key: str,
    headers: Dict[str, str],
    payload,
) -> None:
    """Upload a file or JSON payload to Supabase Storage using the provided pre-signed URL and headers."""
    response = requests.post(
        upload_url,
        headers=headers,
        data=payload,
        timeout=UPLOAD_TIMEOUT_SECONDS,
    )

    if response.status_code >= 400:
        logger.error(
            "Supabase upload failed for %s (%s): %s",
            object_key,
            response.status_code,
            response.text,
        )
        response.raise_for_status()


def load_files_to_supabase(
    raw_file: str = DEFAULT_RAW_FILE,
    transformed_file: str = DEFAULT_TRANSFORMED_FILE,
    bucket_name: str = DEFAULT_SUPABASE_STORAGE_BUCKET,
    raw_prefix: str = DEFAULT_SUPABASE_RAW_PREFIX,
    transformed_prefix: str = DEFAULT_SUPABASE_TRANSFORMED_PREFIX,
    etl_run_id: Optional[str] = None,
    snapshot_date: Optional[str] = None,
) -> Dict[str, Dict[str, str]]:
    """Upload raw/transformed snapshots and update a manifest for downstream jobs."""
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    supabase_url = supabase_url.strip().rstrip("/")
    service_role_key = service_role_key.strip()
    run_metadata = _resolve_run_metadata(
        etl_run_id=etl_run_id,
        snapshot_date=snapshot_date,
    )
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "x-upsert": "true",
    }
    results: Dict[str, Dict[str, str]] = {}

    logger.info(" Starting to upload data csv files to Supabase storage")
    upload_targets = [
        {
            "result_key": "raw",
            "file_path": raw_file,
            "prefix": raw_prefix,
            "basename": "raw",
        },
        {
            "result_key": "transformed",
            "file_path": transformed_file,
            "prefix": transformed_prefix,
            "basename": "transformed",
        },
    ]

    for target in upload_targets:
        file_path = target["file_path"]
        if not file_path:
            logger.warning("%s file not provided, skipping", target["result_key"])
            continue

        if not os.path.exists(file_path):
            logger.warning(
                "%s file not found, skipping: %s",
                target["result_key"],
                file_path,
            )
            continue

        object_key = (
            f"{target['prefix']}/{target['basename']}_" f"{run_metadata['snapshot_date']}_{run_metadata['etl_run_id']}.csv"
        )
        upload_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{object_key}"

        logger.info(
            "Uploading %s file to %s/%s",
            target["result_key"],
            bucket_name,
            object_key,
        )
        with open(file_path, "rb") as source_file:
            _upload_object(
                upload_url=upload_url,
                object_key=object_key,
                headers={**headers, "Content-Type": "text/csv"},
                payload=source_file,
            )

        results[target["result_key"]] = {
            "bucket": bucket_name,
            "object_key": object_key,
            "file_path": file_path,
        }

    if not results:
        raise FileNotFoundError("No files loaded. Provide valid raw/transformed file paths.")

    logger.info(" Starting to upload manifest to Supabase storage")
    manifest_object_key = f"{raw_prefix}/_latest.json"
    manifest_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{manifest_object_key}"
    manifest_payload = _build_manifest_payload(
        bucket_name=bucket_name,
        snapshot_date=run_metadata["snapshot_date"],
        etl_run_id=run_metadata["etl_run_id"],
        raw_object_key=results.get("raw", {}).get("object_key"),
        transformed_object_key=results.get("transformed", {}).get("object_key"),
    )

    logger.info("Uploading manifest to %s/%s", bucket_name, manifest_object_key)
    _upload_object(
        upload_url=manifest_url,
        object_key=manifest_object_key,
        headers={**headers, "Content-Type": "application/json"},
        payload=manifest_payload,
    )

    results["raw_latest_manifest"] = {
        "bucket": bucket_name,
        "object_key": manifest_object_key,
    }

    logger.info("Supabase Storage uploads completed successfully")
    return results


if __name__ == "__main__":
    logger.info("RUNNING IN %s ENVIRONMENT", config.ENV_TYPE.upper())
    logger.info("\nConfiguration:")
    logger.info("  Raw file: %s", DEFAULT_RAW_FILE)
    logger.info("  Transformed file: %s", DEFAULT_TRANSFORMED_FILE)
    logger.info("  Supabase storage bucket: %s", DEFAULT_SUPABASE_STORAGE_BUCKET)
    logger.info("  Supabase raw prefix: %s", DEFAULT_SUPABASE_RAW_PREFIX)
    logger.info("  Supabase transformed prefix: %s", DEFAULT_SUPABASE_TRANSFORMED_PREFIX)

    try:
        start_time = datetime.now(timezone.utc)
        load_files_to_supabase()
        duration = datetime.now(timezone.utc) - start_time
        logger.info("DATA LOAD COMPLETED SUCCESSFULLY")
        logger.info("Duration: %s", duration)
    except Exception as exc:
        logger.error("DATA LOAD FAILED")
        logger.error("Error: %s", str(exc))
        raise SystemExit(1)
