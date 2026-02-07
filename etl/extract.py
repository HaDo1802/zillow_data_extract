import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from logger import get_logger  # noqa: E402

from utils.config import config  # noqa: E402

logger = get_logger(__name__)


def fetch_zillow(location, max_pages=2):
    """Fetch property listings from Zillow API for a specific location."""

    if not config.RAPID_API_KEY:
        logger.error("RAPID_API_KEY not configured in environment")
        raise RuntimeError("RAPID_API_KEY not configured in environment")

    headers = {
        "x-rapidapi-key": config.RAPID_API_KEY,
        "x-rapidapi-host": "us-housing-market-data1.p.rapidapi.com",
    }
    url = "https://us-housing-market-data1.p.rapidapi.com/propertyExtendedSearch"
    all_props = []
    page = 1

    logger.info(
        f"Starting extraction for location: {location} (max_pages: {max_pages})"
    )

    try:
        while page <= max_pages:
            params = {
                "location": location,
                "page": page,
            }

            logger.info(f"Fetching page {page} for {location}")
            request_start = time.time()

            res = requests.get(url, headers=headers, params=params, timeout=30)
            request_duration = round(time.time() - request_start, 2)

            if res.status_code != 200:
                logger.warning(
                    f"API request failed for {location} page {page} - "
                    f"Status: {res.status_code}, Duration: {request_duration}s"
                )
                break

            json_data = res.json()

            if not json_data or "props" not in json_data:
                logger.warning(
                    f"No property data in response for {location} page {page}"
                )
                break

            props = json_data["props"]
            logger.info(
                f"Page {page} fetched successfully - "
                f"Properties: {len(props)}, Duration: {request_duration}s"
            )
            all_props.extend(props)

            # Pagination logic
            total_pages = int(json_data.get("totalPages", page))
            if page >= total_pages:
                logger.info(f"Reached last page ({total_pages}) for {location}")
                break

            page += 1
            time.sleep(0.2)

        if not all_props:
            logger.error(f"No data extracted from {location} after {page - 1} page(s)")
            return pd.DataFrame()
        df_raw = pd.DataFrame(all_props)
        df_raw["extracted_at"] = datetime.now(timezone.utc)

        logger.info(
            f"Extraction complete for {location} - "
            f"Total properties: {len(df_raw)}, Pages processed: {page - 1}"
        )

        return df_raw

    except requests.exceptions.Timeout as e:
        logger.error(f"Request timeout for {location} page {page}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {location} page {page}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching {location}: {str(e)}", exc_info=True)
        raise


def fetch_all_locations(locations=None, max_pages=2):
    """
    Fetch data from all configured locations and combine into single file.

    Returns:
        pd.DataFrame: Combined data from all locations
    """
    if locations is None:
        LOCATIONS = ["Las Vegas, NV"]
    else:
        LOCATIONS = locations

    logger.info("STARTING DATA EXTRACTION FROM ALL LISTED LOCATIONS")
    logger.info(f"Locations configured: {len(LOCATIONS)}")
    logger.info(f"Target locations: {', '.join(LOCATIONS)}")

    all_data = []
    total_properties = 0
    successful_locations = 0
    failed_locations = 0

    for loc in LOCATIONS:
        logger.info(f"Processing location: {loc}")
        try:
            df_result = fetch_zillow(loc, max_pages=max_pages)

            if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                all_data.append(df_result)
                total_properties += len(df_result)
                successful_locations += 1
                logger.info(f"SUCCESS - Fetched {len(df_result)} properties from {loc}")
            else:
                failed_locations += 1
                logger.warning(f"FAILED - No data fetched for {loc}")

            # Rate limiting between locations
            time.sleep(2)

        except Exception as e:
            failed_locations += 1
            logger.error(f"FAILED - Error processing {loc}: {str(e)}", exc_info=True)
            continue

    # Combine all location data
    if not all_data:
        logger.error("EXTRACTION FAILED - No data extracted from any location")
        return pd.DataFrame()

    df_combined = pd.concat(all_data, ignore_index=True)

    initial_count = len(df_combined)
    df_combined = df_combined.drop_duplicates(subset=["zpid"], keep="first")
    duplicates = initial_count - len(df_combined)

    logger.info("EXTRACTION SUMMARY")
    logger.info(f"Locations processed: {len(LOCATIONS)}")
    logger.info(f"Successful: {successful_locations}")
    logger.info(f"Failed: {failed_locations}")
    logger.info(f"Total properties fetched: {initial_count}")
    logger.info(f"Unique properties: {len(df_combined)}")
    logger.info(f"Duplicates removed: {duplicates}")

    # Save combined files
    if os.path.exists("/opt/airflow"):
        raw_dir = "/opt/airflow/data/raw"
    else:
        raw_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "data", "raw"
        )

    os.makedirs(raw_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    raw_timestamped = os.path.join(raw_dir, f"raw_{timestamp}.csv")
    df_combined.to_csv(
        raw_timestamped, index=False
    )  # Consider using Polars for optimizing large files
    logger.info(f"Saved timestamped file: {raw_timestamped}")

    # Save latest file (for transform script to read)
    raw_latest = os.path.join(raw_dir, "raw_latest.csv")
    df_combined.to_csv(raw_latest, index=False)
    logger.info(f"Saved latest file: {raw_latest}")

    return df_combined


if __name__ == "__main__":
    logger.info("Starting Zillow data extraction script:")
    start_time = datetime.now(timezone.utc)
    df_result = fetch_all_locations(["Las Vegas, NV"], 2)
    duration = datetime.now(timezone.utc) - start_time
    if not df_result.empty:
        logger.info("\n" + "=" * 70)
        logger.info("EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"Total unique properties: {len(df_result)}")
        logger.info(f"Total duration: {duration}")

        if "homeStatus" in df_result.columns:
            logger.info("\nProperty Status Breakdown:")
            status_counts = df_result["homeStatus"].value_counts()
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count}")

        logger.info("=" * 70)
    else:
        logger.error("\n" + "=" * 70)
        logger.error("EXTRACTION FAILED - No data retrieved")
        logger.error("=" * 70)
        raise SystemExit(1)
