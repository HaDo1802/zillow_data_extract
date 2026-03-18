import os
import random
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


def fetch_zillow(location, max_pages=2, snapshot_date=None):
    """Fetch property listings from Zillow API for a specific location.

    Pages are sampled randomly per run. `max_pages` controls how many pages
    are fetched (up to the API-reported totalPages).
    Seeding random with `snapshot_date` allows for reproducible sampling across runs on the same date.
    """

    if not config.RAPID_API_KEY:
        logger.error("RAPID_API_KEY not configured in environment")
        raise RuntimeError("RAPID_API_KEY not configured in environment")

    headers = {
        "x-rapidapi-key": config.RAPID_API_KEY,
        "x-rapidapi-host": "us-housing-market-data1.p.rapidapi.com",
    }
    url = "https://us-housing-market-data1.p.rapidapi.com/propertyExtendedSearch"
    all_props = []
    last_page_attempted = 1

    logger.info(f"Starting extraction for location: {location} (max_pages: {max_pages})")

    try:
        if max_pages < 1:
            logger.warning(f"max_pages={max_pages} for {location}; nothing to fetch. " "Use max_pages >= 1.")
            return pd.DataFrame()

        # Initial request to discover totalPages and validate response shape.
        first_page = 1
        params = {"location": location, "page": first_page}
        logger.info(f"Fetching discovery page {first_page} for {location}")
        request_start = time.time()
        res = requests.get(url, headers=headers, params=params, timeout=30)
        request_duration = round(time.time() - request_start, 2)

        if res.status_code != 200:
            logger.warning(
                f"API request failed for {location} page {first_page} - "
                f"Status: {res.status_code}, Duration: {request_duration}s"
            )
            return pd.DataFrame()

        json_data = res.json()
        if not json_data or "props" not in json_data:
            logger.warning(f"No property data in response for {location} page {first_page}")
            return pd.DataFrame()

        total_pages = int(json_data.get("totalPages", first_page))
        pages_to_fetch_count = min(max_pages, total_pages)
        if snapshot_date is not None:
            random.seed(int(snapshot_date))
        pages_to_fetch = random.sample(range(1, total_pages + 1), k=pages_to_fetch_count)

        logger.info(
            f"Random page sample for {location}: {sorted(pages_to_fetch)} " f"(requested={max_pages}, available={total_pages})"
        )

        processed_pages = 0

        for page in pages_to_fetch:
            last_page_attempted = page
            if page == first_page:
                props = json_data["props"]
                logger.info(
                    f"Page {page} fetched successfully (cached) - " f"Properties: {len(props)}, Duration: {request_duration}s"
                )
                all_props.extend(props)
                processed_pages += 1
                time.sleep(0.2)
                continue

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
                logger.warning(f"No property data in response for {location} page {page}")
                break

            props = json_data["props"]
            logger.info(f"Page {page} fetched successfully - " f"Properties: {len(props)}, Duration: {request_duration}s")
            all_props.extend(props)
            processed_pages += 1
            time.sleep(0.2)

        if not all_props:
            logger.error(f"No data extracted from {location} after {processed_pages} page(s)")
            return pd.DataFrame()
        df_raw = pd.DataFrame(all_props)
        df_raw["extracted_at"] = datetime.now(timezone.utc)

        logger.info(
            f"Extraction complete for {location} - " f"Total properties: {len(df_raw)}, Pages processed: {processed_pages}"
        )

        return df_raw

    except requests.exceptions.Timeout:
        logger.error(f"Request timeout for {location} page {last_page_attempted}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {location} page {last_page_attempted}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching {location}: {str(e)}", exc_info=True)
        raise


def fetch_all_locations(locations=None, max_pages=5, snapshot_date=None):
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
            df_result = fetch_zillow(
                loc,
                max_pages=max_pages,
                snapshot_date=snapshot_date,
            )

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
        raw_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

    os.makedirs(raw_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    raw_timestamped = os.path.join(raw_dir, f"raw_{timestamp}.csv")
    df_combined.to_csv(raw_timestamped, index=False)  # Consider using Polars for optimizing large files
    logger.info(f"Saved timestamped file: {raw_timestamped}")

    # Save latest file (for transform script to read)
    raw_latest = os.path.join(raw_dir, "raw_latest.csv")
    df_combined.to_csv(raw_latest, index=False)
    logger.info(f"Saved latest file: {raw_latest}")

    return df_combined


if __name__ == "__main__":
    logger.info("Starting Zillow data extraction script:")
    start_time = datetime.now(timezone.utc)
    df_result = fetch_all_locations(["Las Vegas, NV"], 1)
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
