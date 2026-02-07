import csv
import json
import os
from datetime import datetime, timezone

import pandas as pd
from logger import get_logger

logger = get_logger(__name__)

DEFAULT_INPUT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw", "raw_latest.csv"))
DEFAULT_OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "transformed"))
DISTRICT_MAPPING_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "utils", "district_map.csv"))


def load_district_mapping(file_path: str = DISTRICT_MAPPING_FILE) -> dict:
    """Load district mapping from CSV (keyword,district)."""
    if not os.path.exists(file_path):
        logger.warning("District mapping file not found, using defaults: %s", file_path)
        raise FileNotFoundError(f"District mapping file not found: {file_path}")

    mapping = {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                keyword = (row.get("keyword") or "").strip().lower()
                district = (row.get("district") or "").strip()
                if keyword and district:
                    mapping[keyword] = district
    except Exception as e:
        logger.warning("Failed to load district mapping, using defaults: %s", str(e))
        raise e

    return mapping 



def extract_address_components(address: str) -> dict:
    """Extract street, city, state, zip from address string."""
    if not address or pd.isna(address):
        return {"street_address": None, "city": None, "state": None, "zip_code": None}
    parts = address.split(", ")

    if len(parts) >= 3:
        street_address = parts[0]
        city = parts[1]
        state_zip = parts[2].split(" ")
        state = state_zip[0] if state_zip else None
        zip_code = state_zip[1] if len(state_zip) > 1 else None
    else:
        street_address = address
        city = None
        state = None
        zip_code = None

    return {
        "street_address": street_address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
    }


def convert_unix_timestamp(timestamp_value) -> datetime:
    """Convert Unix timestamp (in milliseconds) to datetime."""
    if pd.isna(timestamp_value) or not timestamp_value:
        return None
    try:
        # Handle both string and numeric timestamps
        if isinstance(timestamp_value, str):
            timestamp_value = float(timestamp_value)

        # Convert from milliseconds to seconds
        timestamp_seconds = float(timestamp_value) / 1000.0

        # Convert to datetime
        return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def extract_listing_subtype_info(listing_subtype) -> dict:
    """Extract FSBA and open house flags from listing subtype."""
    default_flags = {"is_fsba": False, "is_open_house": False}

    if not listing_subtype or pd.isna(listing_subtype):
        return default_flags

    if isinstance(listing_subtype, str):
        try:
            listing_subtype = json.loads(listing_subtype.replace("'", '"'))
        except json.JSONDecodeError:
            return default_flags

    if isinstance(listing_subtype, dict):
        return {
            "is_fsba": listing_subtype.get("is_FSBA", False),
            "is_open_house": listing_subtype.get("is_openHouse", False),
        }

    return default_flags


def normalize_lot_area_value(area_value, unit_value) -> float:
    """
    Normalize lot area: convert acres to sqft (1 acre = 43560 sqft).
    """
    if pd.isna(area_value) or area_value is None:
        return None

    if unit_value is not None and not pd.isna(unit_value):
        unit_str = str(unit_value).lower()
        if "acre" in unit_str:
            return round(area_value * 43560.0, 2)

    return area_value


def extract_vegas_district(address: str, city: str) -> str:
    """Extract Vegas district/neighborhood from address."""
    if not address or pd.isna(address):
        return "Unknown"

    address_lower = address.lower()
    DISTRICT_MAPPING = load_district_mapping()

    # Check address for district keywords
    for keyword, district in DISTRICT_MAPPING.items():
        if keyword in address_lower:
            return district

    # Fallback to city name if available
    if city and not pd.isna(city):
        city_clean = str(city).strip()
        if city_clean:
            return city_clean

    return "Las Vegas"


def main_transform(input_file=DEFAULT_INPUT, output_dir=DEFAULT_OUTPUT_DIR):
    """Main transformation function to clean and enrich raw property data."""

    try:
        logger.info("STARTING DATA TRANSFORMATION")
        logger.info(f"Reading input file: {input_file}")

        if not os.path.exists(input_file):
            logger.error(f"Input file does not exist: {input_file}")
            raise FileNotFoundError(f"Input file does not exist: {input_file}")

        df = pd.read_csv(input_file)
        initial_count = len(df)
        logger.info(f"Loaded {initial_count} raw records")
        logger.info(f"Columns found: {len(df.columns)}")

        df_transformed = df.copy()
        logger.info("Step 1: Extracting address components...")
        if "address" in df_transformed.columns:
            address_components = df_transformed["address"].apply(extract_address_components)
            for component in ["street_address", "city", "state", "zip_code"]:
                df_transformed[component] = address_components.apply(lambda x: x.get(component))
            logger.info("Address components extracted successfully")
        else:
            df_transformed["street_address"] = None
            df_transformed["city"] = None
            df_transformed["state"] = None
            df_transformed["zip_code"] = None
            logger.warning("'address' column not found, skipped")

        logger.info("Step 2: Extracting listing subtype information...")
        if "listingSubType" in df_transformed.columns:
            listing_flags = df_transformed["listingSubType"].apply(extract_listing_subtype_info)
            df_transformed["is_fsba"] = listing_flags.apply(lambda x: x.get("is_fsba"))
            df_transformed["is_open_house"] = listing_flags.apply(lambda x: x.get("is_open_house"))
            logger.info("Listing subtype flags extracted")
        else:
            df_transformed["is_fsba"] = False
            df_transformed["is_open_house"] = False
            logger.warning("'listingSubType' column not found, using defaults")

        # Convert Unix timestamp fields
        logger.info("Step 3: Converting timestamp fields...")
        timestamp_fields = ["datePriceChanged"]
        for field in timestamp_fields:
            if field in df_transformed.columns:
                df_transformed[field] = df_transformed[field].apply(convert_unix_timestamp)
                logger.info(f"Converted {field} to datetime")

        # Add snapshot column
        logger.info("Step 4: Add snapshot dates...")
        current_date = datetime.now(timezone.utc).date().strftime("%Y%m%d")
        df_transformed["snapshot_date"] = current_date
        logger.info("Snapshot dates added")

        # Normalize lot area to sqft
        logger.info("Step 5: Normalizing lot area to square feet...")
        df_transformed["Normalized_lotAreaValue"] = df_transformed.apply(
            lambda row: normalize_lot_area_value(row.get("lotAreaValue"), row.get("lotAreaUnit")),
            axis=1,
        )
        df_transformed["lotAreaUnit"] = "sqft"
        logger.info("Lot areas normalized to sqft")

        # Extract Vegas districts
        logger.info("Step 6: Extracting Vegas districts...")
        df_transformed["vegas_district"] = df_transformed.apply(
            lambda row: extract_vegas_district(row.get("address", ""), row.get("city", "")),
            axis=1,
        )
        district_counts = df_transformed["vegas_district"].value_counts()
        logger.info(f"Identified {len(district_counts)} unique districts")

        # Rename zpid to zillow_property_id
        df_final = df_transformed.rename(columns={"zpid": "zillow_property_id"})
        df_final = df_final.drop(columns=["address", "extraction_location"], errors="ignore")
        # Remove unnecessary columns
        logger.info("Step 7: Removing unnecessary columns...")
        columns_to_delete = [
            "variableData",
            "currency",
            "Country",
            "lotAreaUnit",
            "imgSrc",
            "carouselPhotos",
            "listingSubType",
            "detailUrl",
        ]
        deleted_cols = [col for col in columns_to_delete if col in df_final.columns]
        df_final.drop(columns=deleted_cols, inplace=True)
        logger.info(f"Removed {len(deleted_cols)} unnecessary columns")

        current_time = datetime.now(timezone.utc)
        logger.info(f"Processed at: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        essential_fields = ["zillow_property_id", "price"]
        before_filter = len(df_final)
        df_final = df_final.dropna(subset=essential_fields)
        removed_count = before_filter - len(df_final)

        if removed_count > 0:
            logger.warning(f"Removed {removed_count} records with missing critical fields")
        else:
            logger.info("All records passed essential field validation")

        final_count = len(df_final)
        logger.info(f"Final record count: {final_count}")

        os.makedirs(output_dir, exist_ok=True)
        timestamp = current_time.strftime("%Y%m%d")
        timestamped_file = os.path.join(output_dir, f"transformed_{timestamp}.csv")
        df_final.to_csv(timestamped_file, index=False)
        logger.info(f"Saved timestamped file: {timestamped_file}")

        latest_file = os.path.join(output_dir, "transformed_latest.csv")
        df_final.to_csv(latest_file, index=False)
        logger.info(f"Saved latest file: {latest_file}")

        logger.info("TRANSFORMATION SUMMARY")
        logger.info(f"Input file: {os.path.basename(input_file)}")
        logger.info(f"Initial records: {initial_count}")
        logger.info(f"Final records: {final_count}")
        filtered_count = initial_count - final_count
        filter_rate = (filtered_count / initial_count) * 100
        logger.info(f"Records filtered: {filtered_count} ({filter_rate:.1f}%)")
        logger.info(f"Output directory: {output_dir}")

        return df_final, timestamped_file, latest_file

    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
        raise
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    is_docker = os.path.exists("/opt/airflow")
    env_name = "Docker/Airflow" if is_docker else "Local"
    logger.info(f"Running in {env_name} environment\n")
    try:
        start_time = datetime.now(timezone.utc)
        df_transformed, timestamped_path, latest_path = main_transform()
        duration = datetime.now(timezone.utc) - start_time
        logger.info("TRANSFORMATION COMPLETED SUCCESSFULLY")
        logger.info(f"Total duration: {duration}")
        logger.info(f"Records transformed: {len(df_transformed)}")

    except Exception as e:
        logger.error("TRANSFORMATION FAILED")
        logger.error(f"Error: {str(e)}")
        exit(1)
