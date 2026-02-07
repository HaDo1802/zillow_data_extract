import os
import sys
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2 import sql


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from logger import get_logger  
from utils.config import config 
# Initialize logger for this module
logger = get_logger(__name__)

DEFAULT_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "transformed",
        "transformed_latest.csv",
    )
)

HISTORY_TABLE = "properties_data_history"

ORDERED_COLS = [
    "zillow_property_id",
    "snapshot_date",
    "price",
    "priceChange",
    "bedrooms",
    "bathrooms",
    "livingArea",
    "lotAreaValue",
    "Normalized_lotAreaValue",
    "propertyType",
    "listingStatus",
    "rentZestimate",
    "zestimate",
    "street_address",
    "city",
    "state",
    "zip_code",
    "vegas_district",
    "latitude",
    "longitude",
    "daysOnZillow",
    "has3DModel",
    "hasImage",
    "hasVideo",
    "is_fsba",
    "is_open_house",
    "extracted_at",
]


def get_connection():
    """Connect to Postgres using config with environment auto-detection."""

    db_config = config.get_db_config()
    logger.info(
        f"Connecting to PostgreSQL ({config.ENV_TYPE}): "
        f"{db_config['host']}:{db_config['port']}/{db_config['dbname']} "
        f"as {db_config['user']}"
    )

    return psycopg2.connect(**db_config)


def ensure_schema_and_objects(conn):
    """Create schema, tables, and views if they don't exist."""
    cur = conn.cursor()
    # Create schema
    logger.info(f"Creating schema if not exists: {config.DEFAULT_SCHEMA}")
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(config.DEFAULT_SCHEMA)))

    # Create history table
    logger.info(f"Creating table if not exists: {config.DEFAULT_SCHEMA}.{HISTORY_TABLE}")
    cur.execute(
        sql.SQL(
            f"""
        CREATE TABLE IF NOT EXISTS {config.DEFAULT_SCHEMA}.{HISTORY_TABLE} (
            zillow_property_id BIGINT,
            snapshot_date DATE,

            price DOUBLE PRECISION,
            priceChange DOUBLE PRECISION,
            bedrooms INTEGER,
            bathrooms DOUBLE PRECISION,
            livingArea DOUBLE PRECISION,
            lotAreaValue DOUBLE PRECISION,
            Normalized_lotAreaValue DOUBLE PRECISION,
            propertyType TEXT,
            listingStatus TEXT,

            rentZestimate DOUBLE PRECISION,
            zestimate DOUBLE PRECISION,

            street_address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            vegas_district TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,

            daysOnZillow INTEGER,
            has3DModel BOOLEAN,
            hasImage BOOLEAN,
            hasVideo BOOLEAN,
            is_fsba BOOLEAN,
            is_open_house BOOLEAN,
            extracted_at TIMESTAMPTZ,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        )
    )

    conn.commit()
    cur.close()
    logger.info("Schema and objects verified/created")


def load_csv(csv_file=DEFAULT_FILE):
    """Load transformed CSV data into PostgreSQL history table."""

    logger.info("STARTING DATA LOAD TO POSTGRESQL")
    logger.info(f"Loading file: {csv_file}")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Ensure schema and tables exist
        ensure_schema_and_objects(conn)

        # Read CSV file
        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        df = pd.read_csv(csv_file)[ORDERED_COLS]
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"].astype(str), format="%Y%m%d").dt.date
        logger.info(f"Loaded {len(df)} records from CSV")
        logger.info(f"Columns: {len(df.columns)}")

        df = df.where(pd.notna(df), None)
        df["loaded_at"] = datetime.now(timezone.utc)
        # Write to temporary file
        tmp_file = "/tmp/property_history_load.csv"
        df.to_csv(tmp_file, index=False)
        logger.info(f"Created temporary file: {tmp_file}")
        with open(tmp_file, "r", encoding="utf-8") as f:
            cur.copy_expert(
                sql.SQL("COPY {}.{} FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(config.DEFAULT_SCHEMA), sql.Identifier(HISTORY_TABLE)
                ),
                f,
            )

        conn.commit()

        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(config.DEFAULT_SCHEMA), sql.Identifier(HISTORY_TABLE))
        )
        total_rows = cur.fetchone()[0]

        logger.info("COPY completed successfully")
        logger.info(f"Total rows in history table: {total_rows}")

    except FileNotFoundError as e:
        conn.rollback()
        logger.error(f"File not found error: {str(e)}")
        raise
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Database error: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Unexpected error during load: {str(e)}", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    logger.info(f"RUNNING IN {config.ENV_TYPE.upper()} ENVIRONMENT")

    # Validate required environment variables based on environment
    db_config = config.get_db_config()

    logger.info("\nConfiguration:")
    logger.info(f"  Host: {db_config['host']}")
    logger.info(f"  Database: {db_config['dbname']}")
    logger.info(f"  User: {db_config['user']}")
    logger.info(f"  Port: {db_config['port']}")
    logger.info(f"  Schema: {config.DEFAULT_SCHEMA}")
    logger.info(f"  Table: {HISTORY_TABLE}")
    logger.info(f"  Input file: {DEFAULT_FILE}")

    try:
        conn = get_connection()
        conn.close()
        logger.info("Database connection successful!\n")

        start_time = datetime.now(timezone.utc)
        load_csv()
        duration = datetime.now(timezone.utc) - start_time

        logger.info("DATA LOAD COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration}")
        logger.info(f"Schema: {config.DEFAULT_SCHEMA}")
        logger.info(f"History table: {HISTORY_TABLE}")
        logger.info("\nQuery the data with:")

    except Exception as e:
        logger.error("DATA LOAD FAILED")
        logger.error(f"Error: {str(e)}")
        exit(1)
