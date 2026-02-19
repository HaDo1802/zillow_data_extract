import argparse
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql


DEFAULT_COLUMNS = (
    "address, bathrooms, bedrooms, brokerName, carouselPhotos, comingSoonOnMarketDate, "
    "contingentListingType, country, currency, datePriceChanged, daysOnZillow, detailUrl, "
    "has3DModel, hasImage, hasVideo, imgSrc, latitude, listingStatus, listingSubType, "
    "livingArea, longitude, lotAreaUnit, lotAreaValue, price, priceChange, propertyType, "
    "rentZestimate, variableData, zestimate, zpid, unit, newConstructionType, extracted_at, "
    "ingested_time, snapshot_date, source_file"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load a local CSV file into Supabase (Postgres) using COPY."
    )
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to the local CSV file you want to load.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("SUPABASE_SCHEMA", "public"),
        help="Target schema (default: public or SUPABASE_SCHEMA).",
    )
    parser.add_argument(
        "--table",
        default=os.getenv("SUPABASE_TABLE", "property_master_data"),
        help="Target table (default: property_master_data or SUPABASE_TABLE).",
    )
    parser.add_argument(
        "--columns",
        default=os.getenv("SUPABASE_COLUMNS", DEFAULT_COLUMNS),
        help="Comma-separated target columns in CSV order.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    db_host = os.getenv("SUPABASE_DB_HOST")
    db_port = os.getenv("SUPABASE_DB_PORT", "5432")
    db_name = os.getenv("SUPABASE_DB_NAME", "postgres")
    db_user = os.getenv("SUPABASE_DB_USER")
    db_password = os.getenv("SUPABASE_DB_PASSWORD")
    db_sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")

    missing = [
        name
        for name, value in (
            ("SUPABASE_DB_HOST", db_host),
            ("SUPABASE_DB_USER", db_user),
            ("SUPABASE_DB_PASSWORD", db_password),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    columns = [col.strip() for col in args.columns.split(",") if col.strip()]
    if not columns:
        raise ValueError("No columns provided. Set --columns or SUPABASE_COLUMNS.")

    copy_query = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    ).format(
        sql.Identifier(args.schema),
        sql.Identifier(args.table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            sslmode=db_sslmode,
        )
        cur = conn.cursor()

        print(f"Connecting to Supabase Postgres: {db_host}:{db_port}/{db_name}")
        print(f"Loading CSV: {csv_path} -> {args.schema}.{args.table}")
        with csv_path.open("r", encoding="utf-8") as f:
            cur.copy_expert(copy_query.as_string(conn), f)

        conn.commit()
        print("Load successful.")
    except Exception as e:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Load failed: {e}") from e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
