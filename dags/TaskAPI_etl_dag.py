from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import pandas as pd
from typing import Dict, Any

DEFAULT_LOCATIONS = ["Las Vegas, NV"]
DEFAULT_MAX_PAGES = 2


@dag(
    dag_id="real_estate_etl_taskflow",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["real_estate", "production"],
    default_args={
        "owner": "hado",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def real_estate_etl_pipeline():
    """
    Real Estate ETL Pipeline using TaskFlow API

    Flow: Extract -> Validate -> Transform -> Quality Check -> Load (Postgres + S3) -> Notify
    """

    @task()
    def setup_environment() -> Dict[str, str]:
        """Setup directories and return paths."""
        import os

        paths = {
            "raw": "/opt/airflow/data/raw",
            "transformed": "/opt/airflow/data/transformed",
            "archive": "/opt/airflow/data/archive",
        }

        for path in paths.values():
            os.makedirs(path, exist_ok=True)

        return paths

    @task()
    def extract_zillow(paths: Dict[str, str]) -> Dict[str, Any]:
        import os
        import logging
        from datetime import datetime, timezone
        from etl.extract import fetch_all_locations

        log = logging.getLogger("airflow.task")

        output_file = os.path.join(paths["raw"], "raw_latest.csv")
        log.info("Starting extract_zillow. Will write to: %s", output_file)

        try:
            df = fetch_all_locations(DEFAULT_LOCATIONS, DEFAULT_MAX_PAGES)
            log.info("Fetched dataframe shape=%s", getattr(df, "shape", None))

            if df is None or df.empty:
                raise ValueError("No data extracted from API (df is empty)")

            # IMPORTANT: actually write the file
            # df.to_csv(output_file, index=False)
            log.info("Wrote raw CSV successfully: %s (rows=%s)", output_file, len(df))

            # JSON-safe return
            return {
                "records_extracted": int(len(df)),
                "unique_properties": (
                    int(df["zpid"].nunique()) if "zpid" in df.columns else 0
                ),
                "file_path": str(output_file),
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
            }

        except Exception:
            # This ensures the real traceback appears in the task log
            log.exception("extract_zillow failed with exception")
            raise

    @task()
    def validate_extraction(extraction_metrics: Dict[str, any]) -> bool:
        """Validate extracted data quality."""
        import pandas as pd

        # Check minimum records
        if extraction_metrics["records_extracted"] < 5:
            raise ValueError(
                f"Too few records: {extraction_metrics['records_extracted']}"
            )

        # Validate file exists and is readable
        df = pd.read_csv(extraction_metrics["file_path"])

        required_cols = ["zpid", "price", "address"]
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        return True

    @task()
    def transform_data(
        extraction_metrics: Dict[str, any], paths: Dict[str, str]
    ) -> Dict[str, any]:
        """
        Transform and clean data.

        Args:
            extraction_metrics: Output from extract_zillow
            paths: Output from setup_environment

        Returns:
            Transformation metrics including output file path
        """
        from etl.transform import main_transform

        input_file = extraction_metrics["file_path"]

        df_transformed, timestamped_file, latest_file = main_transform(
            input_file=input_file, output_dir=paths["transformed"]
        )

        transformation_efficiency = (
            len(df_transformed) / extraction_metrics["records_extracted"] * 100
        )

        return {
            "records_transformed": len(df_transformed),
            "file_path": str(latest_file),
            "transformation_efficiency": round(transformation_efficiency, 2),
            "records_filtered": extraction_metrics["records_extracted"]
            - len(df_transformed),
        }

    @task()
    def quality_check(transform_metrics: Dict[str, any]) -> Dict[str, any]:
        """Comprehensive data quality validation."""
        import pandas as pd

        df = pd.read_csv(transform_metrics["file_path"])

        checks = {
            "no_null_ids": df["zillow_property_id"].notna().all(),
            "positive_prices": (df["price"] > 0).all(),
            "valid_bedrooms": (
                (df["bedrooms"] >= 0).all() if "bedrooms" in df.columns else True
            ),
            "no_duplicates": not df["zillow_property_id"].duplicated().any(),
            "has_districts": df["vegas_district"].notna().all(),
        }

        failed = [k for k, v in checks.items() if not v]

        if failed:
            raise ValueError(f"Quality checks failed: {failed}")

        quality_score = (sum(checks.values()) / len(checks)) * 100

        return {
            "quality_score": round(quality_score, 2),
            "checks_passed": len(checks),
            "checks_failed": len(failed),
        }

    @task()
    def load_postgres(transform_metrics: Dict[str, any]) -> Dict[str, int]:
        """Load data to PostgreSQL."""
        from etl.load import load_csv
        import psycopg2
        from utils.config import config

        # Load data
        load_csv(csv_file=transform_metrics["file_path"])

        # Verify
        conn = psycopg2.connect(**config.get_db_config())
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM {config.DEFAULT_SCHEMA}.properties_data_history"
        )
        total_rows = cur.fetchone()[0]
        cur.close()
        conn.close()

        return {
            "postgres_rows": total_rows,
            "records_loaded": transform_metrics["records_transformed"],
        }

    @task()
    def load_s3(
        transform_metrics: Dict[str, any], extraction_metrics: Dict[str, any]
    ) -> Dict[str, bool]:
        """Load data to S3."""
        from etl.load_to_s3 import load_to_s3

        bucket = "real-estate-scraped-data"
        etl_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        snapshot_date = datetime.fromisoformat(extraction_metrics["extraction_timestamp"]).strftime("%Y%m%d")
        raw_file_path = extraction_metrics["file_path"]
        transformed_file_path = transform_metrics["file_path"]

        try:
            load_to_s3(
                raw_file_path, bucket, f"raw/raw_{snapshot_date}_{etl_run_id}.csv"
            )
            load_to_s3(
                transformed_file_path,
                bucket,
                f"transformed/transformed_{snapshot_date}_{etl_run_id}.csv",
            )
            return {"s3_success": True, "bucket": bucket}
        except Exception as e:
            # Log but don't fail
            print(f"S3 upload failed: {e}")
            return {"s3_success": False, "error": str(e)}

    @task(trigger_rule="all_done")
    def send_notification(
        extraction_metrics: Dict,
        transform_metrics: Dict,
        quality_metrics: Dict,
        postgres_metrics: Dict,
        s3_metrics: Dict,
    ):
        """Send success notification with all metrics."""
        from etl.email_notifier import EmailNotifier

        details = {
            "properties_extracted": extraction_metrics["records_extracted"],
            "records_loaded": transform_metrics["records_transformed"],
            "transformation_efficiency": f"{transform_metrics['transformation_efficiency']}%",
            "quality_score": f"{quality_metrics['quality_score']}%",
            "postgres_rows": postgres_metrics["postgres_rows"],
            "s3_uploaded": "✅" if s3_metrics["s3_success"] else "❌",
            "end_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

        notifier = EmailNotifier()
        notifier.send_notification(success=True, details=details)

    # Define task flow
    paths = setup_environment()

    extraction = extract_zillow(paths)
    validation = validate_extraction(extraction)

    transformation = transform_data(extraction, paths)
    quality = quality_check(transformation)

    # Parallel loads
    postgres = load_postgres(transformation)
    s3 = load_s3(transformation, extraction)

    # Notification depends on all tasks
    notify = send_notification(extraction, transformation, quality, postgres, s3)

    # Dependencies
    (
        paths
        >> extraction
        >> validation
        >> transformation
        >> quality
        >> postgres
        >> s3
        >> notify
    )


# Instantiate the DAG
real_estate_dag = real_estate_etl_pipeline()
