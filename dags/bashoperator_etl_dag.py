import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl.email_notifier import EmailNotifier

# Add ETL directory to Python path
# sys.path.insert(0, "/opt/airflow")


def send_success_notification(**context):
    """Send success notification with pipeline metrics."""
    try:
        # Get pipeline metrics from XCom or calculate
        records_processed = context["task_instance"].xcom_pull(task_ids="upload_cleaned_data_to_postgres") or "Unknown"
        #records_processed_s3 = context["task_instance"].xcom_pull(task_ids="load_to_s3") or "Unknown"
        #records_processed = f"Postgres: {records_processed_postgres}"
        # Calculate duration using task instance timing
        task_instance = context["task_instance"]
        if task_instance.start_date and task_instance.end_date:
            duration = str(task_instance.end_date - task_instance.start_date).split(".")[0]
        else:
            # Fallback: calculate from data interval
            start_time = context["data_interval_start"]
            end_time = datetime.now(timezone.utc)
            duration = str(end_time - start_time).split(".")[0]

        details = {
            "records_processed": records_processed,
            "duration": duration,
            "location": "Las Vegas, NV",
            "dag_id": context["dag"].dag_id,
            "execution_date": context["ds"],
            "task_instance": context["task_instance"].task_id,
            "dag_run_id": context["dag_run"].run_id,
            "execution_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

        notifier = EmailNotifier()
        success = notifier.send_notification(success=True, details=details)

        if not success:
            logging.warning("Email notification failed, but pipeline completed successfully")
        else:
            logging.info("Success notification sent successfully")

    except Exception as e:
        logging.error(f"Error sending success notification: {e}")
        # Don't fail the DAG just because email failed


def send_failure_notification(context):
    """Send failure notification with error details."""
    try:
        # Get error information
        task_instance = context["task_instance"]
        exception = context.get("exception")

        details = {
            "error": str(exception) if exception else "Unknown error",
            "failed_task": task_instance.task_id,
            "dag_id": context["dag"].dag_id,
            "execution_date": context["ds"],
            "log_url": getattr(task_instance, "log_url", "N/A"),
            "failure_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "dag_run_id": context["dag_run"].run_id,
            "try_number": task_instance.try_number,
        }

        notifier = EmailNotifier()
        success = notifier.send_notification(success=False, details=details)

        if success:
            logging.info("Failure notification sent successfully")
        else:
            logging.error("Failed to send failure notification")

    except Exception as e:
        logging.error(f"Error sending failure notification: {e}")


# Default arguments for the DAG
default_args = {
    "owner": "hado",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 25),
    "email": ["havando1802@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "catchup": False,
    "on_failure_callback": send_failure_notification,
}

# Create the DAG
with DAG(
    "real_estate_etl_pipeline",
    default_args=default_args,
    description="Real Estate ETL Pipeline - Runs every day at 6 AM",
    schedule_interval="0 6 * * *",  # Every day at 6 AM
    max_active_runs=1,  # Only one instance running at a time
    tags=["real_estate", "zillow"],
    catchup=False,
) as dag:

    # Setup data directory
    setup_data_dir = BashOperator(
        task_id="setup_data_directory",
        bash_command="mkdir -p /opt/airflow/data && chmod 755 /opt/airflow/data",
    )

    # 1. scrape
    scrape_real_estate_data = BashOperator(
        task_id="scrape_real_estate_data",
        bash_command="python /opt/airflow/etl/extract.py ",
    )
    # 2. Log note
    note = BashOperator(
        task_id="note",
        bash_command="echo '✅ Extracted real estate data'",
    )

    # 3. Clean / transform
    clean_data = BashOperator(
        task_id="clean_data",
        bash_command="python /opt/airflow/etl/transform.py "
        "--input /opt/airflow/data/raw/raw_data.csv "
        "--output /opt/airflow/data/transformed/transformed_latest.csv "
        "--log-level INFO",
        retries=2,
    )

    note_clean_data = BashOperator(
        task_id="note_clean_data",
        bash_command="echo '✅ Cleaned data ready for loading'",
    )

    # 4. Upload cleaned data into Postgres
    upload_cleaned_data_to_postgres = BashOperator(
        task_id="upload_cleaned_data_to_postgres",
        bash_command="python /opt/airflow/etl/load.py",
        do_xcom_push=True,  # Enable XCom capture
    )
    # 5. Load to s3
    load_to_s3 = BashOperator(
        task_id="load_to_s3",
        bash_command="python /opt/airflow/etl/load_to_s3.py",
        do_xcom_push=True,
        
    )
    # 5. Send success email
    success_email = PythonOperator(
        task_id="send_success_email",
        python_callable=send_success_notification,
        provide_context=True,
    )


# Set task dependencies
(
    setup_data_dir
    >> scrape_real_estate_data
    >> note
    >> clean_data
    >> note_clean_data
    >> load_to_s3
    >> upload_cleaned_data_to_postgres
    >> success_email
)
