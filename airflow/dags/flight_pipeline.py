from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Default arguments ────────────────────────────────────
# these apply to every task in the DAG unless overridden
default_args = {
    "owner":            "vivek",
    "retries":          1,                        # retry once if a task fails
    "retry_delay":      timedelta(minutes=5),     # wait 5 mins before retrying
    "email_on_failure": False,
}

# ── DAG definition ───────────────────────────────────────
with DAG(
    dag_id="flight_pipeline",                     # unique name shown in UI
    default_args=default_args,
    description="End to end flight data pipeline for India",
    schedule_interval="0 6 * * *",               # runs every day at 6AM IST
    start_date=datetime(2026, 3, 7),
    catchup=False,                                # don't backfill missed runs
    tags=["flights", "gcp", "pyspark"],
) as dag:

    # ── Task 1: Fetch raw flights from OpenSky API ───────
    fetch_flights = BashOperator(
        task_id="fetch_flights",
        bash_command="cd /opt/airflow/ingestion && python fetch_flights.py",
    )

    # ── Task 2: PySpark transformation ──────────────────
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command="cd /opt/airflow/ingestion && python spark_transform.py",
    )

    # ── Task 3: Load Parquet into BigQuery staging ───────
    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command="cd /opt/airflow/ingestion && python load_to_bigquery.py",
    )

    # ── Task 4: SQL transformations → flight_marts ───────
    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow/ingestion && python transform.py",
    )

    # ── Task dependencies ────────────────────────────────
    # this defines the order — each task waits for the previous to succeed
    fetch_flights >> spark_transform >> load_to_bigquery >> transform