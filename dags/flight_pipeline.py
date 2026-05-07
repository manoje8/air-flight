import sys
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion
from scripts.silver_layer import run_silver_transform
from scripts.quality_checks import run_quality_check
from scripts.gold_layer import run_gold_layer
from scripts.snowflake_conn import snowflake_load


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_ops_medallion_pipe",
    default_args=default_args,
    start_date=datetime(2025, 12, 10),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["flight", "medallion", "v1"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
    )

    gold = PythonOperator(
        task_id="gold_layer",
        python_callable=run_gold_layer,
    )

    snowflake = PythonOperator(
        task_id="snowflake_load",
        python_callable=snowflake_load,
    )

    bronze >> silver >> quality_check >> gold >> snowflake