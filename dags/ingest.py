import sys
from pathlib import Path
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion
from scripts.failure_callback import on_failure_callback

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

@dag(
    dag_id="flight_ingest",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=Variable.get("FLIGHT_SCHEDULE_INTERVAL", default_var="*/30 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["flight", "ingest", "bronze", "snowflake", "v1"],
)

def ingest():
    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10)
    )
    def bronze(**context):
        return run_bronze_ingestion(**context)

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="flight_transform",
        conf={"bronze_file": "{{ ti.xcom_pull(task_ids='bronze') }}"},
        wait_for_completion=False, # fire and forget
    )

    bronze() >> trigger_transform

ingest()