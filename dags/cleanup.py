"""
flight_cleanup DAG — File retention policy maintenance DAG.

Runs daily to delete Bronze files older than 7 days, preventing disk bloat.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

AIRFLOW_HOME = Path("/opt/airflow")
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.file_cleanup import run_bronze_cleanup  # noqa: E402
from scripts.failure_callback import on_failure_callback  # noqa: E402

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="flight_cleanup",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["flight", "maintenance", "cleanup"],
)
def cleanup_dag():

    @task
    def cleanup_bronze_files(**context) -> int:
        return run_bronze_cleanup(**context)

    cleanup_bronze_files()


cleanup_dag()
