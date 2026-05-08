import sys
import os
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion
from scripts.silver_layer import run_silver_transform
from scripts.quality_checks import run_quality_check
from scripts.gold_layer import run_gold_layer
from scripts.snowflake_conn import snowflake_load
from scripts.snowflake_backup import create_snapshot


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="flight_ops_medallion_pipe",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["flight", "medallion", "snowflake", "v1"],
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

    # Create Snowflake snapshot before loading new data
    snowflake_snapshot = PythonOperator(
        task_id="snowflake_snapshot",
        python_callable=create_snapshot,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    snowflake = PythonOperator(
        task_id="snowflake_load",
        python_callable=snowflake_load,
        provide_context=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt run \
            --select +gold \
            --target prod \
            --profiles-dir /opt/airflow/dbt
        """,
        env={
            'HOME': '/home/airflow',
            'SNOWFLAKE_ACCOUNT': '{{ conn.flight_snowflake.extra_dejson.account }}',
            'SNOWFLAKE_USER': '{{ conn.flight_snowflake.login }}',
            'SNOWFLAKE_PASSWORD': '{{ conn.flight_snowflake.password }}',
        },
    append_env = True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt test \
            --select +gold \
            --target prod \
            --profiles-dir /opt/airflow/dbt
            
        """
    )

    bronze >> silver >> quality_check >> gold
    gold >> snowflake_snapshot >> snowflake
    snowflake >> dbt_run >> dbt_test