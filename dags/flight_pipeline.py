import sys
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion
from scripts.silver_layer import run_silver_transform
from scripts.quality_checks import run_quality_check
from scripts.gold_layer import run_gold_layer
from scripts.snowflake_conn import snowflake_load_bronze, snowflake_load_silver, snowflake_load_gold


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

    snowflake_bronze = PythonOperator(
        task_id="snowflake_load_bronze",
        python_callable=snowflake_load_bronze,
    )

    snowflake_silver = PythonOperator(
        task_id="snowflake_load_silver",
        python_callable=snowflake_load_silver,
    )

    snowflake_gold = PythonOperator(
        task_id="snowflake_load_gold",
        python_callable=snowflake_load_gold,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/flight_dbt && dbt run --profiles-dir .",
        env={
            "SNOWFLAKE_ACCOUNT": "{{ conn.flight_snowflake.extra_dejson.account }}",
            "SNOWFLAKE_USER": "{{ conn.flight_snowflake.login }}",
            "SNOWFLAKE_PASSWORD": "{{ conn.flight_snowflake.password }}",
            "SNOWFLAKE_ROLE": "{{ conn.flight_snowflake.extra_dejson.role }}",
            "SNOWFLAKE_WAREHOUSE": "{{ conn.flight_snowflake.extra_dejson.warehouse }}",
        }
    )
    
    # Dependencies
    bronze >> snowflake_bronze
    bronze >> silver >> quality_check >> gold >> snowflake_gold
    quality_check >> snowflake_silver

    [snowflake_bronze, snowflake_silver, snowflake_gold] >> dbt_run