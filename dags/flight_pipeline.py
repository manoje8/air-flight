import sys
import os
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
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


def     on_failure_callback(context):
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    msg = (
        f":red_circle: DAG *{context['dag'].dag_id}* failed.\n"
        f"Task: `{context['task_instance'].task_id}`\n"
        f"Run: `{context['run_id']}`\n"
        f"Log: {context['task_instance'].log_url}"
    )
    SlackWebhookOperator(
        task_id="slack_alert",
        slack_webhook_conn_id="slack_default",
        message=msg,
    ).execute(context)


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

@dag(
    dag_id="flight_ops_medallion_pipe",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=Variable.get("FLIGHT_SCHEDULE_INTERVAL", default_var="*/30 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["flight", "medallion", "snowflake", "v1"],
)


def flight_pipeline():
    
    @task(retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=10))
    def bronze(**context):
        return run_bronze_ingestion(**context)
    
    @task
    def silver(bronze_file: str, **context):
        return run_silver_transform(bronze_file=bronze_file, **context)
    
    @task
    def quality(silver_file: str, **context):
        return run_quality_check(silver_file=silver_file, **context)
    
    @task
    def gold(silver_file: str, quality_report: dict, **context):
        if quality_report["row_count"] == 0:
            raise ValueError("Quality check reported zero rows — aborting gold layer.")
        return run_gold_layer(silver_file=silver_file, **context)
    
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def snapshot(**context):
        return create_snapshot(**context)
    
    @task
    def load(bronze_file: str, silver_file: str, gold_file: str, **context):
        return snowflake_load(bronze_file=bronze_file, silver_file=silver_file, gold_file=gold_file, **context)
    

    # TODO: Fix dbt tasks for gold layer transformations and testing
    # dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command="""
    #         echo "Current directory: $(pwd)"
    #         echo "Checking dbt project files:"
    #         ls -la /opt/airflow/dbt/
    #         echo "Running dbt..."
    #         cd /opt/airflow/dbt && \
    #         dbt run \
    #         --select tag:gold \
    #         --target dev \
    #         --profiles-dir /opt/airflow/dbt \
    #         --project-dir /opt/airflow/dbt
    #     """,
    #     env={
    #         "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    #         "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
    #         "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
    #         "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE", ""),
    #         "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", ""),
    #         "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
    #         "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", ""),
    #     },
    # )

    # dbt_test = BashOperator(
    #     task_id="dbt_test",
    #     bash_command="""
    #         cd /opt/airflow/dbt && \
    #         dbt test \
    #         --select tag:gold \
    #         --target dev \
    #         --profiles-dir /opt/airflow/dbt
    #     """,
    # )
    
    bronze_file = bronze()
    silver_file = silver(bronze_file)
    quality_report = quality(silver_file)
    gold_file = gold(silver_file, quality_report)
    snapshot_ts = snapshot()

    gold_file >> snapshot_ts
    load(bronze_file, silver_file, gold_file) << snapshot_ts


flight_pipeline()