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
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion
from scripts.silver_layer import run_silver_transform
from scripts.quality_checks import run_quality_check
from scripts.gold_layer import run_gold_layer
from scripts.snowflake_conn import snowflake_load
from scripts.snowflake_backup import create_snapshot


_SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT":    os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER":       os.getenv("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD":   os.getenv("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_ROLE":       os.getenv("SNOWFLAKE_ROLE", ""),
    "SNOWFLAKE_DATABASE":   os.getenv("SNOWFLAKE_DATABASE", ""),
    "SNOWFLAKE_WAREHOUSE":  os.getenv("SNOWFLAKE_WAREHOUSE", ""),
    "SNOWFLAKE_SCHEMA":     os.getenv("SNOWFLAKE_SCHEMA", ""),
}



profile_config = ProfileConfig(
    profile_name="flight_analytics",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=os.getenv("SNOWFLAKE_CONN_ID"),
        profile_args={
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        },
    ),
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt",
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

render_config = RenderConfig(
    select=["tag:gold"]
)


def on_failure_callback(context):
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    from airflow.exceptions import AirflowNotFoundException
    msg = (
        f":red_circle: DAG *{context['dag'].dag_id}* failed.\n"
        f"Task: `{context['task_instance'].task_id}`\n"
        f"Run: `{context['run_id']}`\n"
        f"Log: {context['task_instance'].log_url}"
    )

    try:
        SlackWebhookOperator(
            task_id="slack_alert",
            slack_webhook_conn_id="slack_default",
            message=msg,
        ).execute(context)
    except AirflowNotFoundException:
        print("WARNING: slack_default connection not configured — skipping Slack alert")


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
    
    _DBT_ENV = {
        **_SNOWFLAKE_ENV,
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    }
    
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def snapshot(**context):
        return create_snapshot(**context)
    
    @task
    def load(bronze_file: str, silver_file: str, gold_file: str, **context):
        return snowflake_load(bronze_file=bronze_file, silver_file=silver_file, gold_file=gold_file, **context)
    


    bronze_run = DbtTaskGroup(
        group_id="bronze_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True, "env": _DBT_ENV},
        render_config=RenderConfig(select=["tag:bronze"])
    )

    silver_run = DbtTaskGroup(
        group_id="silver_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True, "env": _DBT_ENV},
        render_config=RenderConfig(select=["tag:silver"])
    )

    dbt_run = DbtTaskGroup(
        group_id="gold_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True, "env": _DBT_ENV},
        render_config=RenderConfig(select=["tag:gold"])
    )
    
    
    bronze_file = bronze()
    silver_file = silver(bronze_file)
    quality_report = quality(silver_file)
    gold_file = gold(silver_file, quality_report)
    snapshot_ts = snapshot()

    gold_file >> bronze_run >> silver_run >> dbt_run >> snapshot_ts
    load(bronze_file, silver_file, gold_file) << snapshot_ts


flight_pipeline()