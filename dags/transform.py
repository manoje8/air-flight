"""
Silver transform → DataQualityOperator gate → Gold layer → dbt runs.
Receives ``bronze_file`` from ingest_dag via DagRun conf.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.failure_callback import on_failure_callback  # noqa: E402
from scripts.quality_checks import run_quality_check  # noqa: E402
from scripts.silver_layer import run_silver_transform  # noqa: E402
from scripts.gold_layer import run_gold_layer  # noqa: E402
from dags.data_quality import DataQualityOperator  # noqa: E402


_SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE", ""),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", ""),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
    "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", ""),
}

_DBT_ENV = {
    **_SNOWFLAKE_ENV,
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
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
project_config = ProjectConfig(dbt_project_path="/opt/airflow/dbt")
execution_config = ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt")
render_config = RenderConfig(select=["tag:gold"])

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="flight_transform",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # triggered externally by ingest_dag
    catchup=False,
    max_active_runs=1,
    tags=["flight", "transform", "silver", "gold", "snowflake", "v1"],
)
def transform():
    @task
    def get_bronze_file(**context):
        bronze_file: str = context["dag_run"].conf.get("bronze_file", "")

        if not bronze_file:
            raise ValueError("transform_dag: no bronze_file received in dag_run.conf.")
        return bronze_file

    @task
    def silver(bronze_file: str, **context):
        return run_silver_transform(bronze_file=bronze_file, **context)

    @task
    def quality(silver_file: str, **context):
        return run_quality_check(silver_file=silver_file, **context)

    dq_gate = DataQualityOperator(
        task_id="quality_gate",
        source_task_id="quality",
        min_row_count=1,
        fail_on_empty=True,
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    @task
    def gold(silver_file: str, **context):

        return run_gold_layer(silver_file=silver_file, **context)

    bronze_run = DbtTaskGroup(
        group_id="bronze_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True, "env": _DBT_ENV},
        render_config=RenderConfig(select=["tag:bronze"]),
    )

    silver_run = DbtTaskGroup(
        group_id="silver_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True, "env": _DBT_ENV},
        render_config=RenderConfig(select=["tag:silver"]),
    )

    gold_run = DbtTaskGroup(
        group_id="gold_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True, "env": _DBT_ENV},
        render_config=RenderConfig(select=["tag:gold"]),
    )

    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="flight_load",
        conf={
            "bronze_file": "{{ ti.xcom_pull(task_ids='get_bronze_file') }}",
            "silver_file": "{{ ti.xcom_pull(task_ids='silver') }}",
            "gold_file": "{{ ti.xcom_pull(task_ids='gold') }}",
        },
        wait_for_completion=False,
    )

    bronze_file = get_bronze_file()
    silver_file = silver(bronze_file)
    quality_report = quality(silver_file)

    quality_report >> dq_gate

    gold_file = gold(silver_file)

    dq_gate >> gold_file

    # dbt runs after Gold task succeeds
    gold_file >> bronze_run >> silver_run >> gold_run >> trigger_load


transform()
