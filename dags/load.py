import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule


AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.failure_callback import on_failure_callback
from scripts.snowflake_backup import create_snapshot
from scripts.snowflake_conn import snowflake_load

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

@dag(
    dag_id="flight_load",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=Variable.get("FLIGHT_SCHEDULE_INTERVAL", default_var="*/30 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["flight", "medallion", "snowflake", "v1"],
)
def load():

    @task
    def get_file_paths(**context):
        conf = context['dag_run'].conf or {}

        missing = [k for k in ('bronze_file', 'silver_file', 'gold_file') if not conf.get(k)]

        if missing:
            raise ValueError(f"load_dag: missing conf keys: {missing}")

        return {
            'bronze_file': conf['bronze_file'],
            'silver_file': conf['silver_file'],
            'gold_file': conf['gold_file']
        }

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def snapshot(**context):
        return create_snapshot(**context)

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def load(paths: dict, **context):
        return snowflake_load(
            bronze_file=paths['bronze_file'],
            silver_file=paths['silver_file'],
            gold_file=paths['gold_file'],
            **context
        )

    paths = get_file_paths()
    snapshot = snapshot()

    paths >> snapshot >> load(paths)


load()