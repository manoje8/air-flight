"""
Snowflake connector with batch operations, staging tables,
and multi-layer data loading (Bronze, Silver, Gold).
"""

import logging
import pandas as pd
from utils.snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)

def snowflake_load(**context):
    """
    Airflow task: Snowflake loader using batch operations.
    Loads Bronze, Silver, and Gold layers efficiently.
    """

    ti = context['ti']

    bronze_file = ti.xcom_pull(key='bronze_file', task_ids='bronze_ingest')
    silver_file = ti.xcom_pull(key='silver_file', task_ids='silver_transform')
    gold_file = ti.xcom_pull(key='gold_file', task_ids='gold_layer')

    exec_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")
    ingestion_branch = f"airflow-{context['dag_run'].run_id}"

    loader = SnowflakeLoader()
    loader.setup_tables()

    # raw JSON as VARIANT
    if bronze_file:
        bronze_rows = loader.load_bronze(bronze_file, ingestion_branch)
        logger.info(f"Bronze layer loaded: {bronze_rows} rows")

    # batch upload with write_pandas
    if silver_file:
        silver_df = pd.read_csv(silver_file)
        silver_rows = loader.load_silver_batch(silver_df, ingestion_branch)
        logger.info(f"Silver layer loaded: {silver_rows} rows")

    # staging + MERGE pattern
    if gold_file:
        gold_df = pd.read_csv(gold_file)
        gold_df = gold_df.rename(columns={'on_ground': 'ON_GROUND_SUM'})
        gold_rows = loader.load_gold_batch(gold_df, exec_date)
        logger.info(f"Gold layer loaded: {gold_rows} rows merged")

    ti.xcom_push('snowflake_metric', value={
        'bronze_rows': bronze_rows if bronze_file else 0,
        'silver_rows': silver_rows if silver_file else 0,
        'gold_rows': gold_rows if gold_file else 0,
        'timestamp': exec_date
    })