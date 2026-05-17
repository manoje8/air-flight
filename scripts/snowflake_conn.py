"""
Snowflake connector with batch operations, staging tables,
and multi-layer data loading (Bronze, Silver, Gold).
"""

import logging
from pathlib import Path
import pandas as pd
from utils.snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)


def snowflake_load(bronze_file: str, silver_file: str, gold_file: str, **context):
    """
    Airflow task: Snowflake loader using batch operations.
    Loads Bronze, Silver, and Gold layers efficiently.
    """

    ti = context["ti"]

    bronze_file = ti.xcom_pull(key="bronze_file", task_ids="bronze_ingest")
    silver_file = ti.xcom_pull(key="silver_file", task_ids="silver_transform")
    gold_file = ti.xcom_pull(key="gold_file", task_ids="gold_layer")

    exec_date = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
    ingestion_branch = f"airflow-{context['dag_run'].run_id}"

    bronze_rows = silver_rows = gold_rows = 0

    loader = SnowflakeLoader()
    loader.setup_tables()

    # raw JSON as VARIANT
    if bronze_file:
        bronze_size_kb = Path(bronze_file).stat().st_size / 1024 if Path(bronze_file).exists() else 0
        bronze_rows = loader.load_bronze(bronze_file, ingestion_branch)
        logger.info(f"Bronze layer loaded: {bronze_rows} rows (File size: {bronze_size_kb:.2f} KB)")

    # batch upload with write_pandas
    if silver_file:
        silver_size_kb = Path(silver_file).stat().st_size / 1024 if Path(silver_file).exists() else 0
        silver_df = pd.read_csv(silver_file)
        silver_rows = loader.load_silver_batch(silver_df, ingestion_branch)
        logger.info(f"Silver layer loaded: {silver_rows} rows (File size: {silver_size_kb:.2f} KB)")

    # staging + MERGE pattern
    if gold_file:
        gold_size_kb = Path(gold_file).stat().st_size / 1024 if Path(gold_file).exists() else 0
        gold_df = pd.read_csv(gold_file)
        gold_df = gold_df.rename(columns={"on_ground": "ON_GROUND_SUM"})
        gold_rows = loader.load_gold_batch(gold_df, exec_date)
        logger.info(f"Gold layer loaded: {gold_rows} rows merged (File size: {gold_size_kb:.2f} KB)")

    return {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "gold_rows": gold_rows,
        "timestamp": exec_date,
    }
