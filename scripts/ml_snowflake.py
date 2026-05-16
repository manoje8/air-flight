"""
Load ML predictions into Snowflake GOLD_ML_PREDICTIONS table.

Follows the same pattern as utils/snowflake_loader.py:
  - setup_ml_table() creates the table if it doesn't exist
  - load_ml_batch()  writes a predictions DataFrame using write_pandas()

Entry-point (Airflow task)
--------------------------
    from scripts.ml_snowflake import run_ml_snowflake_load
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.hooks.base import BaseHook

from config import config

logger = logging.getLogger(__name__)

ML_TABLE = "GOLD_ML_PREDICTIONS"
STG_TABLE = "STG_GOLD_ML_PREDICTIONS"

# Columns written to Snowflake (uppercase to match Snowflake convention)
ML_COLUMNS = [
    "ICAO24",
    "ORIGIN_COUNTRY",
    "VELOCITY",
    "BARO_ALTITUDE",
    "PREDICTED_ON_GROUND",
    "ONGROUND_PROBABILITY",
    "ANOMALY_SCORE",
    "IS_ANOMALY",
    "WINDOW_START",
    "LOAD_TIME",
]

_RENAME_MAP = {
    "icao24": "ICAO24",
    "origin_country": "ORIGIN_COUNTRY",
    "velocity": "VELOCITY",
    "baro_altitude": "BARO_ALTITUDE",
    "predicted_on_ground": "PREDICTED_ON_GROUND",
    "onground_probability": "ONGROUND_PROBABILITY",
    "anomaly_score": "ANOMALY_SCORE",
    "is_anomaly": "IS_ANOMALY",
    "window_start": "WINDOW_START",
}


class MLSnowflakeLoader:
    """Handles batch loading of ML predictions into Snowflake."""

    def __init__(self, conn_id: str = "flight_snowflake"):
        self.conn_id = conn_id
        self.database = config.snowflake_db
        self.schema = config.snowflake_schema
        self._init_connection()

    def _init_connection(self):
        conn = BaseHook.get_connection(self.conn_id)
        self.conn_params = {
            "user": conn.login,
            "password": conn.password,
            "account": conn.extra_dejson["account"],
            "warehouse": conn.extra_dejson.get("warehouse", config.snowflake_warehouse),
            "database": self.database,
            "schema": self.schema,
            "role": conn.extra_dejson.get("role", config.snowflake_role),
        }

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(**self.conn_params)

    def setup_ml_table(self) -> None:
        """Create GOLD_ML_PREDICTIONS and its staging table if they don't exist."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{ML_TABLE} (
                    ICAO24                VARCHAR(6)     NOT NULL,
                    ORIGIN_COUNTRY        VARCHAR(50),
                    VELOCITY              FLOAT,
                    BARO_ALTITUDE         FLOAT,
                    PREDICTED_ON_GROUND   BOOLEAN        NOT NULL,
                    ONGROUND_PROBABILITY  FLOAT          NOT NULL,
                    ANOMALY_SCORE         FLOAT          NOT NULL,
                    IS_ANOMALY            BOOLEAN        NOT NULL,
                    WINDOW_START          TIMESTAMP_NTZ  NOT NULL,
                    LOAD_TIME             TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

                    CONSTRAINT pk_ml_predictions
                        PRIMARY KEY (ICAO24, WINDOW_START)
                )
                CLUSTER BY (DATE_TRUNC('day', WINDOW_START))
                DATA_RETENTION_TIME_IN_DAYS = 30
                CHANGE_TRACKING = TRUE
            """)

            cur.execute(f"""
                CREATE TRANSIENT TABLE IF NOT EXISTS {self.database}.{self.schema}.{STG_TABLE} (
                    ICAO24                VARCHAR(6),
                    ORIGIN_COUNTRY        VARCHAR(50),
                    VELOCITY              FLOAT,
                    BARO_ALTITUDE         FLOAT,
                    PREDICTED_ON_GROUND   BOOLEAN,
                    ONGROUND_PROBABILITY  FLOAT,
                    ANOMALY_SCORE         FLOAT,
                    IS_ANOMALY            BOOLEAN,
                    WINDOW_START          TIMESTAMP_NTZ,
                    LOAD_TIME             TIMESTAMP_NTZ
                )
            """)

            conn.commit()
            logger.info("ML Snowflake tables created/verified.")
        except Exception as exc:
            conn.rollback()
            logger.error("setup_ml_table failed: %s", exc)
            raise
        finally:
            conn.close()

    def load_ml_batch(self, df: pd.DataFrame) -> int:
        """
        Upsert ML predictions using staging table + MERGE pattern.

        Returns number of rows merged.
        """
        conn = self.get_connection()
        try:
            cur = conn.cursor()

            # Prepare DataFrame
            df = df.rename(columns=_RENAME_MAP)
            df["LOAD_TIME"] = datetime.now(timezone.utc).replace(tzinfo=None)
            ws = pd.to_datetime(df["WINDOW_START"], utc=True)
            df["WINDOW_START"] = ws.dt.tz_localize(None).dt.to_pydatetime()

            df_to_load = df[[c for c in ML_COLUMNS if c in df.columns]]

            # Stage
            cur.execute(f"TRUNCATE TABLE {self.database}.{self.schema}.{STG_TABLE}")
            conn.commit()

            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_to_load,
                table_name=STG_TABLE,
                database=self.database,
                schema=self.schema,
                auto_create_table=False,
                quote_identifiers=False,
            )
            if not success:
                raise RuntimeError("write_pandas into staging table failed.")

            # MERGE
            merge_sql = f"""
                MERGE INTO {self.database}.{self.schema}.{ML_TABLE} tgt
                USING {self.database}.{self.schema}.{STG_TABLE} src
                ON tgt.ICAO24 = src.ICAO24
                   AND tgt.WINDOW_START = src.WINDOW_START

                WHEN MATCHED THEN UPDATE SET
                    tgt.PREDICTED_ON_GROUND  = src.PREDICTED_ON_GROUND,
                    tgt.ONGROUND_PROBABILITY = src.ONGROUND_PROBABILITY,
                    tgt.ANOMALY_SCORE        = src.ANOMALY_SCORE,
                    tgt.IS_ANOMALY           = src.IS_ANOMALY,
                    tgt.VELOCITY             = src.VELOCITY,
                    tgt.BARO_ALTITUDE        = src.BARO_ALTITUDE,
                    tgt.LOAD_TIME            = CURRENT_TIMESTAMP()

                WHEN NOT MATCHED THEN INSERT
                    ({', '.join(ML_COLUMNS)})
                VALUES
                    ({', '.join(f'src.{c}' for c in ML_COLUMNS)})
            """
            cur.execute(merge_sql)
            merged = cur.rowcount
            conn.commit()
            logger.info("Merged %d rows into %s", merged, ML_TABLE)
            return merged

        except Exception as exc:
            conn.rollback()
            logger.error("load_ml_batch failed: %s", exc)
            raise
        finally:
            conn.close()


def run_ml_snowflake_load(ml_predictions_file: str, **context) -> dict:
    """
    Airflow task wrapper: reads predictions CSV and loads to Snowflake.
    """
    ti = context.get("ti")

    if not ml_predictions_file and ti:
        ml_predictions_file = ti.xcom_pull(key="ml_predictions_file")

    if not ml_predictions_file:
        raise ValueError("run_ml_snowflake_load: ml_predictions_file not found.")

    pred_path = Path(ml_predictions_file)
    if not pred_path.exists():
        raise FileNotFoundError(f"Predictions file not found: {pred_path}")

    df = pd.read_csv(pred_path)
    logger.info("Loaded %d prediction rows from %s", len(df), pred_path)

    loader = MLSnowflakeLoader()
    loader.setup_ml_table()
    rows_merged = loader.load_ml_batch(df)

    result = {
        "ml_rows_merged": rows_merged,
        "source_file": str(pred_path),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("ML Snowflake load complete: %s", result)
    return result
