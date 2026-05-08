import logging
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook
from snowflake.connector.pandas_tools import write_pandas

from config import config
from utils.constants import SILVER_COLUMNS

logger = logging.getLogger(__name__)

class SnowflakeLoader:
    """ Handles batch loading of flight data into snowflake using stage tables """

    def __init__(self, conn_id: str = 'flight_snowflake'):


        self.conn_id = conn_id
        self.database = config.snowflake_db
        self.schema = config.snowflake_schema
        self.warehouse = None
        self.role = None
        self._init_connection()

    def _init_connection(self):
        conn = BaseHook.get_connection(self.conn_id)

        self.conn_params = {
            'user': conn.login,
            'password': conn.password,
            'account': conn.extra_dejson['account'],
            'warehouse': conn.extra_dejson.get('warehouse', config.snowflake_warehouse),
            'database': self.database,
            'schema': self.schema,
            'role': conn.extra_dejson.get('role', config.snowflake_role)
        }

        self.warehouse = conn.extra_dejson.get('warehouse', config.snowflake_warehouse)
        self.role = conn.extra_dejson.get('role', config.snowflake_role)


    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(**self.conn_params)


    def setup_tables(self):
        conn = self.get_connection()

        try:

            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS FLIGHTS.FLIGHT_SCHEMA.BRONZE_FLIGHTS (
                    INGESTION_TIME TIMESTAMP_NTZ,
                    RAW_DATA VARIANT,
                    SOURCE_FILE VARCHAR,
                    INGESTION_BATCH VARCHAR
                )
                
                CLUSTER BY (INGESTION_TIME)
                DATA_RETENTION_TIME_IN_DAYS = 90
                CHANGE_TRACKING = TRUE
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS FLIGHTS.FLIGHT_SCHEMA.SILVER_FLIGHTS (
                    INGESTION_TIME TIMESTAMP_NTZ,
                    ICAO24 VARCHAR(6) NOT NULL,
                    ORIGIN_COUNTRY VARCHAR(50),
                    LATITUDE FLOAT,
                    LONGITUDE FLOAT,
                    TIME_POSITION INTEGER,
                    LAST_CONTACT INTEGER,
                    VELOCITY FLOAT,
                    VERTICAL_RATE FLOAT,
                    TRUE_TRACK FLOAT,
                    BARO_ALTITUDE FLOAT,
                    GEO_ALTITUDE FLOAT,
                    ON_GROUND BOOLEAN
                )
                
                CLUSTER BY (INGESTION_TIME, ORIGIN_COUNTRY)
                DATA_RETENTION_TIME_IN_DAYS = 90
                CHANGE_TRACKING = TRUE
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS FLIGHTS.FLIGHT_SCHEMA.GOLD_FLIGHT_AGG (
                    WINDOW_START TIMESTAMP_NTZ NOT NULL,
                    ORIGIN_COUNTRY VARCHAR(50),
                    TOTAL_FLIGHTS INTEGER,
                    AVG_VELOCITY FLOAT,
                    ON_GROUND_SUM INTEGER,
                    LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    
                    CONSTRAINT pk_window_country PRIMARY KEY (WINDOW_START, ORIGIN_COUNTRY)
                )
                
                CLUSTER BY (DATE_TRUNC('day', WINDOW_START))
                DATA_RETENTION_TIME_IN_DAYS = 90
                CHANGE_TRACKING = TRUE
            """)


            # Create staging table
            cursor.execute("""
                CREATE TRANSIENT TABLE IF NOT EXISTS FLIGHTS.FLIGHT_SCHEMA.STG_GOLD_FLIGHT_AGG
                LIKE FLIGHTS.FLIGHT_SCHEMA.GOLD_FLIGHT_AGG
            """)


            # Create time travel clones (rolling 7 days backup)
            try:
                cursor.execute("""
                    CREATE OR REPLACE TABLE FLIGHTS.FLIGHT_SCHEMA.GOLD_FLIGHT_AGG_BACKUP
                    CLONE FLIGHTS.FLIGHT_SCHEMA.GOLD_FLIGHT_AGG
                    AT (OFFSET => -60*60*24*7)
                """)
            except Exception as clone_err:
                logger.warning(f"Skipping 7-day clone for GOLD_FLIGHT_AGG (table might be too new): {clone_err}")

            conn.commit()
            logger.info("Successfully created/verified all Snowflake tables")


        except Exception as e:
            logger.error(f'Failed to setup tables: {e}')
            conn.rollback()
            raise

        finally:
            conn.close()



    def load_bronze(self, bronze_file: str, ingestion_batch: str):
        conn = self.get_connection()

        try:
            cursor = conn.cursor()

            with open(bronze_file) as f:
                raw_data = f.read()

            ingestion_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute(
                """
                INSERT INTO FLIGHTS.FLIGHT_SCHEMA.BRONZE_FLIGHTS 
                (INGESTION_TIME, RAW_DATA, SOURCE_FILE, INGESTION_BATCH)
                SELECT %s, PARSE_JSON(%s), %s, %s
                """, (ingestion_time, raw_data, bronze_file, ingestion_batch)
            )

            row_count = cursor.rowcount
            conn.commit()
            logger.info(f"Loaded {row_count} rows into BRONZE FLIGHTS")
            return row_count

        except Exception as e:
            logger.error(f"Failed to load bronze data: {e}")
            conn.rollback()
            raise

        finally:
            conn.close()


    def load_silver_batch(self, silver_df: pd.DataFrame, ingestion_batch: str):

        conn = self.get_connection()

        try:
            silver_df['INGESTION_TIME'] = datetime.now(timezone.utc)
            silver_df['INGESTION_BATCH'] = ingestion_batch

            silver_cols = ['INGESTION_TIME'] + SILVER_COLUMNS

            df_to_load = silver_df[[c for c in silver_cols if c in silver_df.columns]]

            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_to_load,
                table_name="SILVER_FLIGHTS",
                database=self.database,
                schema=self.schema,
                auto_create_table=False,
                quote_identifiers=False,
            )

            if not success:
                raise Exception("Failed to load Silver data using write_pandas")

            logger.info(f"Loaded {nrows} rows into SILVER_FLIGHTS in {nchunks} chunks")
            return nrows

        except Exception as e:
            logger.error(f"Failed to load silver data: {e}")
            conn.rollback()
            raise

        finally:
            conn.close()


    def load_gold_batch(self, gold_df: pd.DataFrame, exec_date: str):
        """ Load Gold aggregations using staging table and MERGE for upsert. """
        conn = self.get_connection()

        try:
            cursor = conn.cursor()

            gold_df['WINDOW_START'] = exec_date
            gold_df['LOAD_TIME'] = datetime.now(timezone.utc)

            cursor.execute("TRUNCATE TABLE FLIGHTS.FLIGHT_SCHEMA.STG_GOLD_FLIGHT_AGG")
            conn.commit()

            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=gold_df,
                table_name="STG_GOLD_FLIGHT_AGG",
                database=self.database,
                schema=self.schema,
                auto_create_table=False,
                quote_identifiers=False,
            )

            if not success:
                raise Exception("Failed to load into staging table")

            merge_sql = """
                MERGE INTO FLIGHTS.FLIGHT_SCHEMA.GOLD_FLIGHT_AGG tgt
                USING FLIGHTS.FLIGHT_SCHEMA.STG_GOLD_FLIGHT_AGG src
                ON tgt.WINDOW_START = src.WINDOW_START
                    AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY
                    
                WHEN MATCHED THEN UPDATE SET
                    tgt.TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
                    tgt.AVG_VELOCITY = src.AVG_VELOCITY,
                    tgt.ON_GROUND_SUM = src.ON_GROUND_SUM,
                    tgt.LOAD_TIME = CURRENT_TIMESTAMP()
                    
                WHEN NOT MATCHED THEN INSERT
                    (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND_SUM, LOAD_TIME)
                
                VALUES
                    (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.ON_GROUND_SUM, CURRENT_TIMESTAMP())
            """

            cursor.execute(merge_sql)
            merged_count = cursor.rowcount

            conn.commit()
            logger.info(f"Merged {merged_count} rows into GOLD_FLIGHT_AGG")
            return merged_count

        except Exception as e:
            logger.error(f"Failed to load gold data: {e}")
            conn.rollback()
            raise

        finally:
            conn.close()