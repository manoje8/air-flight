"""
Snowflake Time Travel and snapshot management.
Creates point-in-time clones for safe rollback capabilities.
"""

import logging
from datetime import datetime, timezone

import snowflake.connector
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def create_snapshot(**context):
    """
    Create a TimeTravel snapshot before loading new data.
    """
    from config import config

    conn_details = BaseHook.get_connection('flight_snowflake')

    conn = snowflake.connector.connect(
        user=conn_details.login,
        password=conn_details.password,
        account=conn_details.extra_dejson['account'],
        warehouse=conn_details.extra_dejson.get('warehouse', config.snowflake_warehouse),
        database=config.snowflake_db,
        schema=config.snowflake_schema,
        role=conn_details.extra_dejson.get('role', config.snowflake_role),
    )

    try:
        cursor = conn.cursor()
        snapshot_ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        tables_to_snapshot = ['GOLD_FLIGHT_AGG', 'SILVER_FLIGHTS']

        for table in tables_to_snapshot:
            snapshot_name = f"{table}_SNAPSHOT_{snapshot_ts}"

            cursor.execute(f"""
                CREATE OR REPLACE TABLE FLIGHT_SCHEMA.{snapshot_name}
                CLONE FLIGHT_SCHEMA.{table}
                AT (OFFSET => -60)
            """)

            logger.info(f"Created snapshot: {snapshot_name}")

        cursor.execute("""
            ALTER TABLE FLIGHT_SCHEMA.GOLD_FLIGHT_AGG 
            SET DATA_RETENTION_TIME_IN_DAYS = 90
        """)

        conn.commit()

        context['ti'].xcom_push(key='snapshot_timestamp', value=snapshot_ts)

    except Exception as e:
        logger.error(f"Snapshot creation failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def rollback_to_snapshot(snapshot_ts: str, **context):
    """
    Rollback to a specific snapshot.
    Restores tables from their snapshot versions.
    """
    conn_details = BaseHook.get_connection('flight_snowflake')

    conn = snowflake.connector.connect(
        user=conn_details.login,
        password=conn_details.password,
        account=conn_details.extra_dejson['account'],
        warehouse=conn_details.extra_dejson.get('warehouse', 'COMPUTE_WH'),
        database='FLIGHTS',
        schema='FLIGHT_SCHEMA',
    )

    try:
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE OR REPLACE TABLE FLIGHT_SCHEMA.GOLD_FLIGHT_AGG 
            CLONE FLIGHT_SCHEMA.GOLD_FLIGHT_AGG_SNAPSHOT_{snapshot_ts}
        """)

        cursor.execute(f"""
            CREATE OR REPLACE TABLE FLIGHT_SCHEMA.SILVER_FLIGHTS
            CLONE FLIGHT_SCHEMA.SILVER_FLIGHTS_SNAPSHOT_{snapshot_ts}
        """)

        conn.commit()
        logger.info(f"Successfully rolled back to snapshot {snapshot_ts}")

    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()