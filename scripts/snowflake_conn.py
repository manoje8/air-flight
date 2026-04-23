import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook

def snowflake_load(**context):
    gold_file = context['ti'].xcom_pull(key='gold_file', task_ids='gold_layer')

    if not gold_file:
        raise ValueError("Gold file path is not found in xcom")

    exec_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")

    df = pd.read_csv(gold_file)

    conn = BaseHook.get_connection('flight_snowflake')

    conn_sf = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson['account'],
        warehouse=conn.extra_dejson.get('warehouse'),
        database="FLIGHTS",
        schema="FLIGHT_SCHEMA",
        role=conn.extra_dejson.get('role')
    )

    base_sql = """
    MERGE INTO FLIGHTS.FLIGHT_SCHEMA.FLIGHT tgt
    USING (
        SELECT
            TO_TIMESTAMP(%s) AS WINDOW_START,
            %s AS ORIGIN_COUNTRY,
            %s AS TOTAL_FLIGHTS,
            %s AS AVG_VELOCITY,
            %s AS ON_GROUND
    ) src
    ON tgt.WINDOW_START = src.WINDOW_START
       AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY

    WHEN MATCHED THEN UPDATE SET
        TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
        AVG_VELOCITY = src.AVG_VELOCITY,
        ON_GROUND = src.ON_GROUND,
        LOAD_TIME = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND)
    VALUES
    (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.ON_GROUND); 
    """

    with conn_sf.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(
                base_sql, (
                    exec_date,
                    row['origin_country'],
                    row['total_flights'],
                    row['avg_velocity'],
                    row['on_ground']
                )
            )

    conn_sf.close()