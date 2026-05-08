import json
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category"
]

def get_snowflake_conn():
    conn = BaseHook.get_connection('flight_snowflake')
    return snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson['account'],
        warehouse=conn.extra_dejson.get('warehouse'),
        database="FLIGHTS",
        schema="FLIGHT_SCHEMA",
        role=conn.extra_dejson.get('role')
    )

def _upload_df_to_snowflake(conn_sf, df, table_name, merge_keys):
    if df.empty:
        logger.info(f"DataFrame for {table_name} is empty, skipping upload.")
        return

    # Capitalize column names for Snowflake
    df.columns = [str(c).upper() for c in df.columns]
    
    stg_table = f"{table_name}_STG"
    
    col_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        if col == 'WINDOW_START':
            col_defs.append(f"{col} TIMESTAMP_NTZ")
        elif pd.api.types.is_float_dtype(dtype):
            col_defs.append(f"{col} FLOAT")
        elif pd.api.types.is_integer_dtype(dtype):
            col_defs.append(f"{col} NUMBER")
        elif pd.api.types.is_bool_dtype(dtype):
            col_defs.append(f"{col} BOOLEAN")
        else:
            col_defs.append(f"{col} VARCHAR")
            
    col_def_str = ",\n        ".join(col_defs)
    
    # Target Table DDL with Time Travel (7 days) and Clustering
    ddl = f"""
    CREATE TABLE IF NOT EXISTS FLIGHTS.FLIGHT_SCHEMA.{table_name} (
        {col_def_str},
        LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    DATA_RETENTION_TIME_IN_DAYS = 7
    CLUSTER BY (WINDOW_START);
    """
    
    # Staging Table DDL (Transient, 0 days retention to save cost)
    ddl_stg = f"""
    CREATE TRANSIENT TABLE IF NOT EXISTS FLIGHTS.FLIGHT_SCHEMA.{stg_table} (
        {col_def_str}
    )
    DATA_RETENTION_TIME_IN_DAYS = 0;
    """
    
    with conn_sf.cursor() as cursor:
        cursor.execute(ddl)
        cursor.execute(ddl_stg)
        cursor.execute(f"TRUNCATE TABLE FLIGHTS.FLIGHT_SCHEMA.{stg_table}")
        
    # Write to staging table using write_pandas for batch performance
    logger.info(f"Uploading {len(df)} rows to staging table {stg_table}...")
    success, nchunks, nrows, _ = write_pandas(
        conn_sf, df, stg_table, database="FLIGHTS", schema="FLIGHT_SCHEMA", auto_create_table=False
    )
    
    if not success:
        raise Exception(f"Failed to write to staging table {stg_table}")
        
    # Build MERGE statement
    match_conditions = " AND ".join([f"tgt.{k.upper()} = src.{k.upper()}" for k in merge_keys])
    
    update_cols = [c for c in df.columns if c not in [k.upper() for k in merge_keys]]
    if update_cols:
        update_sets = ",\n        ".join([f"tgt.{c} = src.{c}" for c in update_cols])
        update_clause = f"WHEN MATCHED THEN UPDATE SET\n        {update_sets},\n        tgt.LOAD_TIME = CURRENT_TIMESTAMP()"
    else:
        update_clause = "WHEN MATCHED THEN UPDATE SET tgt.LOAD_TIME = CURRENT_TIMESTAMP()"
        
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join([f"src.{c}" for c in df.columns])
    
    merge_sql = f"""
    MERGE INTO FLIGHTS.FLIGHT_SCHEMA.{table_name} tgt
    USING FLIGHTS.FLIGHT_SCHEMA.{stg_table} src
    ON {match_conditions}
    {update_clause}
    WHEN NOT MATCHED THEN INSERT
    ({insert_cols}, LOAD_TIME)
    VALUES
    ({insert_vals}, CURRENT_TIMESTAMP());
    """
    
    logger.info(f"Executing MERGE into {table_name}...")
    with conn_sf.cursor() as cursor:
        cursor.execute(merge_sql)
    logger.info(f"Successfully merged {nrows} rows into {table_name}.")


def snowflake_load_bronze(**context):
    bronze_file = context['ti'].xcom_pull(key='bronze_file', task_ids='bronze_ingest')
    if not bronze_file:
        logger.info("No bronze_file in XCom, skipping Snowflake Bronze load.")
        return
        
    exec_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")
    
    with open(bronze_file) as f:
        raw = json.load(f)
        
    if not raw or not raw.get("states"):
        logger.info("Bronze file is empty or has no states.")
        return
        
    n_cols = len(raw['states'][0])
    cols = OPENSKY_COLUMNS[:n_cols]
    
    df = pd.DataFrame(raw["states"], columns=cols)
    
    # Convert list/dict columns to string so write_pandas doesn't fail
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].astype(str)
            
    df['WINDOW_START'] = pd.to_datetime(exec_date)
    
    conn_sf = get_snowflake_conn()
    try:
        _upload_df_to_snowflake(conn_sf, df, "FLIGHT_BRONZE", ["WINDOW_START", "ICAO24"])
    finally:
        conn_sf.close()


def snowflake_load_silver(**context):
    silver_file = context['ti'].xcom_pull(key='silver_file', task_ids='silver_transform')
    if not silver_file:
        logger.info("No silver_file in XCom, skipping Snowflake Silver load.")
        return
        
    exec_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")
    df = pd.read_csv(silver_file)
    df['WINDOW_START'] = pd.to_datetime(exec_date)
    
    conn_sf = get_snowflake_conn()
    try:
        _upload_df_to_snowflake(conn_sf, df, "FLIGHT_SILVER", ["WINDOW_START", "ICAO24"])
    finally:
        conn_sf.close()


def snowflake_load_gold(**context):
    gold_file = context['ti'].xcom_pull(key='gold_file', task_ids='gold_layer')
    if not gold_file:
        logger.info("No gold_file in XCom, skipping Snowflake Gold load.")
        return
        
    exec_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")
    df = pd.read_csv(gold_file)
    df['WINDOW_START'] = pd.to_datetime(exec_date)
    
    conn_sf = get_snowflake_conn()
    try:
        _upload_df_to_snowflake(conn_sf, df, "FLIGHT_GOLD", ["WINDOW_START", "ORIGIN_COUNTRY"])
    finally:
        conn_sf.close()