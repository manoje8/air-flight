import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    home_path = os.getenv("HOME_PATH", "/opt/airflow")
    snowflake_conn_id: str = os.getenv("SNOWFLAKE_CONN_ID", "flight_snowflake")
    snowflake_db: str = os.getenv("SNOWFLAKE_DATABASE", "FLIGHTS")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "FLIGHT_SCHEMA")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    snowflake_role: str = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")



config = Config()