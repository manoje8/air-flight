import json
import logging
from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

# OpenSky Network API column names
OPENSKY_COLUMNS = [
    "icao24",           # 0
    "callsign",         # 1
    "origin_country",   # 2
    "time_position",    # 3  int, nullable
    "last_contact",     # 4  int
    "longitude",        # 5  float, nullable
    "latitude",         # 6  float, nullable
    "baro_altitude",    # 7  float, nullable
    "on_ground",        # 8  bool
    "velocity",         # 9  float, nullable
    "true_track",       # 10 float, nullable
    "vertical_rate",    # 11 float, nullable
    "sensors",          # 12 int[], nullable
    "geo_altitude",     # 13 float, nullable
    "squawk",           # 14 string, nullable
    "spi",              # 15 bool
    "position_source",  # 16 int
    "category",         # 17 int, nullable — only present if extended=1
]

# Subset of Silver layer columns
SILVER_COLUMNS = [
    "icao24", "origin_country", "latitude", "longitude",
    "time_position", "last_contact", "velocity",
    "vertical_rate", "true_track", "baro_altitude", "on_ground",
]

FLOAT_COLS = [
    "latitude", "longitude", "velocity",
    "vertical_rate", "true_track", "baro_altitude", "geo_altitude",
]
INT_NULLABLE_COLS = ["time_position", "last_contact"]


def run_silver_transform(bronze_file: str, **context) -> str:
    """
    Transform Bronze raw JSON into a clean, schema-normalised Silver CSV.

    - Raises AirflowSkipException when the Bronze file contains no states,
      so downstream quality checks and Gold layer are skipped gracefully.
    - Raises ValueError when the Bronze file path is missing from XCom.
    """
    exec_date = context["ds_nodash"]

    logger.info("Reading Bronze file: %s", bronze_file)

    with open(bronze_file) as f:
        raw = json.load(f)

    if not raw or raw.get("states") is None:
        raise AirflowSkipException(
            "Bronze file contains no flight states — skipping Silver transform."
        )

    n_cols = len(raw['states'][0])
    cols = OPENSKY_COLUMNS[:n_cols]


    df_raw = pd.DataFrame(raw["states"], columns=cols)

    available_silver = [c for c in SILVER_COLUMNS if c in df_raw.columns]
    df = df_raw[available_silver].copy()

    df[FLOAT_COLS] = df_raw[FLOAT_COLS].apply(pd.to_numeric, errors="coerce")
    df[INT_NULLABLE_COLS] = df_raw[INT_NULLABLE_COLS].apply(pd.to_numeric, errors="coerce").astype("Int64")
    df['on_ground'] = df['on_ground'].astype(bool)
    df['icao24'] = df['icao24'].astype(str).str.strip()
    df["origin_country"] = df["origin_country"].astype(str).str.strip()


    logger.info("Silver DataFrame shape: %s", df.shape)

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    output_file = silver_path / f"flight_silver_{exec_date}.csv"
    df.to_csv(output_file, index=False)

    logger.info("Silver file written: %s", output_file)
    
    return str(output_file)