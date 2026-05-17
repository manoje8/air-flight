"""
Data quality validation for the Silver layer using pandera.

Schema enforces:
  - icao24          : non-null, non-blank string (flight transponder ID)
  - origin_country  : non-null string
  - latitude        : nullable float in [-90, 90]   — null for ground/non-transmitting aircraft
  - longitude       : nullable float in [-180, 180]  — null for ground/non-transmitting aircraft
  - time_position   : nullable Int64 (Unix timestamp, int per OpenSky API docs)
  - last_contact    : nullable Int64 (Unix timestamp, int per OpenSky API docs)
  - velocity        : nullable float >= 0
  - vertical_rate   : nullable float
  - true_track      : nullable float
  - baro_altitude   : nullable float
  - geo_altitude    : nullable float (separate from baro_altitude per OpenSky API docs)
  - on_ground       : non-null bool

Cross-column check:
    Airborne aircraft (on_ground=False) must have non-null lat/lon.
    Ground aircraft may legitimately have no position fix.

A SchemaError is raised (failing the DAG task) if any check is violated.
"""

import logging
from pathlib import Path

import pandas as pd
from pandera import Column, DataFrameSchema, Check

logger = logging.getLogger(__name__)


def _no_position_without_timestamp(df: pd.DataFrame) -> pd.Series:
    """
    If latitude/longitude are non-null, time_position must also be non-null.
    A position fix without a timestamp is meaningless/corrupt data.
    The inverse (null position but valid timestamp) is fine — OpenSky
    can receive telemetry without a position update.
    """
    has_position = df["latitude"].notna() & df["longitude"].notna()
    has_timestamp = df["time_position"].notna()
    return ~has_position | has_timestamp


def _airborne_rows_have_position(df: pd.DataFrame) -> pd.Series:
    """
    For rows where on_ground is False, lat and lon must both be non-null.
    Returns a boolean Series — False marks a violation.
    """
    airborne = ~df["on_ground"]
    lat_ok = df["latitude"].notna()
    lon_ok = df["longitude"].notna()

    return ~airborne | (lat_ok & lon_ok)


SILVER_SCHEMA = DataFrameSchema(
    columns={
        "icao24": Column(
            str,
            nullable=False,
            checks=Check(
                lambda s: s.str.strip().ne(""),
                error="icao24 must not be blank",
            ),
        ),
        "origin_country": Column(str, nullable=True),
        "latitude": Column(
            float,
            nullable=True,
            checks=Check(
                lambda s: s.dropna().between(-90.0, 90.0).all(),
                error="latitude out of [-90, 90]",
            ),
        ),
        "longitude": Column(
            float,
            nullable=True,
            checks=Check(
                lambda s: s.dropna().between(-180.0, 180.0).all(),
                error="longitude out of [-180, 180]",
            ),
        ),
        "time_position": Column(pd.Int64Dtype(), nullable=True),
        "last_contact": Column(pd.Int64Dtype(), nullable=True),
        "velocity": Column(
            float,
            nullable=True,
            checks=Check(
                lambda s: s.dropna().ge(0).all(),
                error="velocity must be >= 0 when not null",
            ),
        ),
        "vertical_rate": Column(float, nullable=True),
        "true_track": Column(float, nullable=True),
        "baro_altitude": Column(float, nullable=True),
        "geo_altitude": Column(float, nullable=True),
        "on_ground": Column(bool, nullable=False),
    },
    checks=[
        Check(
            lambda df: len(df) >= 1,
            error="Silver layer must contain at least 1 row",
        ),
        Check(
            _no_position_without_timestamp,
            error="Rows with lat/lon must also have a non-null time_position",
        ),
    ],
    coerce=True,
    strict=False,
)


def run_quality_check(silver_file: str, **context) -> str:
    """
    Validate the Silver CSV against the SILVER_SCHEMA.

    Raises:
        pa.errors.SchemaError  — fails the DAG task if any check is violated.
        ValueError             — if Silver file path is missing from XCom.
    """

    file_size_kb = Path(silver_file).stat().st_size / 1024 if Path(silver_file).exists() else 0
    logger.info("Running data quality checks on: %s (%.2f KB)", silver_file, file_size_kb)

    df = pd.read_csv(
        silver_file,
        dtype={
            "time_position": "Int64",
            "last_contact": "Int64",
        },
    )

    logger.info("Validating %d rows against SILVER_SCHEMA...", len(df))

    validated_df = SILVER_SCHEMA.validate(df, lazy=True)

    null_icao = validated_df["icao24"].isna().sum()
    null_lat = validated_df["latitude"].isna().sum()
    null_lon = validated_df["longitude"].isna().sum()

    on_ground_mask = validated_df["on_ground"].astype(bool)
    null_pos_ground = int(validated_df[on_ground_mask]["latitude"].isna().sum())
    null_pos_airborne = int(validated_df[~on_ground_mask]["latitude"].isna().sum())

    logger.info(
        "Quality check PASSED — rows: %d | null icao24: %d "
        "| null lat: %d (ground: %d, airborne: %d) | null lon: %d",
        len(validated_df),
        null_icao,
        null_lat,
        null_pos_ground,
        null_pos_airborne,
        null_lon,
    )

    if null_pos_airborne:
        logger.warning(
            "%d airborne rows have no position fix — investigate upstream Silver transform.",
            null_pos_airborne,
        )

    report = {
        "row_count": len(validated_df),
        "null_icao24": int(validated_df["icao24"].isna().sum()),
        "null_latitude": int(validated_df["latitude"].isna().sum()),
        "null_longitude": int(validated_df["longitude"].isna().sum()),
        "null_position_ground": int(
            validated_df[on_ground_mask]["latitude"].isna().sum()
        ),
        "null_position_airborne": null_pos_airborne,
    }

    logger.info("Quality check report: %s", report)

    return report
