"""
Shared pytest fixtures for the flight pipeline test suite.

Provides:
  - mock_opensky_response : realistic OpenSky API payload (dict)
  - sample_states         : list of raw state vectors
  - mock_context          : fake Airflow task context with a mock TaskInstance
  - bronze_json_file      : a real Bronze JSON file written to tmp_path
  - silver_csv_file       : a real Silver CSV file written to tmp_path
"""

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ── Global Airflow Stubs ──────────────────────────────────────────────────────
# This ensures all tests (unit and integration) use the same Exception classes
# and avoid "Class A is not Class B" errors when catching exceptions.


class AirflowSkipException(Exception):
    """Stub for airflow.exceptions.AirflowSkipException"""

    pass


class AirflowFailException(Exception):
    """Stub for airflow.exceptions.AirflowFailException"""

    pass


def setup_airflow_stubs():
    if "airflow" not in sys.modules:
        airflow_pkg = types.ModuleType("airflow")
        airflow_exceptions = types.ModuleType("airflow.exceptions")
        airflow_exceptions.AirflowSkipException = AirflowSkipException
        airflow_exceptions.AirflowFailException = AirflowFailException
        airflow_pkg.exceptions = airflow_exceptions
        sys.modules["airflow"] = airflow_pkg
        sys.modules["airflow.exceptions"] = airflow_exceptions


setup_airflow_stubs()


# ── Raw state vectors (17 fields per OpenSky spec) ────────────────────────────
SAMPLE_STATES = [
    [
        "abc123",
        "UAL123  ",
        "United States",
        1700000000.0,
        1700000001.0,
        -87.6298,
        41.8827,
        10000.0,
        False,
        250.0,
        90.0,
        0.5,
        None,
        9800.0,
        "1234",
        False,
        0,
    ],
    [
        "def456",
        "BAW456  ",
        "United Kingdom",
        1700000000.0,
        1700000001.0,
        0.1276,
        51.5074,
        11000.0,
        False,
        280.0,
        180.0,
        -0.2,
        None,
        10800.0,
        "5678",
        False,
        0,
    ],
    [
        "ghi789",
        "DLH789  ",
        "Germany",
        1700000000.0,
        1700000001.0,
        13.4050,
        52.5200,
        9000.0,
        False,
        300.0,
        270.0,
        1.0,
        None,
        8800.0,
        "9012",
        False,
        0,
    ],
    [
        "jkl012",
        "AFR012  ",
        "France",
        1700000000.0,
        1700000001.0,
        2.3522,
        48.8566,
        12000.0,
        False,
        260.0,
        0.0,
        -1.0,
        None,
        11800.0,
        "3456",
        False,
        0,
    ],
    [
        "mno345",
        "SIA345  ",
        "Singapore",
        1700000000.0,
        1700000001.0,
        103.8198,
        1.3521,
        11500.0,
        False,
        310.0,
        45.0,
        0.0,
        None,
        11300.0,
        "7890",
        False,
        0,
    ],
]


@pytest.fixture
def sample_states():
    """Return a list of raw OpenSky state vectors."""
    return SAMPLE_STATES


@pytest.fixture
def mock_opensky_response():
    """Return a realistic OpenSky API response dict."""
    return {
        "time": 1700000000,
        "states": SAMPLE_STATES,
    }


@pytest.fixture
def mock_opensky_empty_response():
    """OpenSky response when no flights are tracked (states=null)."""
    return {"time": 1700000000, "states": None}


def _make_mock_context(tmp_path: Path, xcom_store: dict | None = None):
    """
    Build a minimal Airflow task context with a MagicMock TaskInstance.

    XCom state is backed by a plain dict so tests can inspect push/pull calls.
    """
    store = xcom_store if xcom_store is not None else {}

    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: store.update({key: value})
    ti.xcom_pull.side_effect = lambda key, task_ids=None: store.get(key)

    return {
        "ti": ti,
        "ds_nodash": "20251210",
        "logical_date": datetime(2025, 12, 10, 14, 0, 0, tzinfo=timezone.utc),
    }, store


@pytest.fixture
def xcom_store():
    """Shared XCom backing dict — lets tests inspect pushed values."""
    return {}


@pytest.fixture
def mock_context(tmp_path, xcom_store):
    """Airflow context dict with a mock TaskInstance (xcom backed by xcom_store)."""
    ctx, _ = _make_mock_context(tmp_path, xcom_store)
    return ctx


@pytest.fixture
def bronze_json_file(tmp_path, mock_opensky_response):
    """Write a Bronze JSON file to tmp_path and return its Path."""
    path = tmp_path / "bronze" / "flight_2025-12-10-14-00-00.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(mock_opensky_response))
    return path


@pytest.fixture
def silver_csv_file(tmp_path):
    """Write a minimal Silver CSV file to tmp_path and return its Path."""
    data = {
        "icao24": ["abc123", "def456", "ghi789"],
        "origin_country": ["United States", "United Kingdom", "Germany"],
        "latitude": [41.8827, 51.5074, 52.5200],
        "longitude": [-87.6298, 0.1276, 13.4050],
        "time_position": [1700000000.0, 1700000000.0, 1700000000.0],
        "last_contact": [1700000001.0, 1700000001.0, 1700000001.0],
        "velocity": [250.0, 280.0, 300.0],
        "vertical_rate": [0.5, -0.2, 1.0],
        "true_track": [90.0, 180.0, 270.0],
        "baro_altitude": [10000.0, 11000.0, 9000.0],
        "on_ground": [False, False, False],
    }
    path = tmp_path / "silver" / "flight_silver_20251210.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(data).to_csv(path, index=False)
    return path


@pytest.fixture
def path_redir(tmp_path):
    """Fixture that returns a function to redirect /opt/airflow paths to tmp_path."""

    def _redir(p):
        p_obj = Path(p)
        try:
            # Remove leading slash and 'opt/airflow/' if present
            parts = p_obj.parts
            if parts[0] == "/":
                parts = parts[1:]
            if parts[0] == "opt" and parts[1] == "airflow":
                parts = parts[2:]

            return tmp_path / Path(*parts)
        except (ValueError, IndexError):
            return tmp_path / p_obj.name

    return _redir
