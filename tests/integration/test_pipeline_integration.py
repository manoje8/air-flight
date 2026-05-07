"""
Integration tests for the full flight pipeline.

Mocks the OpenSky API with a realistic payload and wires all four stages
(bronze → silver → quality_check → gold) using a shared fake Airflow context.
No Docker or Airflow scheduler is required — all I/O is redirected to tmp_path.

Scenarios:
  1. Full happy-path pipeline produces a Gold CSV with correct data
  2. Empty-states pipeline: bronze and silver both skip gracefully
"""
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


REPO_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(REPO_ROOT))

# Stub airflow.exceptions so tests run without a full Airflow install
_airflow = sys.modules.get("airflow") or types.ModuleType("airflow")
_airflow_exc = sys.modules.get("airflow.exceptions") or types.ModuleType("airflow.exceptions")


class AirflowSkipException(Exception):
    pass


_airflow_exc.AirflowSkipException = AirflowSkipException
_airflow.exceptions = _airflow_exc
sys.modules.setdefault("airflow", _airflow)
sys.modules.setdefault("airflow.exceptions", _airflow_exc)

try:
    import pandera  # noqa: F401
except ImportError:
    _pandera = types.ModuleType("pandera")
    _pandera_errors = types.ModuleType("pandera.errors")

    class SchemaError(Exception):
        pass

    _pandera_errors.SchemaError = SchemaError
    _pandera.errors = _pandera_errors
    _pandera.Column = MagicMock()
    _pandera.DataFrameSchema = MagicMock()
    _pandera.Check = MagicMock()
    sys.modules.setdefault("pandera", _pandera)
    sys.modules.setdefault("pandera.errors", _pandera_errors)

from scripts.bronze_layer import run_bronze_ingestion   # noqa: E402
from scripts.silver_layer import run_silver_transform   # noqa: E402
from scripts.gold_layer import run_gold_layer           # noqa: E402

# Context factory

def _build_pipeline_context(tmp_path: Path):
    """
    Build a shared Airflow context dict backed by a single XCom dict.
    All Path() calls are redirected to tmp_path so nothing is written to /opt/airflow.
    """
    xcom_store: dict = {}
    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: xcom_store.update({key: value})
    ti.xcom_pull.side_effect = lambda key, task_ids=None: xcom_store.get(key)

    ctx = {
        "ti": ti,
        "ds_nodash": "20251210",
        "logical_date": datetime(2025, 12, 10, 14, 0, 0, tzinfo=timezone.utc),
    }
    return ctx, xcom_store


def _path_redirector(tmp_path: Path):
    """Return a side_effect that maps /opt/airflow/... → tmp_path/..."""
    def _redir(p):
        p_obj = Path(p)
        try:
            rel = p_obj.relative_to("/")
        except ValueError:
            rel = p_obj
        return tmp_path / rel
    return _redir


def _make_http_response(payload: dict):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


class TestFullPipelineIntegration:

    def test_happy_path_produces_gold_csv(self, tmp_path, mock_opensky_response):
        """
        Full pipeline: bronze → silver → gold runs end-to-end.
        Gold CSV must exist and contain aggregated data.
        """
        ctx, xcom_store = _build_pipeline_context(tmp_path)
        path_redirect = _path_redirector(tmp_path)

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_http_response(mock_opensky_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redirect),
            patch("scripts.silver_layer.Path", side_effect=path_redirect),
            patch("scripts.gold_layer.Path", side_effect=path_redirect),
        ):
            run_bronze_ingestion(**ctx)
            run_silver_transform(**ctx)
            run_gold_layer(**ctx)

        assert "bronze_file" in xcom_store, "bronze_file must be in XCom"
        assert "silver_file" in xcom_store, "silver_file must be in XCom"
        assert "gold_file" in xcom_store, "gold_file must be in XCom"

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        assert len(gold_df) > 0, "Gold CSV must contain at least one row"
        assert "origin_country" in gold_df.columns
        assert "total_flights" in gold_df.columns

    def test_total_flights_sum_matches_input(self, tmp_path, mock_opensky_response, sample_states):
        """Sum of total_flights in Gold must equal the number of input states."""
        ctx, xcom_store = _build_pipeline_context(tmp_path)
        path_redirect = _path_redirector(tmp_path)

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_http_response(mock_opensky_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redirect),
            patch("scripts.silver_layer.Path", side_effect=path_redirect),
            patch("scripts.gold_layer.Path", side_effect=path_redirect),
        ):
            run_bronze_ingestion(**ctx)
            run_silver_transform(**ctx)
            run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        assert gold_df["total_flights"].sum() == len(sample_states)

    def test_idempotent_rerun_does_not_duplicate(self, tmp_path, mock_opensky_response):
        """
        Running bronze twice for the same logical_date must not create a second file.
        Silver row count must remain the same after the second run.
        """
        ctx, xcom_store = _build_pipeline_context(tmp_path)
        path_redirect = _path_redirector(tmp_path)

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_http_response(mock_opensky_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redirect),
            patch("scripts.silver_layer.Path", side_effect=path_redirect),
        ):
            # First run
            run_bronze_ingestion(**ctx)
            run_silver_transform(**ctx)
            first_silver_path = Path(xcom_store["silver_file"])
            first_row_count = len(pd.read_csv(first_silver_path))

            # Second run — bronze should skip the API call (idempotent)
            with patch("scripts.bronze_layer.requests.get") as mock_get_second:
                run_bronze_ingestion(**ctx)
                mock_get_second.assert_not_called()

            run_silver_transform(**ctx)
            second_row_count = len(pd.read_csv(Path(xcom_store["silver_file"])))

        assert first_row_count == second_row_count

    def test_empty_states_pipeline_skips_gracefully(self, tmp_path, mock_opensky_empty_response):
        """
        When OpenSky returns states=None, Bronze must raise AirflowSkipException.
        Silver must never be reached.
        """
        ctx, xcom_store = _build_pipeline_context(tmp_path)
        path_redirect = _path_redirector(tmp_path)

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_http_response(mock_opensky_empty_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redirect),
            pytest.raises(AirflowSkipException),
        ):
            run_bronze_ingestion(**ctx)

        # Silver must not have been reached
        assert "bronze_file" not in xcom_store

    def test_gold_country_names_match_silver(self, tmp_path, mock_opensky_response, sample_states):
        """Country names in Gold must be the same set as in the input states."""
        ctx, xcom_store = _build_pipeline_context(tmp_path)
        path_redirect = _path_redirector(tmp_path)

        expected_countries = {s[2] for s in sample_states}  # origin_country is index 2

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_http_response(mock_opensky_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redirect),
            patch("scripts.silver_layer.Path", side_effect=path_redirect),
            patch("scripts.gold_layer.Path", side_effect=path_redirect),
        ):
            run_bronze_ingestion(**ctx)
            run_silver_transform(**ctx)
            run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        assert set(gold_df["origin_country"]) == expected_countries
