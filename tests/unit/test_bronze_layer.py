"""
Unit tests for scripts/bronze_layer.py
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from airflow.exceptions import AirflowSkipException

# Make the scripts package importable
sys.path.insert(0, str(Path(__file__).parents[2]))

from scripts.bronze_layer import run_bronze_ingestion  # noqa: E402

def _make_mock_response(payload: dict, status_code: int = 200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp

# Tests

class TestBronzeIngestion:

    def test_creates_json_file_and_pushes_xcom(
        self, tmp_path, mock_opensky_response, mock_context, xcom_store, path_redir
    ):
        """Happy path: file is written, xcom key is set."""
        expected_path = path_redir("/opt/airflow/data/bronze/flight_2025-12-10-14-00-00.json")

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_mock_response(mock_opensky_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redir),
        ):
            run_bronze_ingestion(**mock_context)

        assert expected_path.exists(), f"Bronze JSON file should be created at {expected_path}"
        written = json.loads(expected_path.read_text())
        assert written["states"] == mock_opensky_response["states"]
        assert xcom_store.get("bronze_file") == str(expected_path)

    def test_idempotent_skips_rewrite(self, tmp_path, mock_opensky_response, mock_context, xcom_store, path_redir):
        """
        Re-running for the same logical_date must NOT overwrite the existing file.
        """
        existing_path = path_redir("/opt/airflow/data/bronze/flight_2025-12-10-14-00-00.json")
        existing_path.parent.mkdir(parents=True, exist_ok=True)
        existing_path.write_text(json.dumps({"sentinel": True}))
        mtime_before = existing_path.stat().st_mtime

        with (
            patch("scripts.bronze_layer.requests.get") as mock_get,
            patch("scripts.bronze_layer.Path", side_effect=path_redir),
        ):
            run_bronze_ingestion(**mock_context)
            mock_get.assert_not_called()

        assert existing_path.stat().st_mtime == mtime_before
        assert xcom_store.get("bronze_file") == str(existing_path)

    def test_skip_on_empty_states(self, tmp_path, mock_opensky_empty_response, mock_context, path_redir):
        """states=None from API should raise AirflowSkipException."""
        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_mock_response(mock_opensky_empty_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redir),
            pytest.raises(AirflowSkipException),
        ):
            run_bronze_ingestion(**mock_context)

    def test_http_error_propagates(self, tmp_path, mock_context, path_redir):
        """HTTP 429 must propagate."""
        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_mock_response({}, status_code=429)),
            patch("scripts.bronze_layer.Path", side_effect=path_redir),
            pytest.raises(requests.HTTPError),
        ):
            run_bronze_ingestion(**mock_context)

    def test_json_content_matches_api_response(self, tmp_path, mock_opensky_response, mock_context, path_redir):
        """The written JSON must exactly mirror what the API returned."""
        expected_path = path_redir("/opt/airflow/data/bronze/flight_2025-12-10-14-00-00.json")

        with (
            patch("scripts.bronze_layer.requests.get",
                  return_value=_make_mock_response(mock_opensky_response)),
            patch("scripts.bronze_layer.Path", side_effect=path_redir),
        ):
            run_bronze_ingestion(**mock_context)

        written = json.loads(expected_path.read_text())
        assert written == mock_opensky_response
