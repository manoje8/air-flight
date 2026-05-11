"""
Unit tests for scripts/silver_layer.py
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from airflow.exceptions import AirflowSkipException

sys.path.insert(0, str(Path(__file__).parents[2]))

from scripts.silver_layer import run_silver_transform, SILVER_COLUMNS  # noqa: E402


class TestSilverTransform:

    def test_transforms_and_writes_silver_csv(
        self, tmp_path, bronze_json_file, mock_context, xcom_store, path_redir
    ):
        """Happy path: silver CSV is written with correct columns."""
        xcom_store["bronze_file"] = str(bronze_json_file)

        with patch("scripts.silver_layer.Path", side_effect=path_redir):
            run_silver_transform(**mock_context)

        assert "silver_file" in xcom_store
        silver_path = Path(xcom_store["silver_file"])
        assert silver_path.exists()

        df = pd.read_csv(silver_path)
        assert set(SILVER_COLUMNS).issubset(set(df.columns))
        assert len(df) == 5

    def test_skip_on_none_states(self, tmp_path, mock_context, xcom_store, path_redir):
        """Bronze JSON with states=None should trigger AirflowSkipException."""
        empty_bronze = tmp_path / "flight_empty.json"
        empty_bronze.write_text(json.dumps({"time": 1700000000, "states": None}))
        xcom_store["bronze_file"] = str(empty_bronze)

        with (
            patch("scripts.silver_layer.Path", side_effect=path_redir),
            pytest.raises(AirflowSkipException),
        ):
            run_silver_transform(**mock_context)

    def test_raises_valueerror_on_missing_xcom(self, mock_context):
        """Missing XCom bronze_file key must raise ValueError."""
        with pytest.raises(ValueError, match="Bronze file path not found"):
            run_silver_transform(**mock_context)

    def test_icao24_column_values_preserved(
        self, tmp_path, bronze_json_file, mock_context, xcom_store, path_redir
    ):
        """icao24 values from raw states must be preserved in Silver."""
        xcom_store["bronze_file"] = str(bronze_json_file)

        with patch("scripts.silver_layer.Path", side_effect=path_redir):
            run_silver_transform(**mock_context)

        df = pd.read_csv(Path(xcom_store["silver_file"]))
        assert "abc123" in df["icao24"].values

    def test_output_filename_uses_exec_date(
        self, tmp_path, bronze_json_file, mock_context, xcom_store, path_redir
    ):
        """Silver filename must embed the execution date (ds_nodash)."""
        xcom_store["bronze_file"] = str(bronze_json_file)

        with patch("scripts.silver_layer.Path", side_effect=path_redir):
            run_silver_transform(**mock_context)

        assert "20251210" in xcom_store["silver_file"]
