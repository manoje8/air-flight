"""
Unit tests for scripts/gold_layer.py

Covers:
  - Aggregation produces correct total_flights, avg_velocity, on_ground counts
  - Output file is written to the gold directory
  - XCom push contains the gold file path
  - ValueError propagated when silver_file XCom key is missing
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from scripts.gold_layer import run_gold_layer  # noqa: E402


def _make_context(silver_file_path=None):
    xcom_store = {}
    if silver_file_path:
        xcom_store["silver_file"] = str(silver_file_path)

    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: xcom_store.update({key: value})
    ti.xcom_pull.side_effect = lambda key, task_ids=None: xcom_store.get(key)

    return {"ti": ti}, xcom_store


class TestGoldLayer:

    def test_aggregation_total_flights(self, tmp_path, silver_csv_file):
        """Each distinct origin_country should appear once in Gold with correct count."""
        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        us_row = gold_df[gold_df["origin_country"] == "United States"]
        assert len(us_row) == 1
        assert us_row.iloc[0]["total_flights"] == 1

    def test_aggregation_avg_velocity(self, tmp_path, silver_csv_file):
        """avg_velocity must equal the mean velocity for each country."""
        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        uk_row = gold_df[gold_df["origin_country"] == "United Kingdom"]
        assert abs(uk_row.iloc[0]["avg_velocity"] - 280.0) < 0.01

    def test_aggregation_on_ground_count(self, tmp_path, silver_csv_file):
        """on_ground sum must be 0 when all flights are airborne."""
        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        assert gold_df["on_ground"].sum() == 0

    def test_gold_output_file_exists(self, tmp_path, silver_csv_file):
        """Gold CSV file must be written to disk."""
        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        gold_path = Path(xcom_store["gold_file"])
        assert gold_path.exists(), "Gold CSV file should be written"

    def test_gold_xcom_push_called(self, tmp_path, silver_csv_file):
        """The gold_file key must be pushed to XCom."""
        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        assert "gold_file" in xcom_store
        assert xcom_store["gold_file"].endswith(".csv")

    def test_gold_output_columns(self, tmp_path, silver_csv_file):
        """Gold CSV must contain the expected aggregation columns."""
        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        for col in ["origin_country", "total_flights", "avg_velocity", "on_ground"]:
            assert col in gold_df.columns, f"Missing column: {col}"

    def test_gold_row_count_equals_unique_countries(self, tmp_path, silver_csv_file):
        """Gold should have one row per unique origin_country in Silver."""
        silver_df = pd.read_csv(silver_csv_file)
        expected_countries = silver_df["origin_country"].nunique()

        ctx, xcom_store = _make_context(silver_file_path=silver_csv_file)
        run_gold_layer(**ctx)

        gold_df = pd.read_csv(Path(xcom_store["gold_file"]))
        assert len(gold_df) == expected_countries

    def test_missing_silver_xcom_raises(self):
        """gold_layer must raise when silver_file XCom key is absent."""
        ctx, _ = _make_context(silver_file_path=None)

        with pytest.raises(Exception):
            run_gold_layer(**ctx)
