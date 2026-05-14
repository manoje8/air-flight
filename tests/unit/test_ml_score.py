"""
Unit tests for scripts/ml_score.py
"""

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from scripts.ml_score import score_batch

# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_silver_csv(tmp_path: Path, n: int = 15) -> Path:
    """Write a minimal Silver CSV to tmp_path/silver/ and return its path."""
    rng = np.random.default_rng(0)
    data = {
        "icao24": [f"b{i:05d}" for i in range(n)],
        "origin_country": rng.choice(["United States", "Germany"], n).tolist(),
        "latitude": rng.uniform(-90.0, 90.0, n).tolist(),
        "longitude": rng.uniform(-180.0, 180.0, n).tolist(),
        "time_position": rng.integers(1_700_000_000, 1_700_100_000, n).tolist(),
        "last_contact": rng.integers(1_700_000_001, 1_700_100_001, n).tolist(),
        "velocity": rng.uniform(0.0, 300.0, n).tolist(),
        "vertical_rate": rng.uniform(-10.0, 10.0, n).tolist(),
        "true_track": rng.uniform(0.0, 360.0, n).tolist(),
        "baro_altitude": rng.uniform(0.0, 11_000.0, n).tolist(),
        "on_ground": (rng.random(n) < 0.3).tolist(),
    }
    p = tmp_path / "silver" / "flight_silver_20260101.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(data).to_csv(p, index=False)
    return p


def _make_mock_rf(n: int):
    """Return a MagicMock mimicking a fitted RandomForestClassifier."""
    rf = MagicMock()
    rf.predict.return_value = np.zeros(n, dtype=int)
    rf.predict_proba.return_value = np.column_stack(
        [np.ones(n) * 0.8, np.ones(n) * 0.2]
    )
    return rf


def _make_mock_iforest(n: int):
    """Return a MagicMock mimicking a fitted IsolationForest."""
    iforest = MagicMock()
    iforest.decision_function.return_value = np.random.default_rng(1).uniform(
        -0.3, 0.3, n
    )
    iforest.predict.return_value = np.ones(n, dtype=int)  # all normal
    return iforest


def _make_mock_encoder(countries):
    """Return a MagicMock mimicking a fitted LabelEncoder."""
    from sklearn.preprocessing import LabelEncoder

    enc = LabelEncoder()
    enc.fit(list(countries) + ["Unknown"])
    return enc


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestScoreBatch:

    def test_output_file_created(self, tmp_path):
        n = 15
        silver_file = _make_silver_csv(tmp_path, n)
        rf = _make_mock_rf(n)
        iforest = _make_mock_iforest(n)
        encoder = _make_mock_encoder(["United States", "Germany"])

        out_path = score_batch(
            silver_file=silver_file,
            model_dir=tmp_path / "models",  # not used (models passed directly)
            predictions_dir=tmp_path / "preds",
            window_start="2026-01-01T00:00:00+00:00",
            rf=rf,
            iforest=iforest,
            encoder=encoder,
        )

        assert Path(out_path).exists(), "Predictions CSV must be written to disk"

    def test_output_columns_present(self, tmp_path):
        n = 15
        silver_file = _make_silver_csv(tmp_path, n)
        rf = _make_mock_rf(n)
        iforest = _make_mock_iforest(n)
        encoder = _make_mock_encoder(["United States", "Germany"])

        out_path = score_batch(
            silver_file=silver_file,
            model_dir=tmp_path / "models",
            predictions_dir=tmp_path / "preds",
            window_start="2026-01-01T00:00:00+00:00",
            rf=rf,
            iforest=iforest,
            encoder=encoder,
        )

        df = pd.read_csv(out_path)
        required_cols = [
            "icao24",
            "predicted_on_ground",
            "onground_probability",
            "anomaly_score",
            "is_anomaly",
            "window_start",
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing expected column: {col}"

    def test_row_count_matches_silver(self, tmp_path):
        n = 15
        silver_file = _make_silver_csv(tmp_path, n)
        rf = _make_mock_rf(n)
        iforest = _make_mock_iforest(n)
        encoder = _make_mock_encoder(["United States", "Germany"])

        out_path = score_batch(
            silver_file=silver_file,
            model_dir=tmp_path / "models",
            predictions_dir=tmp_path / "preds",
            window_start="2026-01-01T00:00:00+00:00",
            rf=rf,
            iforest=iforest,
            encoder=encoder,
        )

        df = pd.read_csv(out_path)
        assert len(df) == n, f"Expected {n} rows, got {len(df)}"

    def test_probability_range(self, tmp_path):
        n = 15
        silver_file = _make_silver_csv(tmp_path, n)
        rf = _make_mock_rf(n)
        iforest = _make_mock_iforest(n)
        encoder = _make_mock_encoder(["United States", "Germany"])

        out_path = score_batch(
            silver_file=silver_file,
            model_dir=tmp_path / "models",
            predictions_dir=tmp_path / "preds",
            rf=rf,
            iforest=iforest,
            encoder=encoder,
        )

        df = pd.read_csv(out_path)
        assert (
            df["onground_probability"].between(0.0, 1.0).all()
        ), "onground_probability must be in [0.0, 1.0]"

    def test_is_anomaly_is_boolean(self, tmp_path):
        n = 15
        silver_file = _make_silver_csv(tmp_path, n)
        rf = _make_mock_rf(n)
        iforest = _make_mock_iforest(n)
        encoder = _make_mock_encoder(["United States", "Germany"])

        out_path = score_batch(
            silver_file=silver_file,
            model_dir=tmp_path / "models",
            predictions_dir=tmp_path / "preds",
            rf=rf,
            iforest=iforest,
            encoder=encoder,
        )

        df = pd.read_csv(out_path)
        assert df["is_anomaly"].dtype in [
            bool,
            object,
            "bool",
        ], "is_anomaly must be boolean-compatible"

    def test_window_start_propagated(self, tmp_path):
        n = 15
        silver_file = _make_silver_csv(tmp_path, n)
        rf = _make_mock_rf(n)
        iforest = _make_mock_iforest(n)
        encoder = _make_mock_encoder(["United States", "Germany"])
        ts = "2026-05-14T12:00:00+00:00"

        out_path = score_batch(
            silver_file=silver_file,
            model_dir=tmp_path / "models",
            predictions_dir=tmp_path / "preds",
            window_start=ts,
            rf=rf,
            iforest=iforest,
            encoder=encoder,
        )

        df = pd.read_csv(out_path)
        assert (
            df["window_start"] == ts
        ).all(), "window_start must be stamped on all rows"

    def test_missing_silver_file_raises(self, tmp_path):
        encoder = _make_mock_encoder(["United States"])
        with pytest.raises((FileNotFoundError, Exception)):
            score_batch(
                silver_file=tmp_path / "nonexistent.csv",
                model_dir=tmp_path / "models",
                predictions_dir=tmp_path / "preds",
                rf=_make_mock_rf(1),
                iforest=_make_mock_iforest(1),
                encoder=encoder,
            )
