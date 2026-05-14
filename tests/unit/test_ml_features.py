"""
Unit tests for scripts/ml_features.py
"""

import numpy as np
import pandas as pd
import pytest

from scripts.ml_features import (
    FEATURE_COLS,
    build_feature_store,
    build_score_features,
    engineer_features,
)
from sklearn.preprocessing import LabelEncoder

# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_silver_df(n: int = 20, include_ground: bool = True) -> pd.DataFrame:
    """Build a minimal Silver-schema DataFrame for testing."""
    rng = np.random.default_rng(42)
    data = {
        "icao24": [f"a{i:05d}" for i in range(n)],
        "origin_country": rng.choice(
            ["United States", "Germany", "France", "India"], n
        ).tolist(),
        "latitude": rng.uniform(-90.0, 90.0, n).tolist(),
        "longitude": rng.uniform(-180.0, 180.0, n).tolist(),
        "time_position": rng.integers(1_700_000_000, 1_700_100_000, n).tolist(),
        "last_contact": rng.integers(1_700_000_001, 1_700_100_001, n).tolist(),
        "velocity": rng.uniform(0.0, 350.0, n).tolist(),
        "vertical_rate": rng.uniform(-20.0, 20.0, n).tolist(),
        "true_track": rng.uniform(0.0, 360.0, n).tolist(),
        "baro_altitude": rng.uniform(0.0, 12_000.0, n).tolist(),
        "on_ground": (rng.random(n) < 0.2).tolist(),
    }
    return pd.DataFrame(data)


@pytest.fixture
def silver_df():
    return _make_silver_df(n=50)


@pytest.fixture
def silver_df_with_nulls():
    df = _make_silver_df(n=30)
    # Introduce some nulls in nullable columns
    df.loc[[0, 5, 10], "velocity"] = None
    df.loc[[1, 6], "baro_altitude"] = None
    df.loc[[2], "vertical_rate"] = None
    df.loc[[3, 7], "true_track"] = None
    df.loc[[4, 8], "latitude"] = None
    df.loc[[4, 8], "longitude"] = None
    return df


@pytest.fixture
def silver_csv_dir(tmp_path, silver_df):
    """Write two Silver CSVs to a tmp dir and return the dir path."""
    d = tmp_path / "silver"
    d.mkdir()
    silver_df.to_csv(d / "flight_silver_20260101.csv", index=False)
    silver_df.to_csv(d / "flight_silver_20260102.csv", index=False)
    return d


# ── engineer_features ─────────────────────────────────────────────────────────


class TestEngineerFeatures:

    def test_output_shapes_match(self, silver_df):
        X, y, enc = engineer_features(silver_df)
        assert X.shape[0] == y.shape[0], "X and y must have equal row counts"
        assert X.shape[1] == len(
            FEATURE_COLS
        ), "X must have exactly the expected feature columns"

    def test_feature_columns_present(self, silver_df):
        X, _, _ = engineer_features(silver_df)
        assert list(X.columns) == FEATURE_COLS

    def test_no_nulls_in_X(self, silver_df_with_nulls):
        X, y, _ = engineer_features(silver_df_with_nulls)
        assert (
            not X.isnull().any().any()
        ), "Feature matrix must contain no NaN after imputation"

    def test_no_inf_in_X(self, silver_df):
        X, _, _ = engineer_features(silver_df)
        assert not np.isinf(X.values).any(), "Feature matrix must contain no inf values"

    def test_y_is_binary(self, silver_df):
        _, y, _ = engineer_features(silver_df)
        assert set(y).issubset({0, 1}), "Target must be binary 0/1"

    def test_label_encoder_fitted(self, silver_df):
        _, _, enc = engineer_features(silver_df)
        assert isinstance(enc, LabelEncoder)
        assert len(enc.classes_) > 0

    def test_country_encoded_column_range(self, silver_df):
        X, _, enc = engineer_features(silver_df)
        max_encoded = X["country_encoded"].max()
        assert max_encoded < len(
            enc.classes_
        ), "Encoded values must be within encoder class range"

    def test_lat_lon_bucket_granularity(self, silver_df):
        X, _, _ = engineer_features(silver_df)
        # All lat_bucket values should be multiples of 10
        assert (X["lat_bucket"] % 10 == 0).all(), "lat_bucket should be multiples of 10"
        assert (X["lon_bucket"] % 10 == 0).all(), "lon_bucket should be multiples of 10"

    def test_reuse_encoder_no_refit(self, silver_df):
        _, _, enc = engineer_features(silver_df)
        original_classes = list(enc.classes_)

        # Second call with fit_encoder=False must preserve the encoder
        X2, _, enc2 = engineer_features(silver_df, label_encoder=enc, fit_encoder=False)
        assert list(enc2.classes_) == original_classes

    def test_unseen_country_handled_gracefully(self, silver_df):
        _, _, enc = engineer_features(silver_df)

        df_new = silver_df.copy()
        df_new["origin_country"] = "Wakanda"  # unseen country

        # Should not raise; unseen → "Unknown" fallback
        X, _, _ = engineer_features(df_new, label_encoder=enc, fit_encoder=False)
        assert not X.isnull().any().any()

    def test_empty_dataframe_raises(self):
        df = pd.DataFrame(columns=["icao24", "on_ground", "velocity"])
        with pytest.raises(ValueError, match="No valid rows"):
            engineer_features(df)


# ── build_feature_store ───────────────────────────────────────────────────────


class TestBuildFeatureStore:

    def test_loads_all_csvs(self, silver_csv_dir, silver_df):
        X, y, enc = build_feature_store(silver_csv_dir)
        # Two files × 50 rows each (minus any null-on_ground drops = 0 here)
        assert len(X) == len(silver_df) * 2

    def test_missing_dir_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            build_feature_store(tmp_path / "nonexistent")

    def test_empty_dir_raises(self, tmp_path):
        empty_dir = tmp_path / "silver_empty"
        empty_dir.mkdir()
        with pytest.raises(FileNotFoundError):
            build_feature_store(empty_dir)


# ── build_score_features ──────────────────────────────────────────────────────


class TestBuildScoreFeatures:

    def test_returns_X_and_meta(self, silver_csv_dir, silver_df):
        # First fit an encoder
        _, _, enc = engineer_features(silver_df)

        csv_file = next((silver_csv_dir).glob("*.csv"))
        X, meta = build_score_features(csv_file, enc)

        assert not X.empty
        assert "icao24" in meta.columns
        assert len(X) == len(meta), "X and meta must be row-aligned"

    def test_feature_cols_correct(self, silver_csv_dir, silver_df):
        _, _, enc = engineer_features(silver_df)
        csv_file = next(silver_csv_dir.glob("*.csv"))
        X, _ = build_score_features(csv_file, enc)
        assert list(X.columns) == FEATURE_COLS
