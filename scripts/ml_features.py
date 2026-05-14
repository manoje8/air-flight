"""
Feature engineering for the flight ML layer.

Reads all Silver CSVs from ``silver_dir`` (or a single file), cleans the
data, engineers numeric features, and returns an (X, y) tuple ready for
scikit-learn estimators.

Feature set
-----------
velocity            — raw m/s from OpenSky (nullable → median-imputed)
baro_altitude       — pressure altitude in metres (nullable → 0 for ground)
vertical_rate       — climb/descent rate m/s (nullable → 0)
true_track          — heading 0-360° (nullable → 180 = neutral)
lat_bucket          — latitude rounded to nearest 10° grid cell
lon_bucket          — longitude rounded to nearest 10° grid cell
country_encoded     — LabelEncoded origin_country ordinal

Target (y)
----------
on_ground  — boolean (1 = on ground, 0 = airborne)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

# ── Column definitions ────────────────────────────────────────────────────────

FEATURE_COLS = [
    "velocity",
    "baro_altitude",
    "vertical_rate",
    "true_track",
    "lat_bucket",
    "lon_bucket",
    "country_encoded",
]

TARGET_COL = "on_ground"

# Imputation defaults for nullable numeric columns
_IMPUTE = {
    "velocity": None,  # filled with median across dataset
    "baro_altitude": 0.0,  # 0 = ground level
    "vertical_rate": 0.0,  # no climb / no descent
    "true_track": 180.0,  # south — arbitrary neutral heading
}


def _load_silver_dir(silver_dir: str | Path) -> pd.DataFrame:
    """Load and union all Silver CSVs from *silver_dir*."""
    silver_path = Path(silver_dir)
    csv_files = sorted(silver_path.glob("flight_silver_*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No Silver CSV files found in {silver_path}")

    frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(
                f,
                dtype={"time_position": "Int64", "last_contact": "Int64"},
            )
            frames.append(df)
            logger.debug("Loaded %s (%d rows)", f.name, len(df))
        except Exception as exc:
            logger.warning("Skipping %s — %s", f.name, exc)

    if not frames:
        raise ValueError("All Silver CSV files failed to load.")

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d Silver rows from %d files", len(combined), len(frames))
    return combined


def _load_single_silver(silver_file: str | Path) -> pd.DataFrame:
    """Load a single Silver CSV."""
    return pd.read_csv(
        silver_file,
        dtype={"time_position": "Int64", "last_contact": "Int64"},
    )


def engineer_features(
    df: pd.DataFrame,
    label_encoder: LabelEncoder | None = None,
    fit_encoder: bool = True,
) -> tuple[pd.DataFrame, np.ndarray | None, LabelEncoder]:
    """
    Engineer ML features from a Silver-schema DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw Silver data (columns per ``utils.constants.SILVER_COLUMNS``).
    label_encoder : LabelEncoder, optional
        Pre-fitted encoder for ``origin_country``.  Pass when scoring to avoid
        refit on unseen categories.
    fit_encoder : bool
        If True, fit a new LabelEncoder on the current data.

    Returns
    -------
    X : pd.DataFrame
        Feature matrix with columns in FEATURE_COLS order.
    y : np.ndarray or None
        Target vector (int 0/1) if ``on_ground`` is present, else None.
    label_encoder : LabelEncoder
        The (possibly newly fitted) encoder — persist this alongside the model.
    """
    df = df.copy()

    # ── Drop rows with no positional data and no ground flag ──────────────────
    required = ["on_ground"]
    df = df.dropna(subset=required)

    if df.empty:
        raise ValueError("No valid rows after dropping nulls on required columns.")

    # ── Impute numeric nulls ─────────────────────────────────────────────────
    vel_median = df["velocity"].median()
    impute_values = {**_IMPUTE, "velocity": vel_median}

    for col, fill in impute_values.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(fill)

    # ── Grid bucketing for lat / lon ─────────────────────────────────────────
    df["lat_bucket"] = (df["latitude"].fillna(0.0) / 10).round(0) * 10
    df["lon_bucket"] = (df["longitude"].fillna(0.0) / 10).round(0) * 10

    # ── Country encoding ─────────────────────────────────────────────────────
    country_series = df["origin_country"].fillna("Unknown").astype(str)

    if label_encoder is None or fit_encoder:
        label_encoder = LabelEncoder()
        label_encoder.fit(country_series)

    # Handle unseen countries at score-time by mapping to 0
    known = set(label_encoder.classes_)
    country_safe = country_series.where(country_series.isin(known), "Unknown")
    if "Unknown" not in known:
        # Extend encoder classes to include Unknown
        label_encoder.classes_ = np.append(label_encoder.classes_, "Unknown")

    df["country_encoded"] = label_encoder.transform(country_safe)

    # ── Assemble feature matrix ───────────────────────────────────────────────
    X = df[FEATURE_COLS].astype(float)

    # ── Target vector ─────────────────────────────────────────────────────────
    y: np.ndarray | None = None
    if TARGET_COL in df.columns:
        y = df[TARGET_COL].astype(bool).astype(int).values

    logger.info(
        "Feature engineering complete — X: %s, y: %s",
        X.shape,
        y.shape if y is not None else "None",
    )

    return X, y, label_encoder


def build_feature_store(
    silver_dir: str | Path,
    label_encoder: LabelEncoder | None = None,
    fit_encoder: bool = True,
) -> tuple[pd.DataFrame, np.ndarray, LabelEncoder]:
    """
    Load all Silver CSVs and return the full (X, y, encoder) tuple.

    Raises ValueError if y is None (no on_ground column found).
    """
    df = _load_silver_dir(silver_dir)
    X, y, enc = engineer_features(
        df, label_encoder=label_encoder, fit_encoder=fit_encoder
    )

    if y is None:
        raise ValueError("Silver data has no 'on_ground' column — cannot train.")

    return X, y, enc


def build_score_features(
    silver_file: str | Path,
    label_encoder: LabelEncoder,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load a single Silver CSV, engineer features for scoring.

    Returns
    -------
    X : pd.DataFrame      — feature matrix
    meta : pd.DataFrame   — icao24 + original columns kept for the results table
    """
    df = _load_single_silver(silver_file)

    # keep icao24 for join-back
    meta_cols = ["icao24", "origin_country", "velocity", "baro_altitude", "on_ground"]
    meta = df[[c for c in meta_cols if c in df.columns]].copy()

    X, _, enc = engineer_features(df, label_encoder=label_encoder, fit_encoder=False)

    # Align lengths — engineer_features drops nulls; meta must match
    df_work = df.dropna(subset=["on_ground"]).copy()
    meta = meta.loc[df_work.index].reset_index(drop=True)
    X = X.reset_index(drop=True)

    return X, meta
