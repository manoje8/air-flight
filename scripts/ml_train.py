"""
Model training and persistence for the flight ML layer.

Models
------
1. RandomForestClassifier  — predicts ``on_ground`` (binary classification).
2. IsolationForest         — unsupervised velocity anomaly detector.

Both models are pickled to *model_dir* alongside a LabelEncoder used during
feature engineering.  A freshness check prevents unnecessary retraining on
every 30-min Airflow run: if the model files are younger than
``MAX_MODEL_AGE_HOURS`` (default 24h) they are loaded without retraining.

Entry-point
-----------
    from scripts.ml_train import load_or_train

    rf, iforest, encoder = load_or_train(silver_dir, model_dir)
"""

import logging
import time
from pathlib import Path

import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import LabelEncoder

from scripts.ml_features import build_feature_store, FEATURE_COLS

logger = logging.getLogger(__name__)

# Constants

MAX_MODEL_AGE_HOURS = 24
RF_MODEL_FILE = "rf_onground.pkl"
IFOREST_MODEL_FILE = "iforest_velocity.pkl"
ENCODER_FILE = "label_encoder.pkl"

# Random Forest hyper-parameters
RF_PARAMS = {
    "n_estimators": 200,
    "max_depth": 10,
    "min_samples_leaf": 5,
    "n_jobs": -1,
    "random_state": 42,
    "class_weight": "balanced",  # handles any on_ground class imbalance
}

# Isolation Forest hyper-parameters
# contamination = expected fraction of anomalies (conservative 5%)
IFOREST_PARAMS = {
    "n_estimators": 100,
    "contamination": 0.05,
    "random_state": 42,
    "n_jobs": -1,
}


# Freshness check


def _models_are_fresh(
    model_dir: Path, max_age_hours: float = MAX_MODEL_AGE_HOURS
) -> bool:
    """Return True if all model files exist and are younger than *max_age_hours*."""
    required = [RF_MODEL_FILE, IFOREST_MODEL_FILE, ENCODER_FILE]
    for fname in required:
        p = model_dir / fname
        if not p.exists():
            logger.info("Model file missing: %s — will retrain.", fname)
            return False
        age_hours = (time.time() - p.stat().st_mtime) / 3600
        if age_hours > max_age_hours:
            logger.info(
                "Model file %s is %.1fh old (> %sh threshold) — will retrain.",
                fname,
                age_hours,
                max_age_hours,
            )
            return False
    return True


# Training


def train_classifier(
    X: pd.DataFrame,
    y: np.ndarray,
) -> RandomForestClassifier:
    """Train a Random Forest classifier and log evaluation metrics."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    logger.info(
        "Training RandomForestClassifier on %d samples (%d features)...",
        len(X_train),
        X_train.shape[1],
    )
    t0 = time.time()
    rf = RandomForestClassifier(**RF_PARAMS)
    rf.fit(X_train, y_train)
    elapsed = time.time() - t0

    y_pred = rf.predict(X_test)
    report = classification_report(
        y_test, y_pred, target_names=["airborne", "on_ground"]
    )
    logger.info(
        "RF training complete in %.1fs\n%s",
        elapsed,
        report,
    )

    # Log feature importances
    importances = sorted(
        zip(FEATURE_COLS, rf.feature_importances_), key=lambda x: x[1], reverse=True
    )
    for feat, imp in importances:
        logger.info("  Feature importance — %s: %.4f", feat, imp)

    return rf


def train_anomaly_detector(X: pd.DataFrame) -> IsolationForest:
    """
    Train an Isolation Forest on the velocity feature sub-space.

    Uses velocity + baro_altitude + vertical_rate as the anomaly signal space.
    """
    anomaly_cols = ["velocity", "baro_altitude", "vertical_rate"]
    X_anom = X[anomaly_cols]

    logger.info(
        "Training IsolationForest on %d samples (%d features)...",
        len(X_anom),
        X_anom.shape[1],
    )
    t0 = time.time()
    iforest = IsolationForest(**IFOREST_PARAMS)
    iforest.fit(X_anom)
    elapsed = time.time() - t0

    # Quick sanity: fraction flagged as anomaly in training set
    preds = iforest.predict(X_anom)
    anomaly_frac = (preds == -1).mean()
    logger.info(
        "IsolationForest training complete in %.1fs — anomaly fraction: %.2f%%",
        elapsed,
        anomaly_frac * 100,
    )

    return iforest


# Persistence


def save_models(
    rf: RandomForestClassifier,
    iforest: IsolationForest,
    encoder: LabelEncoder,
    model_dir: Path,
) -> None:
    """Persist all three model artefacts to *model_dir*."""
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(rf, model_dir / RF_MODEL_FILE)
    joblib.dump(iforest, model_dir / IFOREST_MODEL_FILE)
    joblib.dump(encoder, model_dir / ENCODER_FILE)
    logger.info("Models saved to %s", model_dir)


def load_models(
    model_dir: Path,
) -> tuple[RandomForestClassifier, IsolationForest, LabelEncoder]:
    """Load all three model artefacts from *model_dir*."""
    rf = joblib.load(model_dir / RF_MODEL_FILE)
    iforest = joblib.load(model_dir / IFOREST_MODEL_FILE)
    encoder = joblib.load(model_dir / ENCODER_FILE)
    logger.info("Models loaded from %s", model_dir)
    return rf, iforest, encoder


# Entry-point


def load_or_train(
    silver_dir: str | Path,
    model_dir: str | Path,
    force_retrain: bool = False,
    max_age_hours: float = MAX_MODEL_AGE_HOURS,
) -> tuple[RandomForestClassifier, IsolationForest, LabelEncoder]:
    """
    Return (rf, iforest, encoder).

    If cached models are fresh (< *max_age_hours* old) they are loaded
    without retraining, unless *force_retrain* is True.

    Parameters
    ----------
    silver_dir     : directory containing Silver CSV files for training
    model_dir      : directory where model pkl files are read from / written to
    force_retrain  : bypass freshness check and always retrain
    max_age_hours  : freshness threshold in hours (default 24)
    """
    model_dir = Path(model_dir)

    if not force_retrain and _models_are_fresh(model_dir, max_age_hours):
        logger.info("Cached models are fresh — skipping retraining.")
        return load_models(model_dir)

    logger.info("Building feature store from Silver directory: %s", silver_dir)
    X, y, encoder = build_feature_store(silver_dir)

    rf = train_classifier(X, y)
    iforest = train_anomaly_detector(X)

    save_models(rf, iforest, encoder, model_dir)

    return rf, iforest, encoder
