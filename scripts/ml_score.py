"""
Score the latest Silver batch using trained ML models.

Produces a DataFrame with per-flight predictions:
    icao24               — flight transponder ID
    origin_country       — country of origin
    velocity             — observed velocity (m/s)
    baro_altitude        — observed altitude (m)
    predicted_on_ground  — RF classifier output (0=airborne, 1=on_ground)
    onground_probability — probability of on_ground class [0.0–1.0]
    anomaly_score        — IsolationForest raw decision score (lower = more anomalous)
    is_anomaly           — True if IsolationForest flags as outlier
    window_start         — execution timestamp of the DAG run (UTC)

Entry-point
-----------
    from scripts.ml_score import score_batch

    result_file = score_batch(silver_file, model_dir, predictions_dir, window_start)
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

from scripts.ml_features import build_score_features

logger = logging.getLogger(__name__)

ANOMALY_COLS = ["velocity", "baro_altitude", "vertical_rate"]


def score_batch(
    silver_file: str | Path,
    model_dir: str | Path,
    predictions_dir: str | Path,
    window_start: str | None = None,
    rf: RandomForestClassifier | None = None,
    iforest: IsolationForest | None = None,
    encoder: LabelEncoder | None = None,
) -> str:
    """
    Score a single Silver CSV and write predictions to *predictions_dir*.

    Models can be passed directly (e.g. from a previous training step in the
    same DAG run) or loaded from *model_dir* if not supplied.

    Parameters
    ----------
    silver_file     : path to the Silver CSV to score
    model_dir       : directory containing pkl model files
    predictions_dir : output directory for the predictions CSV
    window_start    : ISO timestamp string for the DAG run (defaults to now)
    rf, iforest, encoder : pre-loaded model objects (optional)

    Returns
    -------
    str : absolute path to the predictions CSV written
    """
    import joblib

    silver_file = Path(silver_file)
    model_dir = Path(model_dir)
    pred_dir = Path(predictions_dir)

    # Load models if not passed
    if rf is None:
        rf = joblib.load(model_dir / "rf_onground.pkl")
        logger.info("RF model loaded from %s", model_dir)

    if iforest is None:
        iforest = joblib.load(model_dir / "iforest_velocity.pkl")
        logger.info("IsolationForest loaded from %s", model_dir)

    if encoder is None:
        encoder = joblib.load(model_dir / "label_encoder.pkl")
        logger.info("LabelEncoder loaded from %s", model_dir)

    # Feature engineering
    logger.info("Engineering features from: %s", silver_file)
    X, meta = build_score_features(silver_file, encoder)

    if X.empty:
        raise ValueError(f"No scoreable rows in {silver_file}")

    logger.info("Scoring %d flights...", len(X))

    # RF predictions
    predicted_on_ground = rf.predict(X)
    onground_proba = rf.predict_proba(X)[:, 1]  # prob of class=1 (on_ground)

    # Isolation Forest anomaly scores
    X_anom = X[ANOMALY_COLS]
    raw_anomaly_score = iforest.decision_function(X_anom)  # higher = more normal
    is_anomaly = iforest.predict(X_anom) == -1  # -1 = outlier

    # Assemble results DataFrame
    results = meta.copy()
    results["predicted_on_ground"] = predicted_on_ground.astype(bool)
    results["onground_probability"] = onground_proba.round(4)
    results["anomaly_score"] = raw_anomaly_score.round(6)
    results["is_anomaly"] = is_anomaly

    ts = window_start or datetime.now(timezone.utc).isoformat()
    results["window_start"] = ts

    # Write to disk
    pred_dir.mkdir(parents=True, exist_ok=True)
    date_tag = silver_file.stem.replace("flight_silver_", "")
    output_path = pred_dir / f"ml_{date_tag}.csv"
    results.to_csv(output_path, index=False)

    total = len(results)
    n_ground = int(results["predicted_on_ground"].sum())
    n_anomaly = int(results["is_anomaly"].sum())
    logger.info(
        "Scoring complete — %d flights | %d predicted on-ground | %d anomalies → %s",
        total,
        n_ground,
        n_anomaly,
        output_path,
    )

    return str(output_path)


def run_ml_score(silver_file: str, **context) -> str:
    """
    Airflow task wrapper around :func:`score_batch`.

    Expects *silver_file* from DAG conf or XCom.
    Paths are resolved from Airflow-standard locations.
    """
    from pathlib import Path

    ti = context.get("ti")

    if not silver_file and ti:
        silver_file = ti.xcom_pull(key="silver_file")

    if not silver_file:
        raise ValueError("ml_score: silver_file not found in conf or XCom.")

    model_dir = Path("/opt/airflow/data/ml/models")
    predictions_dir = Path("/opt/airflow/data/ml/predictions")
    window_start = context.get("data_interval_start")
    window_ts = window_start.isoformat() if window_start else None

    output_file = score_batch(
        silver_file=silver_file,
        model_dir=model_dir,
        predictions_dir=predictions_dir,
        window_start=window_ts,
    )

    if ti:
        ti.xcom_push(key="ml_predictions_file", value=output_file)

    return output_file
