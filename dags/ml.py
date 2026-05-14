"""
flight_ml DAG — Machine Learning layer for the flight data pipeline.

Triggered by flight_transform after the Gold dbt run completes.
Receives ``silver_file`` from dag_run.conf.

Task flow
---------
get_silver_file
    └─► train_or_load_models
            └─► score_latest_batch
                    └─► load_predictions_to_snowflake
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

AIRFLOW_HOME = Path("/opt/airflow")
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.failure_callback import on_failure_callback  # noqa: E402
from scripts.ml_train import load_or_train  # noqa: E402
from scripts.ml_score import run_ml_score  # noqa: E402
from scripts.ml_snowflake import run_ml_snowflake_load  # noqa: E402

# Paths (inside the container volume)
_SILVER_DIR = "/opt/airflow/data/silver"
_ML_MODEL_DIR = "/opt/airflow/data/ml/models"
_ML_PRED_DIR = "/opt/airflow/data/ml/predictions"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="flight_ml",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # triggered by flight_transform
    catchup=False,
    max_active_runs=1,
    tags=["flight", "ml", "snowflake", "v1"],
    doc_md="""
## flight_ml DAG

Adds a machine-learning inference layer on top of the medallion pipeline.

### Models
| Model | Type | Target |
|-------|------|--------|
| Random Forest | Binary classifier | `on_ground` (airborne vs landed) |
| Isolation Forest | Unsupervised | Velocity + altitude anomaly score |

### Flow
`get_silver_file → train_or_load → score_batch → load_to_snowflake`

Models are retrained from all accumulated Silver CSVs only when the cached
`.pkl` files are older than **24 hours** — otherwise the cached models are
reused for scoring only.

Predictions are written to `data/ml/predictions/` and upserted into
`GOLD_ML_PREDICTIONS` in Snowflake using the staging + MERGE pattern.
    """,
)
def ml():

    @task
    def get_silver_file(**context) -> str:
        silver_file: str = context["dag_run"].conf.get("silver_file", "")
        if not silver_file:
            raise ValueError("flight_ml: no silver_file received in dag_run.conf.")
        return silver_file

    @task
    def train_or_load(**context) -> dict:
        """
        Train (or load cached) RF + IsolationForest models.

        Returns a dict of model file paths so downstream tasks can load them
        without re-training (XCom passes paths, not model objects).
        """
        rf, iforest, encoder = load_or_train(
            silver_dir=_SILVER_DIR,
            model_dir=_ML_MODEL_DIR,
        )

        return {
            "rf_path": f"{_ML_MODEL_DIR}/rf_onground.pkl",
            "iforest_path": f"{_ML_MODEL_DIR}/iforest_velocity.pkl",
            "encoder_path": f"{_ML_MODEL_DIR}/label_encoder.pkl",
        }

    @task
    def score_batch(silver_file: str, **context) -> str:
        return run_ml_score(silver_file=silver_file, **context)

    @task
    def load_to_snowflake(ml_predictions_file: str, **context) -> dict:
        return run_ml_snowflake_load(ml_predictions_file=ml_predictions_file, **context)

    # Wire up
    silver_file = get_silver_file()
    model_paths = train_or_load()
    predictions = score_batch(silver_file)
    load_to_snowflake(predictions)

    # Models must be ready before scoring
    model_paths >> predictions


ml()
