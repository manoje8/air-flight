import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from airflow.exceptions import AirflowSkipException

OPEN_SKY_NETWORK_URI = "https://opensky-network.org/api/states/all"

logger = logging.getLogger(__name__)


def run_bronze_ingestion(**context) -> str:
    """
    Ingest raw flight state data from the OpenSky Network API into the Bronze layer.

    - Idempotent: uses the DAG's logical_date as the filename timestamp so that
      re-running the same interval always targets the same file.
    - Graceful empty response: raises AirflowSkipException when the API returns
      no flight states (states == None) instead of crashing downstream tasks.
    - Retry-aware: the task is configured in the DAG with retries=3 and
      exponential backoff to handle OpenSky rate limits (HTTP 429).
    """

    ti = context.get("ti")
    # Idempotency: derive a deterministic filename from the logical date
    logical_date = context["logical_date"]
    timestamp = logical_date.strftime("%Y-%m-%d-%H-%M-%S")
    path = Path(f"/opt/airflow/data/bronze/flight_{timestamp}.json")

    if path.exists():
        logger.info(
            "Bronze file already exists for interval %s — skipping ingestion (idempotent).",
            timestamp,
        )
        if ti:
            ti.xcom_push(key="bronze_file", value=str(path))
        return str(path)

    logger.info("Fetching flight data from OpenSky Network API...")
    response = requests.get(OPEN_SKY_NETWORK_URI, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not data or data.get("states") is None:
        raise AirflowSkipException(
            "OpenSky API returned no flight states (states=null). "
            "Skipping this interval gracefully."
        )

    logger.info("Received %d flight states.", len(data["states"]))

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f)

    if ti:
        ti.xcom_push(key="bronze_file", value=str(path))
    logger.info("Bronze file written: %s", path)
    return str(path)
