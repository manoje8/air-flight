import json
from datetime import datetime
from pathlib import Path

import requests

OPEN_SKY_NETWORK_URI="https://opensky-network.org/api/states/all"


def run_bronze_ingestion(**context):
    response = requests.get(OPEN_SKY_NETWORK_URI, timeout=30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

    path = Path(f"/opt/airflow/data/bronze/flight_{timestamp}.json")

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f)

    context['ti'].xcom_push(key="bronze_file", value=str(path))
