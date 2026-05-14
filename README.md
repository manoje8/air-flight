## Air Flight — Flight Telemetry Data Pipeline

A local flight telemetry data engineering pipeline built with **Apache Airflow**, **Docker**, and **Snowflake-friendly** transformation patterns.

This repository demonstrates a full end-to-end data pipeline for ingesting flight JSON payloads, validating data quality, transforming records through **Bronze / Silver / Gold** layers, and loading results into a warehouse-ready schema. The stack is optimized for local development and experimentation.

---

### Architecture Overview

```
Raw JSON (Bronze)
      │
      ▼
flight_ingest DAG  ──►  data/bronze/
      │
      ▼
flight_transform DAG  ──►  Silver / Gold transformations + data cleaning
      │
      ▼
data_quality DAG  ──►  Validation checks at each layer
      │
      ▼
flight_load DAG  ──►  Warehouse-ready output (Snowflake-compatible)
      │
      ▼
flight_ml DAG  ──►  ML feature generation + scoring workflows
```

|Layer|Description|
|---|---|
|**Bronze**|Raw JSON ingested as-is from source|
|**Silver**|Cleaned, validated, and typed records|
|**Gold**|Aggregated, business-ready output|

---

### What's Included

|Directory / File|Purpose|
|---|---|
|`dags/`|Airflow DAGs: ingestion, transformation, loading, ML, data quality|
|`scripts/`|ETL helpers, Snowflake utilities, feature engineering, quality checks|
|`data/`|Sample Bronze / Silver / Gold flight datasets|
|`dbt/`|dbt project files for downstream modeling and testing|
|`dashboard/`|Lightweight app for inspecting pipeline outputs and metrics|
|`tests/`|Unit and integration test suite|
|`docker-compose.yml`|Compose stack definition|
|`Dockerfile`|Custom Airflow image|
|`config.py`|Central configuration|
|`Makefile`|Developer convenience commands|

---

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
- Python 3.9+ for local scripts, linting, and tests
- A `.env` file for local credentials and environment settings

---

### Quickstart

**1. Clone the repository**

bash

```bash
git clone https://github.com/manoje8/air-flight.git
cd airflow_flight
```

**2. Create a virtual environment (optional but recommended)**

bash

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

**3. Install Python dependencies**

bash

```bash
pip install -r requirements.txt
```

**4. Configure environment variables**

bash

```bash
cp .env.example .env
# Edit .env with your credentials — never commit this file
```

**5. Start the stack**

bash

```bash
docker compose up -d --build
```

**6. Open the Airflow UI** Navigate to [http://localhost:8080](http://localhost:8080)

---

### Makefile Commands

|Command|Description|
|---|---|
|`make up`|Start all containers|
|`make down`|Stop all containers|
|`make shutdown`|Stop containers and remove volumes|
|`make logs`|Follow container logs|
|`make test`|Run the full test suite|
|`make lint`|Run lint checks|
|`make format`|Auto-format source code|
|`make trigger-ingest`|Trigger the ingestion DAG|
|`make trigger-transform`|Trigger the transformation DAG|
|`make trigger-load`|Trigger the load DAG|
|`make trigger-ml`|Trigger the ML scoring DAG|

---

### Testing

bash

```bash
pytest tests/ -v --tb=short
```

---

### Infrastructure

The stack runs with **CeleryExecutor** for distributed task execution, **PostgreSQL** as the Airflow metadata database, and **Redis** as the Celery message broker.

---

###  Environment & Credentials

- Copy or create a `.env` file before starting the stack
- Compose services automatically mount `./.env` into the Airflow container
- **Never commit secrets or credentials to Git**

---

### DAG Reference

|DAG|Description|
|---|---|
|`flight_ingest`|Loads raw flight telemetry from `data/bronze/`|
|`flight_transform`|Applies Silver / Gold transformations and data cleaning|
|`flight_load`|Writes processed output to the target warehouse layer|
|`data_quality`|Validates ingestion and transformation stages|
|`flight_ml`|Runs ML feature generation and scoring workflows|

---

### Next Steps

- Review DAG definitions in `dags/`
- Inspect transformation logic in `scripts/` and `dbt/`
- Extend the pipeline with new quality checks or ML scoring models
- Connect a live Snowflake target by configuring credentials in `.env`

---

### Notes

- Raw JSON source files live in `data/bronze/`
- Use `logs/` and the Airflow UI for troubleshooting DAG executions
- The `dashboard/` directory contains a lightweight app for monitoring pipeline metrics
