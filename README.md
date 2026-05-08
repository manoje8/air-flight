# Air Flight

Flight Data Engineering Pipeline

This project implements an end-to-end flight data engineering pipeline using Apache Airflow, snowflake and Docker. 
The pipeline ingests flight data, performs transformations, and loads it into a data warehouse.

1. Architecture Overview


│ Flight Data │ → │  Airflow  (Orchestration)  │ → │  Data Warehouse  │


2. Prerequisites

  * Docker and Docker Compose installed
  * Python 3.9+
  * (Optional) Airflow CLI installed

3. Installation

1. Clone the repository:
   git clone <repository-url>
   cd airflow_flight

2. Install dependencies:
   pip install -r requirements.txt

3. Start the pipeline:
   docker-compose up -d

4. Verification

Airflow UI:
  * Open http://localhost:8080 in your browser
  * Log in with admin/admin
  * Verify the flight_data_pipeline DAG is present

Data Warehouse:
  * Verify data in the data warehouse tables

5. DAG Details

The flight_data_pipeline DAG performs the following steps:

  1. Fetch flight data from source
  2. Transform raw data into structured format
  3. Load data into data warehouse
  4. Run data quality checks
  5. Generate summary report

6. Configuration

Environment variables are defined in .env file:

  * AIRFLOW_HOME: Airflow home directory
  * DATA_SOURCE_URL: URL for flight data

7. Testing

Run unit tests:
  pytest tests

Run integration tests:
  # Run the DAG manually in Airflow UI
  # Check logs for test results

8. Stopping the Pipeline

docker-compose down

9. Common Issues

Docker not starting:
  * Ensure Docker is running
  * Check docker-compose logs for errors

Airflow UI not accessible:
  * Wait for Airflow to initialize (may take 1-2 minutes)
  * Check Airflow logs: docker-compose logs airflow-webserver
