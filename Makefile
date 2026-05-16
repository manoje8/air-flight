# Makefile
.PHONY: up down shutdown restart ps \
		logs logs-worker logs-scheduler logs-webserver \
		test lint format shell db-migrate worker-scale \
		ui stream-run


COMPOSE = docker compose
AIRFLOW_EXEC = $(COMPOSE) exec airflow-webserver airflow


up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

shutdown:
	$(COMPOSE) down -v

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

# Logs

logs:
	$(COMPOSE) logs -f --tail=100

logs-worker:
	$(COMPOSE) logs -f --tail=100 airflow-worker

logs-scheduler:
	$(COMPOSE) logs -f --tail=100 airflow-scheduler

logs-webserver:
	$(COMPOSE) logs -f --tail=100 airflow-webserver

# Airflow Commands

db-migrate:
	$(AIRFLOW_EXEC) db migrate

shell:
	$(COMPOSE) exec airflow-webserver bash

trigger-ingest:
	$(AIRFLOW_EXEC) dags trigger flight_ingest

trigger-transform:
	$(AIRFLOW_EXEC) dags trigger flight_transform

trigger-load:
	$(AIRFLOW_EXEC) dags trigger flight_load

trigger-ml:
	$(AIRFLOW_EXEC) dags trigger flight_ml

dag-list:
	$(AIRFLOW_EXEC) dags list

worker-scale:
	$(COMPOSE) up -d --scale airflow-worker=$(n)
	# usage: make worker-scale n=4


# Code Quality

lint:
	ruff check dags/ plugins/ tests/ scripts/ utils
	black --check dags/ plugins/ tests/ scripts/ utils

format:
	ruff check --fix dags/ plugins/ tests/ scripts/ utils
	black dags/ plugins/ tests/ scripts/ utils

test:
	pytest tests/ -v --tb=short

ci: lint test

stream-run:
	streamlit run dashboard/app.py

flower:
	open http://localhost:5555

ui:
	open http://localhost:8080
