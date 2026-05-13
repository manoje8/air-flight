# Makefile
.PHONY: up down shutdown restart logs test lint format shell db-migrate worker-scale

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

logs:
	$(COMPOSE) logs -f --tail=100

logs-worker:
	$(COMPOSE) logs -f airflow-webserver

lint:
	ruff check dags/ plugins/ tests/ scripts/ utils
	black --check dags/ plugins/ tests/ scripts/ utils

format:
	ruff check --fix dags/ plugins/ tests/ scripts/ utils
	black dags/ plugins/ tests/ scripts/ utils

test:
	pytest tests/ -v --tb=short

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

dag-list:
	$(AIRFLOW_EXEC) dags list

worker-scale:
	$(COMPOSE) up -d --scale airflow-worker=$(n)
	# usage: make worker-scale n=4

flower:
	open http://localhost:5555

ci: lint test
