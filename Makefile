.PHONY: help install test lint format clean docker-up docker-down docker-restart logs airflow-init run-etl

help:
	@echo "Available commands:"
	@echo "  make install          - Install Python dependencies"
	@echo "  make test             - Run tests"
	@echo "  make lint             - Run linting checks"
	@echo "  make format           - Format code with black and isort"
	@echo "  make clean            - Remove generated files"
	@echo "  make docker-up        - Start Docker containers"
	@echo "  make docker-down      - Stop Docker containers"
	@echo "  make docker-restart   - Restart Docker containers"
	@echo "  make logs             - View Docker logs"
	@echo "  make airflow-init     - Initialize Airflow database"
	@echo "  make run-etl          - Run ETL pipeline locally"

install:
	pip install --upgrade pip
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v --cov=etl --cov-report=term-missing

lint:
	flake8 etl/ tests/ dags/ --max-line-length=127 --extend-ignore=E402,

format:
	black etl/ tests/ dags/ --line-length 127
#	isort etl/ tests/ dags/ --profile black

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-restart:
	docker compose down
	docker compose up --build -d

logs:
	docker compose logs -f

airflow-init:
	docker compose run --rm airflow-init

run-etl:
	python etl/main_etl.py

all: install format lint test
