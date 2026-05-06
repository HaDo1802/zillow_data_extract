.PHONY: help install install-dev sync test lint format clean docker-up docker-down docker-restart logs airflow-init run-etl all

help:
	@echo "Available commands:"
	@echo "  make install          - Install production dependencies (uv sync)"
	@echo "  make install-dev      - Install all deps including dev tools (uv sync --group dev)"
	@echo "  make sync             - Sync environment to lockfile exactly (uv sync)"
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

# uv sync reads pyproject.toml + uv.lock and installs exactly what's pinned.
# First run creates uv.lock; subsequent runs are near-instant from cache.
install:
	uv sync

install-dev:
	uv sync --group dev

# Alias: same as install-dev but more explicit about intent
sync:
	uv sync --group dev

test:
	uv run pytest

lint:
	uv run flake8 etl/ tests/ dags/ --max-line-length=127 --extend-ignore=E402,

format:
	uv run black etl/ tests/ dags/ --line-length 127
	uv run isort etl/ tests/ dags/ --profile black

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
	uv run python etl/main_etl.py

all: install-dev format lint test
