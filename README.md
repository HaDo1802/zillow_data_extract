[![Real Estate ETL Pipeline CI/CD](https://github.com/HaDo1802/zillow_data_extract/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/HaDo1802/zillow_data_extract/actions/workflows/ci-cd.yml)

# Real Estate Data Pipeline

<div align="center">
  <img src="image/cover_image.png" alt="Real Estate Data Pipeline Cover Image" width="85%" />
</div>

This project is a production-oriented ETL pipeline for Zillow real estate listings. It extracts listing data from the Zillow API through RapidAPI, transforms it into a cleaner analytics-ready dataset, and stages both raw and transformed outputs in Supabase Storage for downstream modeling and reporting.

The pipeline is orchestrated with Apache Airflow and is designed around two operational goals:

- reproducible runs for the same logical date
- clear separation between ingestion/staging and downstream transformation

---

## Overview

**Pipeline flow**

```text
Zillow API -> Raw CSV -> Transform -> Supabase Storage -> Downstream Analytics / Modeling
```

<div align="center">

![Architecture](image/architecture_update.png)

</div>

**Current scope**

- Source: Zillow API via [RapidAPI](https://rapidapi.com/apimaker/api/zillow-com1/playground)
- Market focus: Las Vegas, NV
- Orchestration: Apache Airflow
- Storage layer: Supabase Storage
- Output artifacts: raw CSV, transformed CSV, and `_latest.json` manifest

---

## Repository Structure

```text
.
├── dags/
│   └── TaskAPI_etl_dag.py
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── main_etl.py
│   └── email_notifier.py
├── data/
│   ├── raw/
│   └── transformed/
├── tests/
│   ├── test_extract.py
│   ├── test_load.py
│   └── test_transform.py
├── .github/workflows/
│   └── ci-cd.yml
├── requirements-airflow.txt
├── requirements-dev.txt
├── requirements.txt
└── README.md
```

**Core modules**

- `etl/extract.py`: fetches Zillow listings with controlled pagination and rate limiting
- `etl/transform.py`: cleans, standardizes, and enriches the raw dataset
- `etl/load.py`: uploads raw and transformed outputs to Supabase Storage and publishes a manifest
- `etl/main_etl.py`: local entrypoint for running the ETL outside Airflow
- `dags/TaskAPI_etl_dag.py`: Airflow TaskFlow DAG for scheduled execution

---

## Data Pipeline

### Extract

- Pulls listing data from the Zillow API via RapidAPI
- Supports configurable locations and page limits
- Uses deterministic page sampling when `snapshot_date` is provided
- Writes `raw_latest.csv` and a date-stamped raw snapshot under `data/raw/`

### Transform

- Standardizes address and listing fields
- Cleans missing or inconsistent values
- Enriches the dataset with derived features used downstream
- Writes `transformed_latest.csv` and a date-stamped transformed snapshot under `data/transformed/`

### Load

- Uploads raw and transformed CSVs to Supabase Storage
- Publishes `raw/_latest.json` so downstream jobs can resolve the latest logical snapshot
- Stores stable object keys derived from `snapshot_date` and `etl_run_id`

---

## Idempotency And Reproducibility

The pipeline is designed to be retry-safe and reproducible at the artifact level for the same logical date.

**What is deterministic / Idempotent**

- Airflow passes `data_interval_start` downstream as `snapshot_date` and `etl_run_id`
- extraction seeds page sampling from `snapshot_date`, so retries fetch the same page set
- load object keys are derived from the logical date rather than wall-clock retry time
- Supabase uploads use upsert behavior, so retries overwrite the same storage objects
- `_latest.json` gives downstream consumers a stable pointer to the current logical snapshot

Note: The pipeline is designed to be idempotent for run identity and artifact paths, while exact row-level content still depends on the upstream source system.

---

## Design Choices

### Why Airflow

- task orchestration with explicit dependencies
- retry support and scheduling
- operational visibility through the Airflow UI
- cleaner production scheduling than ad hoc cron jobs

### Why Supabase Storage

- keeps ingestion separate from downstream warehouse/modeling logic
- provides durable storage for raw and transformed artifacts
- supports replay and backfill workflows
- lets downstream jobs consume a manifest instead of guessing file paths

### Why UTC

All pipeline timestamps use UTC to avoid timezone drift, daylight saving issues, and ambiguous historical comparisons across systems.

---

## Running The Pipeline

### Local ETL run

```bash
pip install -r requirements.txt
python etl/main_etl.py
```

### Airflow DAG

Main DAG:

```text
dags/TaskAPI_etl_dag.py
```

The DAG is scheduled daily and passes Airflow logical date metadata into extract and load so retries remain logically consistent.

### Airflow with Docker

```bash
docker compose up --build
```

The Airflow image installs dependencies from `requirements-airflow.txt`. Local development and testing use `requirements-dev.txt`.

### Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

---

## Operational Notes

- The pipeline auto-detects local vs Airflow-style execution paths
- Logging is centralized through the project logger
- Email notifications are sent on pipeline success or failure

---

## Downstream Handoff

This project stops at staged delivery. It does not load directly into a warehouse table. Instead, it publishes raw and transformed snapshots to Supabase Storage so downstream systems can consume a stable, versioned handoff layer.

Downstream project:

```text
https://github.com/HaDo1802/zillow_data_transformation
```
