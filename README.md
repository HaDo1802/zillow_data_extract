[![Real Estate ETL Pipeline CI/CD](https://github.com/HaDo1802/zillow_data_extract/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/HaDo1802/zillow_data_extract/actions/workflows/ci-cd.yml)

# Real Estate Data Pipeline

![Real Estate Data Pipeline Cover Image](image/cover_image.png)
This project implements a Extract-Transform-Load (ETL) pipeline for real estate listings data, designed to process and analyze real-time property listings. The pipeline extracts property data from Zillow API, leveraging Apache Airflow and Docker to automate data collection and storage for downstream analytics and machine learning applications.

---

## 🗂 Project Structure

```
.
├── dags/                      # Airflow DAG definitions
│   └── real_estate_etl_dag.py # Main pipeline workflow
├── etl/                       # ETL modules
│   ├── extract.py            # API data extraction
│   ├── transform.py          # Data cleaning & enrichment
│   ├── load.py               # S3 staging (raw + transformed)
│   ├── main_etl.py           # Pipeline orchestrator
│   └── email_notifier.py     # Script for customized email
├── data/                      # Data storage (gitignored)
│   ├── raw/                  # Extracted data snapshots
│   └── transformed/          # Cleaned data ready for loading
├── docker/
│   ├── docker-compose.yaml   # Service orchestration
│   └── Dockerfile            # Custom Airflow image
├── .env
├── requirements.txt          # Packages dependencies
└── README.md
```

---

## ⚙️ Technology Stack

- **Data Source**: Zillow API (RapidAPI)
- **Data Processing**: Python 3.9+ with Pandas, NumPy
- **Workflow Orchestration**: Apache Airflow
- **Storage**: S3 (staging for downstream processing)
- **Containerization**: Docker
- **Email Notifications**: SMTP (Gmail)

---

## 🧱 Data Architecture

```markdown
Zillow API --> Raw CSV --> S3 --> (Downstream Postgres + Transformations) --> Email Notification
```

![Architecture](image/architecture_update.png)

### 1. Data Source

The project processes real estate listings from the **Zillow API** via [RapidAPI](https://rapidapi.com/apimaker/api/zillow-com1/playground), focusing on the Las Vegas market with plans to expand to additional locations. Current extraction targets multiple neighborhoods including **Summerlin, Henderson, Downtown Las Vegas, and surrounding areas**.

### 2. Data Processing Pipeline

#### **Data Extraction**

- Automatically fetch property listings from Zillow API via RapidAPI
- Extract comprehensive property details including property idetification, prices, location, and other data
- Store raw data with timestamps in `raw_data_YYYYMMDD.csv`, allowing audit tracing for daily run
- Features intelligent pagination and rate limiting for API compliance
- Multi-location support with configurable location list

#### **Data Transformation & Cleaning**

- Parse and standardize address components (street, city, state, zip)
- Normalize lot area measurements (acres to sqft conversion)
- Calculate derived fields (listing dates, district classification)
- Extract listing features (FSBA status, open house indicators)
- Handle missing values and validate data quality
- Generate cleaned dataset with consistent schema stored in `transformed_YYYYMMDD.csv`

#### **S3 Staging**

- Upload raw and transformed snapshots to S3 for downstream ingestion
- Preserve daily snapshots for auditing and backfills
- Decouple this ETL from downstream Postgres loading and transformations
- S3 object keys include `snapshot_date` + `etl_run_id` to distinguish multiple same-day runs for audit and traceback

### 3. Data Quality Framework

- Essential field validation (property ID, etl_run_id) use for duplicate detection and removal
- Pipeline monitoring via email notifications
- S3 upload validation (file presence and successful transfer)

---

## 🚀 Project Components

### 📊 Airflow DAGs

Located in `dags/`:

- **Pipeline orchestration** for automated data collection every day at 6 AM
- **Task scheduling** with dependency management
- **Retry logic** for fault-tolerant execution
- **Email notifications** on success/failure
- **Execution tracking** via Airflow web UI (port 8080)

### 🛠 ETL Modules

Located in `etl/`:

- **extract.py**: Multi-location API scraper with pagination
- **transform.py**: Data cleaning, feature engineering, validation
- **load.py**: S3 staging for raw + transformed files
- **main_etl.py**: Standalone ETL runner for manual execution
- **email_notifier.py**: SMTP notification service with HTML templates

### 🧭 Why UTC Datetime Standard

All timestamps in this pipeline are generated in UTC. This is a best practice for distributed data systems because it:

- Prevents timezone drift and daylight savings issues
- Keeps scheduling consistent across Airflow, S3, and downstream systems
- Makes historical comparisons and backfills reliable

### 🧩 Downstream Postgres + Transformations

This project intentionally does not load into Postgres. Instead, it stages raw and transformed snapshots in S3 and hands off loading and additional transformations to a downstream project where the final modeling happens. This keeps the pipeline modular and avoids duplicate transformations or conflicting schemas across projects.

Downstream project:

```
https://github.com/HaDo1802/zillow_data_transformation
```

---

## 📦 Key Dependencies

- **pandas==1.5.0** - Data processing and transformation
- **numpy==1.24.0** - Numerical operations
- **requests==2.28.0** - HTTP library for API calls
- **python-dotenv==1.0.0** - Environment variable management
- **apache-airflow==2.9.2** - Workflow orchestration

---

## 🚀 Key Features

### Comprehensive Data Extraction

- **Multi-location support**: Configurable list of target locations
- **Rate limiting**: API-compliant request throttling (0.2s between calls)
- **Pagination handling**: Automatic traversal of result pages
- **Error recovery**: Robust exception handling with retries

### Snapshot-Based Storage

- **Historical tracking**: Full audit trail of all property changes
- **Price history**: Track listing price changes over time
- **Point-in-time queries**: Analyze market state at any date
- **Zero data loss**: Append-only architecture prevents overwrites

### Production-Ready Operations

- **Automated scheduling**: Runs every 10 minutes via Airflow
- **Email notifications**: Success/failure alerts with execution details
- **Comprehensive logging**: Multi-level logs for debugging, documented inside <a href="file:///Users/hado/Desktop/Career/Coding/Data%20Engineer/Project/real_estate_project/etl_log/log.txt">etl_log/log.txt</a>
- **Environment flexibility**: Auto-detects Docker vs local execution
- **Containerized deployment**: Docker Compose for consistent environments
- **Centralize Configuration**: Leveraging modular logger and .env variables configuration, making it easier to scale and ensure safety

---

## 🎯 Design Decisions

### Why Snapshot-Based Storage?

Traditional upsert strategies overwrite historical data, losing valuable time-series information. This pipeline uses **append-only history** with a **current view** to enable:

- Price trend analysis over time
- Market velocity metrics (average days to sale)
- Point-in-time market snapshots
- Complete audit trail for compliance

### Why Airflow Over Cron?

- **Visual monitoring**: Web UI for pipeline status and logs
- **Dependency management**: Task execution order enforcement
- **Retry logic**: Automatic failure recovery with backoff
- **Scalability**: Easy migration to distributed execution
- **Extensibility**: Rich ecosystem of providers and operators

### Why S3 Staging?

- **Decoupling**: Keeps extraction and transformation independent from downstream loading
- **Durability**: Reliable storage for snapshots and reprocessing
- **Backfills**: Easy to replay historical runs
- **Interoperability**: Works across different downstream systems

---
