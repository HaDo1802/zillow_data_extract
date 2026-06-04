FROM apache/airflow:2.9.2-python3.10

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10
# Airflow's official constraints pin compatible versions for providers & deps
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Inject the uv binary from its official image — no pip install of uv needed.
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
COPY requirements-airflow.txt /requirements-airflow.txt
USER airflow
RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    -r /requirements-airflow.txt
    
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root etl/ /opt/airflow/etl/
COPY --chown=airflow:root utils/ /opt/airflow/utils/
