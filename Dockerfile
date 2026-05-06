# Match your compose tag
FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9
# Airflow's official constraints pin compatible versions for providers & deps
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Inject the uv binary from its official image — no pip install of uv needed.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# The Airflow base image ends with USER airflow, so RUN commands inherit that user.
# Switch to root explicitly so uv can write to /usr/local/lib/python3.9/site-packages/.
USER root
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN uv pip install --system --no-cache \
    -r /tmp/requirements-airflow.txt \
    --constraint "${CONSTRAINT_URL}"

USER airflow
