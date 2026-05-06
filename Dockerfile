# Match your compose tag
FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9
# Airflow's official constraints pin compatible versions for providers & deps
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Inject the uv binary from its official image — no pip install of uv needed.
# Using --system in uv pip install below because we want to install into the
# base image's Python env, not a new venv inside the container.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

USER airflow

COPY --chown=airflow:root requirements-airflow.txt /opt/airflow/requirements-airflow.txt

# uv pip install --system = same interface as pip, but Rust-speed resolution.
# --no-cache keeps the layer lean (uv's default is to cache; pip's is not to).
RUN uv pip install --system --no-cache \
    -r /opt/airflow/requirements-airflow.txt \
    --constraint "${CONSTRAINT_URL}"
