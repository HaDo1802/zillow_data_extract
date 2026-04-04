# Match your compose tag
FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9
# Airflow’s official constraints pin compatible versions for providers & deps
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow

# Preserve readability for the non-root airflow user during build.
COPY --chown=airflow:root requirements-airflow.txt /opt/airflow/requirements-airflow.txt
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Use Airflow constraints so custom packages stay compatible with the base image.
RUN pip install --no-cache-dir -r /opt/airflow/requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
