FROM apache/airflow:2.9.3-python3.12

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Only Airflow providers + your Airflow-safe deps
RUN pip install --no-cache-dir \
    apache-airflow-providers-celery \
    apache-airflow-providers-redis \
    -c "${CONSTRAINT_URL}"

# Install your project deps that are compatible with Airflow constraints
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt \
    -c "${CONSTRAINT_URL}"