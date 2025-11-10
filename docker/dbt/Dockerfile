FROM python:3.12-slim

# system deps (adjust if you need others)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates gcc build-essential git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ../requirements.dbt.txt /tmp/requirements.dbt.txt

# No Airflow constraints here; let dbt pull protobuf 6.x
RUN pip install --no-cache-dir -r /tmp/requirements.dbt.txt

# optional: place your dbt project in the image or mount it at runtime
# COPY ./dags/src/qi/dbt_project /opt/dbt