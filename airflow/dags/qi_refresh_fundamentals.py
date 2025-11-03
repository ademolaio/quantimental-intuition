# airflow/dags/qi_refresh_fundamentals.py
from __future__ import annotations
import os
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

REQUIRED_ENV = ["CH_HOST","CH_PORT","CLICKHOUSE_USER","CLICKHOUSE_PASSWORD","CLICKHOUSE_DB"]
DBT_PROJECT_DIR = "/opt/airflow/dags/src/qi/dbt_project"
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt deps && dbt run --select fundamentals.quarterly_fundamentals fundamentals.key_ratios"

TZ = pendulum.timezone("Europe/Berlin")
SCHEDULE = "0 6 5 * *"  # 05th at 06:00 Berlin each month

def _validate_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env: {missing}")

def _run_refresh_fundamentals():
    _validate_env()
    from src.qi.pipelines.refresh_fundamentals import run_refresh
    run_refresh()   # defaults to US; pass a list to include intl

default_args = {"owner":"samuel","retries":1,"retry_delay":timedelta(minutes=5)}

with DAG(
    dag_id="refresh_fundamentals_dag",
    description="Refresh yfinance fundamentals into ClickHouse, then build dbt views",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 11, 5, 6, 0, tz=TZ),  # anchor on a real 5th @ 06:00
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["fundamentals","clickhouse","dbt"],
) as dag:
    refresh_task = PythonOperator(task_id="refresh_fundamentals", python_callable=_run_refresh_fundamentals)
    dbt_task     = BashOperator(task_id="dbt_fundamentals", bash_command=DBT_CMD, env=os.environ.copy())
    refresh_task >> dbt_task