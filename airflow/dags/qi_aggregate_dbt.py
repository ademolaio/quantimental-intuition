# airflow/dags/qi_aggregate_dbt.py
from __future__ import annotations
import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

TZ = pendulum.timezone("Europe/Berlin")
DBT_DIR = "/opt/airflow/dags/src/qi/dbt_project"

def dbt_cmd(tag: str) -> str:
    return f"set -euo pipefail; cd {DBT_DIR}; dbt deps; dbt build --select tag:{tag} --fail-fast"

default_args = {"owner": "samuel", "retries": 1, "retry_delay": timedelta(minutes=5)}
ENV = {**os.environ, "DBT_PROFILES_DIR": DBT_DIR}

# ---------- WEEKLY (Saturday 07:00 Berlin) ----------
with DAG(
    dag_id="build_aggregates_weekly",
    description="Build weekly aggregates from daily_prices",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 11, 8, 7, 0, tz=TZ),  # a Saturday
    schedule="0 7 * * 6",   # Sat 07:00
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "aggregates", "weekly"],
) as dag_weekly:
    BashOperator(
        task_id="dbt_run_weekly",
        bash_command=dbt_cmd("agg_weekly"),
        env=ENV,
    )

# ---------- MONTHLY (only on last calendar day, 22:30 Berlin) ----------
def is_month_end(ds: str) -> bool:
    d = pendulum.parse(ds).in_timezone(TZ)
    return d.add(days=1).day == 1

with DAG(
    dag_id="build_aggregates_monthly",
    description="Build monthly aggregates from daily_prices",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 11, 1, 22, 30, tz=TZ),
    schedule="30 22 * * *",   # every day 22:30 → gated to month-end
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "aggregates", "monthly"],
) as dag_monthly:
    gate_month_end = ShortCircuitOperator(
        task_id="only_if_month_end",
        python_callable=is_month_end,
        op_kwargs={"ds": "{{ ds }}"},
    )
    dbt_monthly = BashOperator(
        task_id="dbt_run_monthly",
        bash_command=dbt_cmd("agg_monthly"),
        env=ENV,
    )
    gate_month_end >> dbt_monthly

# ---------- QUARTERLY (only on last calendar day of quarter, 22:30 Berlin) ----------
def is_quarter_end(ds: str) -> bool:
    d = pendulum.parse(ds).in_timezone(TZ)
    return (d.month in (3, 6, 9, 12)) and (d.add(days=1).month != d.month)

with DAG(
    dag_id="build_aggregates_quarterly",
    description="Build quarterly aggregates from daily_prices",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 10, 1, 22, 30, tz=TZ),
    schedule="30 22 * * *",   # every day 22:30 → gated to quarter-end
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "aggregates", "quarterly"],
) as dag_quarterly:
    gate_quarter_end = ShortCircuitOperator(
        task_id="only_if_quarter_end",
        python_callable=is_quarter_end,
        op_kwargs={"ds": "{{ ds }}"},
    )
    dbt_quarterly = BashOperator(
        task_id="dbt_run_quarterly",
        bash_command=dbt_cmd("agg_quarterly"),
        env=ENV,
    )
    gate_quarter_end >> dbt_quarterly