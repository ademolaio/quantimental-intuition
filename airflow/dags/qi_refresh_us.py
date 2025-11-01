# # airflow/dags/qi_refresh_us.py
# from __future__ import annotations
#
# import os
# import subprocess
# from datetime import timedelta
# import pendulum
#
# from data.tickers.UNITED_STATES import ALL_US as TICKERS
# from src.qi.pipelines.yfinance_loader import refresh_tickers
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
#
#
# # ---- Config -----------------------------------------------------------------
#
# # Required env vars your pipeline expects (loaded via docker compose --env-file)
# REQUIRED_ENV = [
#     "CH_HOST",
#     "CH_PORT",
#     "CLICKHOUSE_USER",
#     "CLICKHOUSE_PASSWORD",
#     "CLICKHOUSE_DB",
# ]
#
# # Path to your dbt project inside the Airflow container.
# # Make sure you map your host path to this container path in docker compose.
# DBT_PROJECT_DIR = "/opt/airflow/dags/src/qi/dbt_project"
#
# # dbt command to refresh the three aggregate tables after the loader runs
# DBT_AGG_CMD = (
#     f"cd {DBT_PROJECT_DIR} && "
#     "dbt deps && "
#     "dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices"
# )
#
# # Schedule: Fridays 20:30 Europe/Berlin
# TZ = pendulum.timezone("Europe/Berlin")
# SCHEDULE = "30 20 * * FRI"  # crontab expression evaluated in Europe/Berlin
#
#
# # ---- Small helpers -----------------------------------------------------------
#
# def _validate_env() -> None:
#     missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
#     if missing:
#         raise RuntimeError(f"Missing required environment variables: {missing}")
#
#
# def _run_refresh_us() -> None:
#     """
#     Call your Python pipeline function that:
#       - pulls latest YF data for ARCX list with a 21-day overlap
#       - upserts into market.daily_prices (and sets ingested_at/batch_id)
#     The function reads ClickHouse connection details from env.
#     """
#     _validate_env()
#
#     # Import here so the DAG file loads even if code isn’t available during parse.
#     from src.qi.pipelines.refresh_us import run_refresh
#
#     # Run the refresh (no args: your pipeline reads env vars internally)
#     run_refresh()
#
#
# # ---- DAG --------------------------------------------------------------------
#
# default_args = {
#     "owner": "samuel",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }
#
# with DAG(
#     dag_id="refresh_us_dag",
#     description="Weekly US refresh (YF → ClickHouse) + dbt aggregates",
#     default_args=default_args,
#     start_date=pendulum.datetime(2025, 10, 31, 0, 0, tz=TZ),
#     schedule=SCHEDULE,
#     catchup=False,
#     max_active_runs=1,
#     tags=["us", "market", "clickhouse", "dbt"],
# ) as dag:
#
#     refresh_us_task = PythonOperator(
#         task_id="refresh_us_pipeline",
#         python_callable=_run_refresh_us,
#     )
#
#     # Optional but recommended: rebuild aggregates right after refresh
#     dbt_aggregates_task = BashOperator(
#         task_id="dbt_aggregates",
#         bash_command=DBT_AGG_CMD,
#         env=os.environ.copy(),  # inherit .env so dbt/profiles see CH creds if needed
#     )
#
#     refresh_us_task >> dbt_aggregates_task

# airflow/dags/qi_refresh_us.py
from __future__ import annotations

import os
import sys
from datetime import timedelta
import pendulum

# Make sure Airflow can import from repo root (…/airflow/dags -> add ../..)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from data.tickers.UNITED_STATES import ALL_US as TICKERS
from src.qi.pipelines.yfinance_loader import refresh_tickers

# ---- Config -----------------------------------------------------------------

REQUIRED_ENV = [
    "CH_HOST",
    "CH_PORT",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_DB",
]

DBT_PROJECT_DIR = "/opt/airflow/dags/src/qi/dbt_project"
DBT_AGG_CMD = (
    f"cd {DBT_PROJECT_DIR} && "
    "dbt deps && "
    "dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices"
)

# Fridays 20:30 Europe/Berlin
TZ = pendulum.timezone("Europe/Berlin")
SCHEDULE = "30 20 * * FRI"

# ---- Helpers ----------------------------------------------------------------

def _validate_env() -> None:
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")

def _run_refresh_us() -> None:
    """
    Pull latest Yahoo Finance data for the US ticker set (NYSE/NASDAQ/ARCX/CBOE)
    with a 21-day overlap and upsert into market.daily_prices.
    """
    _validate_env()
    refresh_tickers(TICKERS, lookback_days=21)

# ---- DAG --------------------------------------------------------------------

default_args = {
    "owner": "samuel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="refresh_us_dag",
    description="Weekly US refresh (YF → ClickHouse) + dbt aggregates",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 10, 31, 0, 0, tz=TZ),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["us", "market", "clickhouse", "dbt"],
) as dag:

    refresh_us_task = PythonOperator(
        task_id="refresh_us_pipeline",
        python_callable=_run_refresh_us,
    )

    dbt_aggregates_task = BashOperator(
        task_id="dbt_aggregates",
        bash_command=DBT_AGG_CMD,
        env=os.environ.copy(),
    )

    refresh_us_task >> dbt_aggregates_task