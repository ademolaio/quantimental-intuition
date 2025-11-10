# Backfilling Market Data

**Scope:** Loads ARCX tickers (SPY, IWM, DIA, XLB…TLT) from yfinance into `market.daily_prices`, then builds weekly/monthly/quarterly aggregates via dbt.

---

## 0) Prereqs (one-time)
- venv: `make venv && source venv/bin/activate`
- deps: `pip install -r requirements.txt`
- ClickHouse/Superset up: `make up` (already configured)
- dbt deps: `cd src/qi/dbt_project && dbt deps`
- Ensure `.env` contains:


```dockerfile
docker exec -it qi-airflow-worker bash -lc "python - <<'PY' from src.qi.pipelines.refresh_fundamentals import run_refresh
# ⬇️ replace with your new tickers
tickers = ['TSLA','NVDA','ORCL']
run_refresh(tickers, sleep_s=0)   # pass a list to load only these
PY"

```

```dockerfile
docker exec -it qi-airflow-worker bash -lc '
  export PATH="$HOME/.local/bin:$PATH"
  export DBT_PROFILES_DIR=/opt/airflow/dags/src/qi/dbt_project
  cd /opt/airflow/dags/src/qi/dbt_project

  dbt --version
  dbt deps
  dbt build --select tag:agg_weekly --fail-fast'
```

```dockerfile
docker exec -it qi-airflow-worker bash -lc '
  export PATH="$HOME/.local/bin:$PATH"
  export DBT_PROFILES_DIR=/opt/airflow/dags/src/qi/dbt_project
  cd /opt/airflow/dags/src/qi/dbt_project
  dbt deps
  dbt build --select tag:agg_monthly   --fail-fast'
```

```dockerfile
docker exec -it qi-airflow-worker bash -lc '
  export PATH="$HOME/.local/bin:$PATH"
  export DBT_PROFILES_DIR=/opt/airflow/dags/src/qi/dbt_project
  cd /opt/airflow/dags/src/qi/dbt_project
  dbt deps
  dbt build --select tag:agg_quarterly --fail-fast
  ```