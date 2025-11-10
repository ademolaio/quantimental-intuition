# Backfilling Market Data

**Scope:** Loads ARCX tickers (SPY, IWM, DIA, XLB…TLT) from yfinance into `market.daily_prices`, then builds weekly/monthly/quarterly aggregates via dbt.

---

## 0) Prereqs (one-time)
- venv: `make venv && source venv/bin/activate`
- deps: `pip install -r requirements.txt`
- ClickHouse/Superset up: `make up` (already configured)
- dbt deps: `cd src/qi/dbt_project && dbt deps`
- Ensure `.env` contains:
```
docker exec -it qi-airflow-worker bash -lc "python - <<'PY' from src.qi.pipelines.refresh_fundamentals import run_refresh
# ⬇️ replace with your new tickers
tickers = ['TSLA','NVDA','ORCL']
run_refresh(tickers, sleep_s=0)   # pass a list to load only these
PY"

```