# Market Pipeline Runbook (ARCX → ClickHouse → dbt)

**Scope:** Loads ARCX tickers (SPY, IWM, DIA, XLB…TLT) from yfinance into `market.daily_prices`, then builds weekly/monthly/quarterly aggregates via dbt.

---

## 0) Prereqs (one-time)
- venv: `make venv && source venv/bin/activate`
- deps: `pip install -r requirements.txt`
- ClickHouse/Superset up: `make up` (already configured)
- dbt deps: `cd src/qi/dbt_project && dbt deps`
- Ensure `.env` contains:
  ```env
  CLICKHOUSE_HOST=localhost
  CLICKHOUSE_PORT=8124
  CLICKHOUSE_USER=qi
  CLICKHOUSE_PASSWORD=mysecurepassword
  CLICKHOUSE_DB=market