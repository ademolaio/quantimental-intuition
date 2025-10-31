PY ?= python3.12
VENV := venv
ENV_FILE := .env
C_CH := docker/qi_clickhouse.yml
C_SS := docker/qi_superset.yml
PY_SRC := src
DBT_DIR := src/qi/dbt_project

# Common env bootstrap (venv + PYTHONPATH + .env)
define ENV_EXPORT
. $(VENV)/bin/activate && \
export PYTHONPATH="$$(pwd)/$(PY_SRC)" && \
set -a && [ -f $(ENV_FILE) ] && . $(ENV_FILE) || true && set +a
endef

.PHONY: up down bootstrap venv install freeze shell dbt-debug backfill_arcx refresh_arcx dbt_agg weekly_close morning_catchup test_market counts tail_spy

# create external network if missing, start both stacks
up:
	@docker network create qi_net >/dev/null 2>&1 || true
	docker compose --env-file $(ENV_FILE) -f $(C_CH) up -d
	docker compose -f $(C_SS) up -d --build

# stop both
down:
	docker compose -f $(C_SS) down
	docker compose -f $(C_CH) down

# fix the 500 by creating admin + init (same as your commands)
bootstrap:
	bash ./scripts/bootstrap_superset.sh

# -------- python venv (you'll activate with: source venv/bin/activate) ------
venv:
	$(PY) -m venv $(VENV)
	@echo "Now run: source $(VENV)/bin/activate"

install: venv
	@if [ -f requirements.txt ]; then \
		. $(VENV)/bin/activate && pip install -r requirements.txt; \
	else \
		echo "No requirements.txt found"; \
	fi

freeze:
	. $(VENV)/bin/activate && pip freeze > requirements.lock

# optional: open an interactive shell with venv pre-activated
shell:
	bash -lc 'source $(VENV)/bin/activate && exec bash -i'


dbt-debug:
	@$(ENV_EXPORT) && dbt debug

dbt-deps:
	@$(ENV_EXPORT) && cd $(DBT_DIR) && dbt deps

pip-upgrade:
	@. $(VENV)/bin/activate && pip install --upgrade pip setuptools wheel

# One-time historical load for ARCX tickers (1999-12-31 → today)
backfill_arcx:
	@$(ENV_EXPORT) && \
	$(PY) -m qi.pipelines.backfill_arcx && \
	cd $(DBT_DIR) && dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

# Weekly refresh (reloads the last ~3 weeks to capture final closes/dividends/splits)
refresh_arcx:
	@$(ENV_EXPORT) && \
	$(PY) -m qi.pipelines.refresh_arcx && \
	cd $(DBT_DIR) && dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

# Just rebuild the aggregates (no new raw loads)
dbt_agg:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

# End-of-day: run after US cash close (or next morning)
weekly_close: refresh_arcx

# Morning catch-up (if you skipped last night)
morning_catchup: refresh_arcx

# Run the dbt tests for market models
test_market:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt test --select market

# Quick object counts (requires clickhouse-client on PATH; optional)
counts:
	@echo "Counts (if clickhouse-client installed):"
	@echo "SELECT 'daily', count() FROM market.daily_prices;" | clickhouse-client -mn || true
	@echo "SELECT 'weekly', count() FROM market.weekly_prices;" | clickhouse-client -mn || true
	@echo "SELECT 'monthly', count() FROM market.monthly_prices;" | clickhouse-client -mn || true
	@echo "SELECT 'quarterly', count() FROM market.quarterly_prices;" | clickhouse-client -mn || true

# Show last few SPY rows (requires clickhouse-client; optional)
tail_spy:
	@echo "SELECT * FROM market.daily_prices WHERE ticker='SPY' ORDER BY date DESC LIMIT 10;" | clickhouse-client -mn || true