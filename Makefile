# ====== Python / local ======
PY ?= python3.12
VENV := venv
ENV_FILE := .env
PY_SRC := src
DBT_DIR := src/qi/dbt_project

# ====== Docker compose files ======
C_CH := docker/qi_clickhouse.yml
C_SS := docker/qi_superset.yml
C_AF := docker/qi_airflow.yml

# --- Common env bootstrap (venv + PYTHONPATH + .env) -------------------------
define ENV_EXPORT
. $(VENV)/bin/activate && \
export PYTHONPATH="$$(pwd)/$(PY_SRC):$$(pwd)" && \
set -a; [ -f $(ENV_FILE) ] && . $(ENV_FILE); set +a
endef

.PHONY: up down bootstrap_superset venv install freeze shell \
        dbt-debug dbt-deps pip-upgrade \
        backfill_us backfill_intl refresh_us refresh_intl dbt_agg \
        weekly_close morning_catchup test_market counts tail_spy \
        airflow-up airflow-down airflow-logs airflow-web \
        airflow-trigger-us airflow-trigger-intl \
        airflow-runs-us airflow-runs-intl \
        airflow-task-logs-us airflow-task-logs-intl \
        print-env validate-exchanges funds-backfill funds-refresh \
        env-shell dbt-provision-fundamentals refresh_fundamentals \
        backfill_fundamentals \
        container-dbt-profile container-dbt-deps container-dbt-run container-dbt-test container-dbt-build

# --- ClickHouse + Superset ----------------------------------------------------
up:
	@docker network create qi_net >/dev/null 2>&1 || true
	docker compose --env-file $(ENV_FILE) -f $(C_CH) up -d
	docker compose -f $(C_SS) up -d --build

down:
	docker compose -f $(C_SS) down
	docker compose -f $(C_CH) down

bootstrap_superset:
	bash ./scripts/bootstrap_superset.sh

# --- Python venv --------------------------------------------------------------
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

shell:
	bash -lc 'source $(VENV)/bin/activate && exec bash -i'

pip-upgrade:
	@. $(VENV)/bin/activate && pip install --upgrade pip setuptools wheel

# --- dbt (LOCAL venv) ---------------------------------------------------------
dbt-debug:
	@$(ENV_EXPORT) && cd $(DBT_DIR) && dbt debug

dbt-deps:
	@$(ENV_EXPORT) && cd $(DBT_DIR) && dbt deps

dbt_agg:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

test_market:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt test --select market

# --- Pipelines (US / INTL) via LOCAL python ----------------------------------
backfill_us:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt deps && dbt run-operation provision_fundamentals_raw && \
	$(PY) -m qi.pipelines.backfill_us && \
	$(PY) -c "from data.tickers.UNITED_STATES import ALL_US; \
	  from src.qi.pipelines.refresh_fundamentals import run_refresh; \
	  run_refresh(ALL_US, sleep_s=0)" && \
	dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

refresh_us:
	@$(ENV_EXPORT) && \
	$(PY) -m qi.pipelines.refresh_us && \
	$(PY) -c "from data.tickers.UNITED_STATES import ALL_US; \
	  from src.qi.pipelines.refresh_fundamentals import run_refresh; \
	  run_refresh(ALL_US, sleep_s=0)" && \
	cd $(DBT_DIR) && dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

backfill_intl:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt deps && dbt run-operation provision_fundamentals_raw && \
	$(PY) -m qi.pipelines.backfill_intl && \
	$(PY) -c "from data.tickers.INTERNATIONAL import ALL_INTL; \
	  from src.qi.pipelines.refresh_fundamentals import run_refresh; \
	  run_refresh(ALL_INTL, sleep_s=0)" && \
	dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

refresh_intl:
	@$(ENV_EXPORT) && \
	$(PY) -m qi.pipelines.refresh_intl && \
	$(PY) -c "from data.tickers.INTERNATIONAL import ALL_INTL; \
	  from src.qi.pipelines.refresh_fundamentals import run_refresh; \
	  run_refresh(ALL_INTL, sleep_s=0)" && \
	cd $(DBT_DIR) && dbt run --select market.weekly_prices market.monthly_prices market.quarterly_prices

backfill_fundamentals:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt deps && dbt run-operation provision_fundamentals_raw && \
	$(PY) -c "from src.qi.pipelines.refresh_fundamentals import run_refresh; run_refresh(None, sleep_s=0)"

refresh_fundamentals:
	@$(ENV_EXPORT) && \
	cd $(DBT_DIR) && dbt deps && dbt run-operation provision_fundamentals_raw && \
	$(PY) -c "from src.qi.pipelines.refresh_fundamentals import run_refresh; run_refresh(None, sleep_s=0)"

weekly_close: refresh_us refresh_intl
morning_catchup: refresh_us refresh_intl

funds-refresh:
	@$(ENV_EXPORT) && \
	$(PY) -m qi.pipelines.refresh_fundamentals && \
	cd $(DBT_DIR) && dbt run --select fundamentals.quarterly_fundamentals fundamentals.key_ratios

funds-backfill:
	@$(ENV_EXPORT) && \
	$(PY) -m qi.pipelines.refresh_fundamentals && \
	cd $(DBT_DIR) && dbt run --select fundamentals.quarterly_fundamentals fundamentals.key_ratios

dbt-provision-fundamentals:
	@$(ENV_EXPORT) && cd $(DBT_DIR) && dbt deps && dbt run-operation provision_fundamentals_raw

# --- Quick ClickHouse checks (optional; requires clickhouse-client) ----------
counts:
	@echo "Counts (if clickhouse-client installed):"
	@echo "SELECT 'daily', count() FROM market.daily_prices;" | clickhouse-client -mn || true
	@echo "SELECT 'weekly', count() FROM market.weekly_prices;" | clickhouse-client -mn || true
	@echo "SELECT 'monthly', count() FROM market.monthly_prices;" | clickhouse-client -mn || true
	@echo "SELECT 'quarterly', count() FROM market.quarterly_prices;" | clickhouse-client -mn || true

tail_spy:
	@echo "SELECT * FROM market.daily_prices WHERE ticker='SPY' ORDER BY date DESC LIMIT 10;" | clickhouse-client -mn || true

# --- Airflow (CeleryExecutor stack) ------------------------------------------
airflow-up:
	@docker network create qi_net >/dev/null 2>&1 || true
	docker compose --env-file $(ENV_FILE) -f $(C_AF) up -d --build

airflow-down:
	docker compose -f $(C_AF) down

airflow-logs:
	@docker compose --env-file $(ENV_FILE) -f $(C_AF) logs -f --tail=100

airflow-web:
	@echo "Open http://localhost:8081  (user: admin / pass: admin)"

# Trigger DAGs by id
airflow-trigger-us:
	@docker exec -it qi-airflow-scheduler bash -lc "airflow dags trigger refresh_us_dag"

airflow-trigger-intl:
	@docker exec -it qi-airflow-scheduler bash -lc "airflow dags trigger refresh_intl_dag"

# List recent runs
airflow-runs-us:
	@docker exec -it qi-airflow-scheduler bash -lc "airflow dags list-runs -d refresh_us_dag --no-backfill -o table | tail -n 20"

airflow-runs-intl:
	@docker exec -it qi-airflow-scheduler bash -lc "airflow dags list-runs -d refresh_intl_dag --no-backfill -o table | tail -n 20"

# Fetch task logs for a given RUN_ID and TASK_ID
# make airflow-task-logs-us RUN_ID=manual__2025-11-01T10:00:00+00:00 TASK_ID=refresh_us_pipeline
airflow-task-logs-us:
	@docker exec -it qi-airflow-scheduler bash -lc "airflow tasks logs refresh_us_dag $(TASK_ID) $(RUN_ID)"

airflow-task-logs-intl:
	@docker exec -it qi-airflow-scheduler bash -lc "airflow tasks logs refresh_intl_dag $(TASK_ID) $(RUN_ID)"

# --- dbt INSIDE AIRFLOW CONTAINER --------------------------------------------
# (useful when you want dbt to run in the same environment as your dags)
DBT_CONTAINER ?= qi-airflow-web
PROFILES_DIR  ?= /opt/airflow/dags/src/qi/dbt_project

# ClickHouse connection for container dbt (override via .env if you like)
CLICKHOUSE_HOST ?= qi-clickhouse
CLICKHOUSE_PORT ?= 8123
CLICKHOUSE_USER ?= qi
CLICKHOUSE_PASSWORD ?= mysecurepassword
CLICKHOUSE_DB ?= default

# Optional selection (override: make container-dbt-run SELECT="fundamentals.key_ratios_*")
SELECT ?=

container-dbt-profile:
	@docker exec -it $(DBT_CONTAINER) bash -lc 'cat > $(PROFILES_DIR)/profiles.yml <<EOF
qi_dbt_project:
  target: dev
  outputs:
    dev:
      type: clickhouse
      threads: 1
      host: "{{ env_var(\"CLICKHOUSE_HOST\", \"$(CLICKHOUSE_HOST)\") }}"
      port: "{{ env_var(\"CLICKHOUSE_PORT\", \"$(CLICKHOUSE_PORT)\") | int }}"
      user: "{{ env_var(\"CLICKHOUSE_USER\", \"$(CLICKHOUSE_USER)\") }}"
      password: "{{ env_var(\"CLICKHOUSE_PASSWORD\", \"$(CLICKHOUSE_PASSWORD)\") }}"
      database: "{{ env_var(\"CLICKHOUSE_DB\", \"$(CLICKHOUSE_DB)\") }}"
      schema: "{{ env_var(\"CLICKHOUSE_DB\", \"$(CLICKHOUSE_DB)\") }}"
      secure: false
EOF'

container-dbt-deps: container-dbt-profile
	@docker exec \
		-e DBT_PROFILES_DIR=$(PROFILES_DIR) \
		-e CLICKHOUSE_HOST=$(CLICKHOUSE_HOST) \
		-e CLICKHOUSE_PORT=$(CLICKHOUSE_PORT) \
		-e CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
		-e CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD) \
		-e CLICKHOUSE_DB=$(CLICKHOUSE_DB) \
		-it $(DBT_CONTAINER) bash -lc 'cd $(PROFILES_DIR) && dbt deps'

container-dbt-run: container-dbt-deps
	@docker exec \
		-e DBT_PROFILES_DIR=$(PROFILES_DIR) \
		-e CLICKHOUSE_HOST=$(CLICKHOUSE_HOST) \
		-e CLICKHOUSE_PORT=$(CLICKHOUSE_PORT) \
		-e CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
		-e CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD) \
		-e CLICKHOUSE_DB=$(CLICKHOUSE_DB) \
		-it $(DBT_CONTAINER) bash -lc 'cd $(PROFILES_DIR) && dbt run $(if $(SELECT),--select "$(SELECT)",)'

container-dbt-test: container-dbt-deps
	@docker exec \
		-e DBT_PROFILES_DIR=$(PROFILES_DIR) \
		-e CLICKHOUSE_HOST=$(CLICKHOUSE_HOST) \
		-e CLICKHOUSE_PORT=$(CLICKHOUSE_PORT) \
		-e CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
		-e CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD) \
		-e CLICKHOUSE_DB=$(CLICKHOUSE_DB) \
		-it $(DBT_CONTAINER) bash -lc 'cd $(PROFILES_DIR) && dbt test $(if $(SELECT),--select "$(SELECT)",)'

container-dbt-build: container-dbt-deps
	@docker exec \
		-e DBT_PROFILES_DIR=$(PROFILES_DIR) \
		-e CLICKHOUSE_HOST=$(CLICKHOUSE_HOST) \
		-e CLICKHOUSE_PORT=$(CLICKHOUSE_PORT) \
		-e CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
		-e CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD) \
		-e CLICKHOUSE_DB=$(CLICKHOUSE_DB) \
		-it $(DBT_CONTAINER) bash -lc 'cd $(PROFILES_DIR) && dbt build $(if $(SELECT),--select "$(SELECT)",)'

# --- Misc ---------------------------------------------------------------------
print-env:
	@$(ENV_EXPORT) && env | grep -E '^(CH_|CLICKHOUSE_|AIRFLOW_|PYTHONPATH|DBT_|PY=)'

env-shell:
	@set -a && [ -f $(ENV_FILE) ] && . $(ENV_FILE) && set +a && bash -i

validate-exchanges:
	@$(ENV_EXPORT) && $(PY) tools/validate_exchanges.py