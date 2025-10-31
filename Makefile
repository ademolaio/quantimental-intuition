PY ?= python3.12
VENV := venv
ENV_FILE := .env
C_CH := docker/qi_clickhouse.yml
C_SS := docker/qi_superset.yml

.PHONY: up down bootstrap

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