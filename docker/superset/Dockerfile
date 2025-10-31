FROM apache/superset:latest
USER root
RUN /app/.venv/bin/python -m ensurepip --upgrade \
 && /app/.venv/bin/python -m pip install --upgrade pip \
 && /app/.venv/bin/python -m pip install --no-cache-dir "clickhouse-connect==0.9.2"
USER superset