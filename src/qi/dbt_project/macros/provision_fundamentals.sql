{% macro provision_fundamentals_raw() %}
  {# Build each DDL as a separate statement and execute with run_query #}
  {% set stmts = [
    "CREATE DATABASE IF NOT EXISTS fundamentals",

    "CREATE TABLE IF NOT EXISTS fundamentals.income_statement ( \
      ticker String, \
      fiscal_date Date, \
      period LowCardinality(String), \
      metric LowCardinality(String), \
      value Float64, \
      currency LowCardinality(String), \
      source LowCardinality(String) DEFAULT 'yfinance', \
      loaded_at DateTime64(3) DEFAULT now64(3), \
      _batch_id UUID DEFAULT generateUUIDv4() \
    ) \
    ENGINE = ReplacingMergeTree(loaded_at) \
    PARTITION BY toYYYYMM(fiscal_date) \
    ORDER BY (ticker, fiscal_date, metric)",

    "CREATE TABLE IF NOT EXISTS fundamentals.balance_sheet ( \
      ticker String, \
      fiscal_date Date, \
      period LowCardinality(String), \
      metric LowCardinality(String), \
      value Float64, \
      currency LowCardinality(String), \
      source LowCardinality(String) DEFAULT 'yfinance', \
      loaded_at DateTime64(3) DEFAULT now64(3), \
      _batch_id UUID DEFAULT generateUUIDv4() \
    ) \
    ENGINE = ReplacingMergeTree(loaded_at) \
    PARTITION BY toYYYYMM(fiscal_date) \
    ORDER BY (ticker, fiscal_date, metric)",

    "CREATE TABLE IF NOT EXISTS fundamentals.cashflow_statement ( \
      ticker String, \
      fiscal_date Date, \
      period LowCardinality(String), \
      metric LowCardinality(String), \
      value Float64, \
      currency LowCardinality(String), \
      source LowCardinality(String) DEFAULT 'yfinance', \
      loaded_at DateTime64(3) DEFAULT now64(3), \
      _batch_id UUID DEFAULT generateUUIDv4() \
    ) \
    ENGINE = ReplacingMergeTree(loaded_at) \
    PARTITION BY toYYYYMM(fiscal_date) \
    ORDER BY (ticker, fiscal_date, metric)"
  ] %}

  {% for s in stmts %}
    {{ log('Running: ' ~ s[:120] ~ ('...' if s|length > 120 else ''), info=True) }}
    {% do run_query(s) %}
  {% endfor %}

  {{ log('Provisioned fundamentals DB + tables ✅', info=True) }}
{% endmacro %}