-- db/init/10_fundamentals.sql
CREATE DATABASE IF NOT EXISTS fundamentals;

CREATE TABLE IF NOT EXISTS fundamentals.income_statement
(
  ticker     String,
  fiscal_date Date,
  period     LowCardinality(String),   -- 'Q' or 'A'
  metric     LowCardinality(String),   -- e.g., 'TotalRevenue', 'NetIncome'
  value      Float64,
  currency   LowCardinality(String),
  source     LowCardinality(String) DEFAULT 'yfinance',
  loaded_at  DateTime DEFAULT now(),
  _batch_id  UUID DEFAULT generateUUIDv4()
)
ENGINE = ReplacingMergeTree(_batch_id)
PARTITION BY toYYYYMM(fiscal_date)
ORDER BY (ticker, fiscal_date, metric);

CREATE TABLE IF NOT EXISTS fundamentals.balance_sheet
(
  ticker      String,
  fiscal_date Date,
  period      LowCardinality(String),
  metric      LowCardinality(String),  -- e.g., 'TotalAssets', 'TotalLiab', 'TotalStockholderEquity'
  value       Float64,
  currency    LowCardinality(String),
  source      LowCardinality(String) DEFAULT 'yfinance',
  loaded_at   DateTime DEFAULT now(),
  _batch_id   UUID DEFAULT generateUUIDv4()
)
ENGINE = ReplacingMergeTree(_batch_id)
PARTITION BY toYYYYMM(fiscal_date)
ORDER BY (ticker, fiscal_date, metric);

CREATE TABLE IF NOT EXISTS fundamentals.cashflow_statement
(
  ticker      String,
  fiscal_date Date,
  period      LowCardinality(String),
  metric      LowCardinality(String),  -- e.g., 'TotalCashFromOperatingActivities', 'CapitalExpenditures'
  value       Float64,
  currency    LowCardinality(String),
  source      LowCardinality(String) DEFAULT 'yfinance',
  loaded_at   DateTime DEFAULT now(),
  _batch_id   UUID DEFAULT generateUUIDv4()
)
ENGINE = ReplacingMergeTree(_batch_id)
PARTITION BY toYYYYMM(fiscal_date)
ORDER BY (ticker, fiscal_date, metric);