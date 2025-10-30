-- Runs automatically on first start
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};

-- sample schema
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.prices
(
  ticker LowCardinality(String),
  trade_date Date,
  open Float64,
  high Float64,
  low  Float64,
  close Float64,
  volume UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ticker, trade_date);