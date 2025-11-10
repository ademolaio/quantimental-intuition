{{ config(materialized='view', database='analytics') }}

WITH
  (12) AS N_WINDOW,   -- rolling window in quarters for z-score stats
  (20) AS N_OUTPUT,   -- keep most recent 20 quarters per ticker (~5y)
  clean AS (
    SELECT
      ticker,
      quarter_ending,
      adj_close
    FROM market.quarterly_prices
    WHERE adj_close > 0
      AND quarter_ending > toDate('1971-01-01')
  ),
  r AS (
    /* simple quarterly return vs prior quarter */
    SELECT
      ticker,
      quarter_ending,
      adj_close AS close_price,
      lagInFrame(adj_close, 1) OVER (
        PARTITION BY ticker ORDER BY quarter_ending
      ) AS prev_close,
      multiIf(
        prev_close IS NULL, NULL,
        prev_close = 0,     NULL,
        (adj_close / prev_close) - 1
      ) AS ret
    FROM clean
  ),
  z AS (
    /* rolling 12-quarter mean/std, excluding current quarter */
    SELECT
      ticker,
      quarter_ending,
      close_price,
      prev_close,
      ret,
      avg(ret) OVER (
        PARTITION BY ticker
        ORDER BY quarter_ending
        ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
      ) AS mean_12q,
      stddevPop(ret) OVER (
        PARTITION BY ticker
        ORDER BY quarter_ending
        ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
      ) AS std_12q,
      row_number() OVER (PARTITION BY ticker ORDER BY quarter_ending DESC) AS rn
    FROM r
  )
SELECT
  ticker,
  quarter_ending,
  close_price,
  prev_close,
  (close_price - prev_close)                 AS net_change,
  ret                                        AS pct_change,
  round(ret * 100, 2)                        AS pct_change_pct,
  mean_12q,
  std_12q,
  if(std_12q > 0, (ret - mean_12q) / std_12q, NULL) AS zscore_12q,
  multiIf(
    ret IS NULL OR std_12q IS NULL,                               'N/A',
    abs((ret - mean_12q) / nullIf(std_12q, 0)) < 1,               'Neutral – Typical',
    ret > 0 AND abs((ret - mean_12q) / nullIf(std_12q, 0)) >= 1
           AND abs((ret - mean_12q) / nullIf(std_12q, 0)) < 2,    'Bull – Volatile',
    ret < 0 AND abs((ret - mean_12q) / nullIf(std_12q, 0)) >= 1
           AND abs((ret - mean_12q) / nullIf(std_12q, 0)) < 2,    'Bear – Volatile',
    ret > 0 AND abs((ret - mean_12q) / nullIf(std_12q, 0)) >= 2,  'Bull – Outlier',
    ret < 0 AND abs((ret - mean_12q) / nullIf(std_12q, 0)) >= 2,  'Bear – Outlier',
    'Neutral – Typical'
  ) AS regime
FROM z
WHERE rn <= N_OUTPUT
ORDER BY ticker, quarter_ending DESC