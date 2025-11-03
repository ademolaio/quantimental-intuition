

WITH latest AS (SELECT max(date) d FROM market.daily_prices)
SELECT
  ticker,
  argMax(short_name, date) AS name,
  anyLast(close)           AS last_close,
  (anyLast(close) - anyLast(close_20d)) / nullIf(anyLast(close_20d), 0) * 100 AS ret_20d_pct,
  (anyLast(close) - anyLast(close_60d)) / nullIf(anyLast(close_60d), 0) * 100 AS ret_60d_pct
FROM
(
  SELECT
    ticker, date, short_name, close,
    lagInFrame(close, 20) OVER (PARTITION BY ticker ORDER BY date) AS close_20d,
    lagInFrame(close, 60) OVER (PARTITION BY ticker ORDER BY date) AS close_60d
  FROM market.daily_prices
) t
GROUP BY ticker
HAVING max(date) = (SELECT d FROM latest)
ORDER BY ret_20d_pct DESC
LIMIT 100