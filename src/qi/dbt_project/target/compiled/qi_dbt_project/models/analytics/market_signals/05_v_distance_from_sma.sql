

WITH latest AS (SELECT max(date) d FROM market.daily_prices)
SELECT
  ticker,
  argMax(short_name, date) AS name,
  anyLast(close) AS last_close,
  anyLast(sma50) AS sma50,
  anyLast(sma200) AS sma200,
  (last_close - sma50) / nullIf(sma50, 0) * 100 AS dist_sma50_pct,
  (last_close - sma200)/ nullIf(sma200,0) * 100 AS dist_sma200_pct
FROM
(
  SELECT
    ticker,
    date,
    short_name,
    close,
    avg(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS sma50,
    avg(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200
  FROM market.daily_prices
) s
GROUP BY ticker
HAVING max(date) = (SELECT d FROM latest)
ORDER BY dist_sma50_pct DESC
LIMIT 100