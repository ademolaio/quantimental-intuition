

WITH latest AS (SELECT max(date) d FROM market.daily_prices)
SELECT
  t.ticker,
  argMax(t.short_name, t.date) AS name,
  anyLast(t.date)              AS date,
  anyLast(t.open)              AS open_today,
  anyLast(prev_close)          AS prev_close,
  (open_today - prev_close) / nullIf(prev_close, 0) * 100 AS gap_pct
FROM
(
  SELECT
    ticker, date, short_name, open,
    lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
  FROM market.daily_prices
) AS t
GROUP BY t.ticker
HAVING anyLast(t.date) = (SELECT d FROM latest)
ORDER BY gap_pct DESC
LIMIT 50