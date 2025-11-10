

WITH
  d AS (SELECT max(date) AS d FROM market.daily_prices),
  prev_d AS (
    SELECT max(date) AS d
    FROM market.daily_prices
    WHERE date < (SELECT d FROM d)
  )
SELECT
  ticker,
  argMax(short_name, date)                                           AS name,
  (SELECT d FROM d)                                                  AS last_date,
  argMaxIf(close, date, date = (SELECT d FROM d))                    AS close_last,
  argMaxIf(close, date, date = (SELECT d FROM prev_d))               AS close_prev,
  round(100 * (close_last - nullIf(close_prev, 0)) / nullIf(close_prev, 0), 2) AS pct_change
FROM market.daily_prices
WHERE date IN ((SELECT d FROM d), (SELECT d FROM prev_d))
GROUP BY ticker
HAVING close_last IS NOT NULL AND close_prev IS NOT NULL
ORDER BY pct_change DESC
LIMIT 50