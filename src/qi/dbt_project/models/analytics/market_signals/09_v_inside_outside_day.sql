{{ config(materialized='view', database='analytics') }}

WITH
  (SELECT max(date) FROM market.daily_prices) AS latest
SELECT
  t.ticker,
  t.name,
  t.last_date AS date,
  t.open_today,
  t.high_today,
  t.low_today,
  t.close_today,
  case
    when t.high_today > t.prev_high AND t.low_today < t.prev_low then 'outside'
    when t.high_today < t.prev_high AND t.low_today > t.prev_low then 'inside'
    else 'normal'
  end AS day_type
FROM
(
  SELECT
    ticker,
    argMax(short_name, date) AS name,
    max(date)                AS last_date,
    argMax(open, date)       AS open_today,
    argMax(high, date)       AS high_today,
    argMax(low, date)        AS low_today,
    argMax(close, date)      AS close_today,
    argMax(prev_high, date)  AS prev_high,
    argMax(prev_low, date)   AS prev_low
  FROM
  (
    SELECT
      ticker,
      date,
      short_name,
      open,
      high,
      low,
      close,
      lagInFrame(high, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_high,
      lagInFrame(low, 1)  OVER (PARTITION BY ticker ORDER BY date) AS prev_low
    FROM market.daily_prices
  )
  GROUP BY ticker
) AS t
WHERE t.last_date = latest
ORDER BY
  case
    when day_type = 'outside' then 1
    when day_type = 'inside'  then 2
    else 3
  end,
  t.ticker
LIMIT 200