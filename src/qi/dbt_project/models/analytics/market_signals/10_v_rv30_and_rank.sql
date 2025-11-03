{{ config(materialized='view', database='analytics') }}

WITH
  (SELECT max(date) FROM market.daily_prices) AS latest,
  addYears(latest, -1) AS cutoff_1y
SELECT
  s.ticker,
  argMax(s.short_name, s.date) AS name,
  argMax(s.rv30, s.date)       AS rv30,         -- current rv30
  round(
    100 * sumIf(s.rv30 <= cur.curr_rv30, s.date >= cutoff_1y)
        / nullIf(countIf(s.date >= cutoff_1y), 0),
    2
  )                            AS rv30_rank_1y
FROM
(
  SELECT
    b.ticker,
    b.date,
    b.short_name,
    stddevPop(b.ret)
      OVER (PARTITION BY b.ticker ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      * sqrt(252)              AS rv30
  FROM
  (
    SELECT
      ticker,
      date,
      short_name,
      (log(close) - log(lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date))) AS ret
    FROM market.daily_prices
  ) AS b
) AS s
INNER JOIN
(
  SELECT
    v.ticker,
    max(v.date)            AS last_date,
    argMax(v.rv30, v.date) AS curr_rv30
  FROM
  (
    SELECT
      b.ticker,
      b.date,
      stddevPop(b.ret)
        OVER (PARTITION BY b.ticker ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        * sqrt(252)        AS rv30
    FROM
    (
      SELECT
        ticker,
        date,
        (log(close) - log(lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date))) AS ret
      FROM market.daily_prices
    ) AS b
  ) AS v
  GROUP BY v.ticker
) AS cur USING (ticker)
WHERE s.date >= cutoff_1y
GROUP BY s.ticker
HAVING max(s.date) = latest
ORDER BY rv30_rank_1y DESC
LIMIT 100