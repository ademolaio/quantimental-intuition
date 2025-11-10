

WITH d AS (SELECT max(date) AS d FROM market.daily_prices)
SELECT
  ticker,
  argMax(short_name, date)                                             AS name,
  (SELECT d FROM d)                                                    AS last_date,
  argMaxIf(volume, date, date = (SELECT d FROM d))                     AS volume_today,
  avgIf(volume, date >= (SELECT d FROM d) - toIntervalDay(20)
                 AND date <= (SELECT d FROM d))                        AS vol_20d_avg,
  round(volume_today / nullIf(vol_20d_avg, 0), 2)                      AS vol_surge
FROM market.daily_prices
WHERE date >= (SELECT d FROM d) - toIntervalDay(20)
GROUP BY ticker
HAVING volume_today IS NOT NULL AND vol_20d_avg > 0
ORDER BY vol_surge DESC
LIMIT 50