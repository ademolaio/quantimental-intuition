

  create or replace view `analytics`.`08_v_atr14_pct` 
  
    
  
  
    
    
  as (
    

WITH
  (SELECT max(date) FROM market.daily_prices) AS latest
SELECT
  t.ticker,
  t.name,
  t.last_date AS date,
  t.last_close,
  t.last_atr14,
  round(t.last_atr14 / nullIf(t.last_close, 0) * 100, 2) AS atr14_pct
FROM
(
  SELECT
    ticker,
    argMax(short_name, date) AS name,
    max(date)                AS last_date,
    argMax(close,  date)     AS last_close,
    argMax(atr14,  date)     AS last_atr14
  FROM
  (
    SELECT
      ticker,
      date,
      short_name,
      close,
      avg(tr) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ) AS atr14
    FROM
    (
      SELECT
        ticker,
        date,
        short_name,
        close,
        high,
        low,
        abs(high - low) AS hl,
        abs(high - lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date)) AS hc,
        abs(low  - lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date)) AS lc,
        greatest(hl, greatest(hc, lc)) AS tr
      FROM market.daily_prices
    )
  )
  GROUP BY ticker
) AS t
WHERE t.last_date = latest
ORDER BY atr14_pct DESC
LIMIT 100
    
  )
      
      
                    -- end_of_sql
                    
                    