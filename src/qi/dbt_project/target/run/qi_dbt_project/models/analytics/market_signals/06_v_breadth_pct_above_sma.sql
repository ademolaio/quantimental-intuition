

  create or replace view `analytics`.`06_v_breadth_pct_above_sma` 
  
    
  
  
    
    
  as (
    

WITH d AS (SELECT max(date) AS d FROM market.daily_prices)
SELECT
  countIf(last_close > sma50)  / toFloat64(count()) * 100 AS pct_above_50d,
  countIf(last_close > sma200) / toFloat64(count()) * 100 AS pct_above_200d
FROM
(
  SELECT
    ticker,
    date,
    close AS last_close,
    avg(close) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS sma50,
    avg(close) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200,
    row_number() OVER (PARTITION BY ticker ORDER BY date DESC)    AS rn
  FROM market.daily_prices
  WHERE date <= (SELECT d FROM d)
)
WHERE rn = 1
    
  )
      
      
                    -- end_of_sql
                    
                    