

  create or replace view `analytics`.`07_v_52w_scanner` 
  
    
  
  
    
    
  as (
    

WITH
  (SELECT max(date) FROM market.daily_prices) AS latest,
  addYears(latest, -1) AS cutoff
SELECT
  ticker,
  argMax(short_name, date)               AS name,
  maxIf(close, date >= cutoff)           AS high_52w,
  minIf(close, date >= cutoff)           AS low_52w,
  argMax(close, date)                    AS last_close,
  round(100 * (last_close - low_52w)  / nullIf(low_52w,  0), 2) AS pct_from_low,
  round(100 * (last_close - high_52w) / nullIf(high_52w, 0), 2) AS pct_from_high
FROM market.daily_prices
WHERE date >= cutoff
GROUP BY ticker
ORDER BY pct_from_low DESC
LIMIT 100
    
  )
      
      
                    -- end_of_sql
                    
                    