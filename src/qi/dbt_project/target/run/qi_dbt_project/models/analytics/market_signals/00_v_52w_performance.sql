

  create or replace view `analytics`.`00_v_52w_performance` 
  
    
  
  
    
    
  as (
    

WITH addYears(today(), -1) AS cutoff
SELECT
    ticker,
    argMax(short_name, date) AS name,
    max(date)                AS last_date,
    argMax(close, date)      AS last_close,
    maxIf(close, date >= cutoff) AS high_52w,
    minIf(close, date >= cutoff) AS low_52w,
    round(100 * (argMax(close, date) - minIf(close, date >= cutoff))
          / nullIf(minIf(close, date >= cutoff), 0), 2) AS pct_from_low,
    round(100 * (argMax(close, date) - maxIf(close, date >= cutoff))
          / nullIf(maxIf(close, date >= cutoff), 0), 2) AS pct_from_high
FROM market.daily_prices
WHERE date >= cutoff
GROUP BY ticker
ORDER BY pct_from_low DESC
LIMIT 20
    
  )
      
      
                    -- end_of_sql
                    
                    