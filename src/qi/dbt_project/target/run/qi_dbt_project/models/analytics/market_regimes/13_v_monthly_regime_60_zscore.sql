

  create or replace view `analytics`.`13_v_monthly_regime_60_zscore` 
  
    
  
  
    
    
  as (
    

WITH
  (12)  AS N_WINDOW,   -- rolling window in months
  (60)  AS N_OUTPUT,   -- most recent 60 months per ticker
  clean AS (
    SELECT
      ticker,
      month_ending,      -- from market.monthly_prices
      adj_close
    FROM market.monthly_prices
    WHERE adj_close > 0
      AND month_ending > toDate('1971-01-01')
  ),
  r AS (
    /* simple monthly returns + prior month’s close */
    SELECT
      ticker,
      month_ending,
      adj_close AS close_price,
      lagInFrame(adj_close, 1) OVER (
        PARTITION BY ticker ORDER BY month_ending
      ) AS prev_close,
      multiIf(
        prev_close IS NULL, NULL,
        prev_close = 0,     NULL,
        (adj_close / prev_close) - 1
      ) AS ret
    FROM clean
  ),
  z AS (
    /* rolling 12-month mean/std, excluding current month */
    SELECT
      ticker,
      month_ending,
      close_price,
      prev_close,
      ret,
      avg(ret) OVER (
        PARTITION BY ticker
        ORDER BY month_ending
        ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
      ) AS mean_12m,
      stddevPop(ret) OVER (
        PARTITION BY ticker
        ORDER BY month_ending
        ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
      ) AS std_12m,
      row_number() OVER (PARTITION BY ticker ORDER BY month_ending DESC) AS rn
    FROM r
  )
SELECT
  ticker,
  month_ending,
  close_price,
  prev_close,
  (close_price - prev_close)                     AS net_change,
  ret                                            AS pct_change,
  round(ret * 100, 2)                            AS pct_change_pct,
  mean_12m,
  std_12m,
  if(std_12m > 0, (ret - mean_12m) / std_12m, NULL) AS zscore_12m,
  multiIf(
    ret IS NULL OR std_12m IS NULL,                               'N/A',
    abs((ret - mean_12m)/nullIf(std_12m, 0)) < 1,                 'Neutral – Typical',
    ret > 0 AND abs((ret - mean_12m)/nullIf(std_12m,0)) >= 1
           AND abs((ret - mean_12m)/nullIf(std_12m,0)) < 2,       'Bull – Volatile',
    ret < 0 AND abs((ret - mean_12m)/nullIf(std_12m,0)) >= 1
           AND abs((ret - mean_12m)/nullIf(std_12m,0)) < 2,       'Bear – Volatile',
    ret > 0 AND abs((ret - mean_12m)/nullIf(std_12m,0)) >= 2,     'Bull – Outlier',
    ret < 0 AND abs((ret - mean_12m)/nullIf(std_12m,0)) >= 2,     'Bear – Outlier',
    'Neutral – Typical'
  ) AS regime
FROM z
WHERE rn <= N_OUTPUT
ORDER BY ticker, month_ending DESC
    
  )
      
      
                    -- end_of_sql
                    
                    