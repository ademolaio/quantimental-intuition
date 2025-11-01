

  create or replace view `fundamentals`.`key_ratios_quarterly_long` 
  
    
  
  
    
    
  as (
    

WITH w AS (
  SELECT * FROM `fundamentals`.`quarterly_fundamentals_wide`
),
r AS (
  SELECT
    w.ticker, w.fiscal_date, w.currency,
    ifNull(w.gross_profit     / nullIf(w.total_revenue, 0), 0) AS gross_margin,
    ifNull(w.operating_income / nullIf(w.total_revenue, 0), 0) AS operating_margin,
    ifNull(w.net_income       / nullIf(w.total_revenue, 0), 0) AS net_margin,
    ifNull(w.current_assets   / nullIf(w.current_liab, 0), 0)  AS current_ratio,
    ifNull(w.total_liab       / nullIf(w.equity, 0), 0)        AS debt_to_equity,
    (w.cfo - w.capex)                                          AS fcf
  FROM w
)
SELECT ticker, fiscal_date, currency, 'gross_margin'     AS metric, toFloat64(gross_margin)     AS value FROM r
UNION ALL
SELECT ticker, fiscal_date, currency, 'operating_margin',          toFloat64(operating_margin)          FROM r
UNION ALL
SELECT ticker, fiscal_date, currency, 'net_margin',                toFloat64(net_margin)                FROM r
UNION ALL
SELECT ticker, fiscal_date, currency, 'current_ratio',             toFloat64(current_ratio)             FROM r
UNION ALL
SELECT ticker, fiscal_date, currency, 'debt_to_equity',            toFloat64(debt_to_equity)            FROM r
UNION ALL
SELECT ticker, fiscal_date, currency, 'fcf',                       toFloat64(fcf)                       FROM r
    
  )
      
      
                    -- end_of_sql
                    
                    