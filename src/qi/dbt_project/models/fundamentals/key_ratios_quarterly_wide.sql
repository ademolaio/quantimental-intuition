{{ config(materialized='view') }}

SELECT
  w.ticker,
  w.fiscal_date,
  w.currency,
  w.total_revenue,
  w.gross_profit,
  w.operating_income,
  w.net_income,
  w.total_assets,
  w.total_liab,
  w.equity,
  w.current_assets,
  w.current_liab,
  w.cash_and_equiv,
  w.long_term_debt,
  w.cfo,
  w.capex,
  ifNull(w.gross_profit     / nullIf(w.total_revenue, 0), 0) AS gross_margin,
  ifNull(w.operating_income / nullIf(w.total_revenue, 0), 0) AS operating_margin,
  ifNull(w.net_income       / nullIf(w.total_revenue, 0), 0) AS net_margin,
  ifNull(w.current_assets   / nullIf(w.current_liab, 0), 0)  AS current_ratio,
  ifNull(w.total_liab       / nullIf(w.equity, 0), 0)        AS debt_to_equity,
  (w.cfo - w.capex)                                          AS fcf
FROM {{ ref('quarterly_fundamentals_wide') }} AS w