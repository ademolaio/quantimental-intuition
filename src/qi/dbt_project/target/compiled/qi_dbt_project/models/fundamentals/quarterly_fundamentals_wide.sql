

WITH base AS (
  SELECT
    ticker      AS ticker,
    fiscal_date AS fiscal_date,
    argMax(currency, loaded_at) AS currency
  FROM `fundamentals`.`quarterly_fundamentals_long`
  GROUP BY ticker, fiscal_date
),
inc AS (
  SELECT
    ticker, fiscal_date,
    maxIf(value, metric = 'Total Revenue')    AS total_revenue,
    maxIf(value, metric = 'Cost Of Revenue')  AS cost_of_revenue,
    maxIf(value, metric = 'Gross Profit')     AS gross_profit,
    maxIf(value, metric = 'Operating Income') AS operating_income,
    maxIf(value, metric = 'Net Income')       AS net_income
  FROM `fundamentals`.`quarterly_fundamentals_long`
  WHERE statement = 'income_statement'
  GROUP BY ticker, fiscal_date
),
bs AS (
  SELECT
    ticker, fiscal_date,
    maxIf(value, metric = 'Total Assets')                  AS total_assets,
    maxIf(value, metric = 'Total Liab')                    AS total_liab,
    maxIf(value, metric = 'Total Stockholder Equity')      AS equity,
    maxIf(value, metric = 'Total Current Assets')          AS current_assets,
    maxIf(value, metric = 'Total Current Liabilities')     AS current_liab,
    maxIf(value, metric = 'Cash And Cash Equivalents')     AS cash_and_equiv,
    maxIf(value, metric = 'Long Term Debt')                AS long_term_debt
  FROM `fundamentals`.`quarterly_fundamentals_long`
  WHERE statement = 'balance_sheet'
  GROUP BY ticker, fiscal_date
),
cf AS (
  SELECT
    ticker, fiscal_date,
    maxIf(value, metric = 'Total Cash From Operating Activities') AS cfo,
    maxIf(value, metric = 'Capital Expenditures')                 AS capex
  FROM `fundamentals`.`quarterly_fundamentals_long`
  WHERE statement = 'cashflow_statement'
  GROUP BY ticker, fiscal_date
)

SELECT
  b.ticker       AS ticker,
  b.fiscal_date  AS fiscal_date,
  b.currency     AS currency,
  inc.total_revenue,
  inc.cost_of_revenue,
  inc.gross_profit,
  inc.operating_income,
  inc.net_income,
  bs.total_assets,
  bs.total_liab,
  bs.equity,
  bs.current_assets,
  bs.current_liab,
  bs.cash_and_equiv,
  bs.long_term_debt,
  cf.cfo,
  cf.capex
FROM base b
LEFT JOIN inc ON b.ticker = inc.ticker AND b.fiscal_date = inc.fiscal_date
LEFT JOIN bs  ON b.ticker = bs.ticker  AND b.fiscal_date = bs.fiscal_date
LEFT JOIN cf  ON b.ticker = cf.ticker  AND b.fiscal_date = cf.fiscal_date