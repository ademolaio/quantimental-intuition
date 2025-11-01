{{ config(materialized="view") }}

-- Unified LONG ledger across the three raw tables, quarterly only
with inc as (
  select
    'income_statement'::String as statement,
    ticker,
    fiscal_date,
    period,
    metric,
    value,
    currency,
    source,
    loaded_at
  from fundamentals.income_statement
  where period = 'Q'
),
bs as (
  select
    'balance_sheet'::String as statement,
    ticker,
    fiscal_date,
    period,
    metric,
    value,
    currency,
    source,
    loaded_at
  from fundamentals.balance_sheet
  where period = 'Q'
),
cf as (
  select
    'cashflow_statement'::String as statement,
    ticker,
    fiscal_date,
    period,
    metric,
    value,
    currency,
    source,
    loaded_at
  from fundamentals.cashflow_statement
  where period = 'Q'
)

select * from inc
union all
select * from bs
union all
select * from cf