{{ config(materialized='view', database='analytics') }}

-- 5-year (60-month) distribution + payoff/expectancy stats per ticker
WITH base AS (
  SELECT
    ticker,
    month_ending,
    adj_close,
    lagInFrame(adj_close, 1) OVER (
      PARTITION BY ticker ORDER BY month_ending
    ) AS prev_adj,
    adj_close / lagInFrame(adj_close, 1) OVER (
      PARTITION BY ticker ORDER BY month_ending
    ) - 1 AS ret_raw
  FROM market.monthly_prices
),

last60 AS (
  SELECT
    ticker,
    month_ending,
    adj_close,
    -- valid simple return only if finite and prev_adj > 0
    if(isFinite(ret_raw) AND prev_adj > 0, ret_raw, NULL)               AS ret_ok,
    -- valid log return only if 1+ret_ok > 0
    if(ret_ok IS NOT NULL AND (1 + ret_ok) > 0, log(1 + ret_ok), NULL)  AS rlog_ok,
    row_number() OVER (PARTITION BY ticker ORDER BY month_ending DESC)   AS rn
  FROM base
)

SELECT
  ticker,

  /* coverage / validity */
  countIf(rn <= 60)                                  AS n_months,
  countIf(rn <= 60 AND ret_ok  IS NOT NULL)          AS n_months_ret_valid,
  countIf(rn <= 60 AND rlog_ok IS NOT NULL)          AS n_months_log_valid,

  /* distribution of monthly log returns (last 60) */
  avgIf(rlog_ok,       rn <= 60)                     AS mu_log,
  stddevPopIf(rlog_ok, rn <= 60)                     AS sigma_log,
  skewPopIf(rlog_ok,   rn <= 60)                     AS skew,
  kurtPopIf(rlog_ok,   rn <= 60)                     AS kurt,

  /* “hit rate” and outlier share on simple returns */
  avgIf(ret_ok > 0,         rn <= 60)                AS hit_rate,      -- share of up months
  avgIf(abs(ret_ok) > 0.05, rn <= 60)                AS pct_outlier,   -- |ret| > 5%

  /* bucket counts + fractions on |ret_ok| (disjoint) */
  countIf(rn <= 60 AND ret_ok IS NOT NULL AND abs(ret_ok) <= 0.05)            AS n_0_5,
  avgIf(           abs(ret_ok) <= 0.05, rn <= 60)                              AS b_0_5,
  countIf(rn <= 60 AND abs(ret_ok) > 0.05 AND abs(ret_ok) <= 0.10)             AS n_5_10,
  avgIf(           abs(ret_ok) > 0.05 AND abs(ret_ok) <= 0.10, rn <= 60)       AS b_5_10,
  countIf(rn <= 60 AND abs(ret_ok) > 0.10 AND abs(ret_ok) <= 0.15)             AS n_10_15,
  avgIf(           abs(ret_ok) > 0.10 AND abs(ret_ok) <= 0.15, rn <= 60)       AS b_10_15,
  countIf(rn <= 60 AND abs(ret_ok) > 0.15 AND abs(ret_ok) <= 0.20)             AS n_15_20,
  avgIf(           abs(ret_ok) > 0.15 AND abs(ret_ok) <= 0.20, rn <= 60)       AS b_15_20,
  countIf(rn <= 60 AND abs(ret_ok) > 0.20)                                     AS n_ge_20,
  avgIf(           abs(ret_ok) > 0.20, rn <= 60)                                AS b_ge_20,

  ( n_0_5 + n_5_10 + n_10_15 + n_15_20 + n_ge_20 )                              AS bucket_count_sum,
  ( b_0_5 + b_5_10 + b_10_15 + b_15_20 + b_ge_20 )                              AS bucket_sum,

  /* last close (latest month in window) */
  argMaxIf(adj_close, month_ending, rn = 1)                                     AS last_close,

  /* convert vol/mean to dollar terms at current price */
  stddevPopIf(ret_ok, rn <= 60) * last_close                                    AS hist_em_abs,
  avgIf(ret_ok, ret_ok > 0 AND rn <= 60)  * last_close                           AS avg_up_dollar,
  abs(avgIf(ret_ok, ret_ok < 0 AND rn <= 60)) * last_close                       AS avg_down_dollar,

  /* payoff = avg up move / |avg down move| */
  if(
    isFinite(
      avgIf(ret_ok, ret_ok > 0 AND rn <= 60)
      / nullIf(abs(avgIf(ret_ok, ret_ok < 0 AND rn <= 60)), 0)
    ),
    avgIf(ret_ok, ret_ok > 0 AND rn <= 60)
    / nullIf(abs(avgIf(ret_ok, ret_ok < 0 AND rn <= 60)), 0),
    NULL
  ) AS payoff,

  /* expectancy per month (in $) at current price */
  if(
    isFinite(
      (avgIf(ret_ok > 0, rn <= 60) * (avgIf(ret_ok, ret_ok > 0 AND rn <= 60)  * last_close))
      - ((1 - avgIf(ret_ok > 0, rn <= 60)) * (abs(avgIf(ret_ok, ret_ok < 0 AND rn <= 60)) * last_close))
    ),
    (avgIf(ret_ok > 0, rn <= 60) * (avgIf(ret_ok, ret_ok > 0 AND rn <= 60)  * last_close))
    - ((1 - avgIf(ret_ok > 0, rn <= 60)) * (abs(avgIf(ret_ok, ret_ok < 0 AND rn <= 60)) * last_close)),
    NULL
  ) AS expectancy_per_mo

FROM last60
GROUP BY ticker
ORDER BY ticker