

WITH vix_w AS (
  SELECT
    toMonday(date)              AS week_start,
    argMax(date, date)          AS last_trading_date,
    argMax(close, date)         AS vix_close_raw,
    argMax(adj_close, date)     AS vix_close_adj
  FROM market.daily_prices
  WHERE ticker = '^VIX'
  GROUP BY toMonday(date)
),
vix_ret AS (
  SELECT
    week_start,
    last_trading_date,
    vix_close_raw,
    vix_close_adj,
    lagInFrame(vix_close_raw) OVER (ORDER BY week_start) AS vix_raw_prev,
    lagInFrame(vix_close_adj) OVER (ORDER BY week_start) AS vix_adj_prev
  FROM vix_w
),
vix_feat AS (
  SELECT
    week_start,
    last_trading_date,
    vix_close_raw,
    vix_close_adj,
    multiIf(
      vix_raw_prev IS NULL OR NOT isFinite(vix_raw_prev) OR NOT isFinite(vix_close_raw),
      NULL,
      vix_close_raw - vix_raw_prev
    ) AS vix_net_change_raw,
    multiIf(
      vix_raw_prev IS NULL OR NOT isFinite(vix_raw_prev) OR vix_raw_prev = 0
        OR NOT isFinite(vix_close_raw) OR vix_close_raw = 0,
      NULL,
      (vix_close_raw / vix_raw_prev) - 1
    ) AS vix_pct_change_raw,
    multiIf(
      vix_adj_prev IS NULL OR NOT isFinite(vix_adj_prev) OR NOT isFinite(vix_close_adj),
      NULL,
      vix_close_adj - vix_adj_prev
    ) AS vix_net_change_adj,
    multiIf(
      vix_adj_prev IS NULL OR NOT isFinite(vix_adj_prev) OR vix_adj_prev = 0
        OR NOT isFinite(vix_close_adj) OR vix_close_adj = 0,
      NULL,
      (vix_close_adj / vix_adj_prev) - 1
    ) AS vix_pct_change_adj
  FROM vix_ret
),
mx AS (
  SELECT max(week_start) AS max_week FROM vix_feat
)
SELECT
  week_start,
  last_trading_date,
  multiIf(
    vix_close_raw < 15, 'Calm',
    vix_close_raw < 20, 'Normal',
                        'Fear'
  ) AS vix_regime,
  round(vix_close_raw, 2)            AS vix_close_raw,
  round(vix_net_change_raw, 2)       AS vix_net_change_raw,
  round(vix_pct_change_raw * 100, 2) AS vix_pct_change_raw_pct,
  round(vix_close_adj, 2)            AS vix_close_adj,
  round(vix_net_change_adj, 2)       AS vix_net_change_adj,
  round(vix_pct_change_adj * 100, 2) AS vix_pct_change_adj_pct
FROM vix_feat
WHERE week_start BETWEEN addWeeks((SELECT max_week FROM mx), -21)
                      AND (SELECT max_week FROM mx)
ORDER BY week_start