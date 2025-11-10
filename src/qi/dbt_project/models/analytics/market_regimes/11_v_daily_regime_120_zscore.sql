{{ config(materialized='view', database='analytics') }}

WITH
    (20)  AS N_WINDOW,   -- rolling window length (days)
    (120) AS N_OUTPUT,   -- keep most recent N rows per ticker
    dedup AS (  -- one row per (ticker, date), take latest by ingested_at
        SELECT
            ticker,
            date,
            argMax(adj_close, ingested_at) AS adj_close
        FROM market.daily_prices
        GROUP BY ticker, date
    ),
    r AS (  -- daily simple return
        SELECT
            ticker,
            date,
            adj_close AS close_price,
            lagInFrame(adj_close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
            multiIf(
                prev_close IS NULL, NULL,
                prev_close = 0,     NULL,
                (adj_close / prev_close) - 1
            ) AS ret
        FROM dedup
        WHERE adj_close > 0
    ),
    z AS (  -- rolling mean/std over prior N_WINDOW days (exclude current row)
        SELECT
            ticker,
            date,
            close_price,
            prev_close,
            ret,
            avg(ret) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
            ) AS mean_20d,
            stddevPop(ret) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
            ) AS std_20d,
            row_number() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM r
    )
SELECT
    ticker,
    date                                AS trade_date,
    close_price,
    prev_close,
    (close_price - prev_close)          AS net_change,
    ret                                 AS pct_change,
    round(ret * 100, 2)                 AS pct_change_pct,
    mean_20d,
    std_20d,
    if(std_20d > 0, (ret - mean_20d) / std_20d, NULL) AS zscore_20d,
    multiIf(
        ret IS NULL OR std_20d IS NULL,                                   'N/A',
        abs((ret - mean_20d) / nullIf(std_20d, 0)) < 1,                   'Neutral – Typical',
        ret > 0 AND abs((ret - mean_20d) / nullIf(std_20d, 0)) < 2
                AND abs((ret - mean_20d) / nullIf(std_20d, 0)) >= 1,      'Bull – Volatile',
        ret < 0 AND abs((ret - mean_20d) / nullIf(std_20d, 0)) < 2
                AND abs((ret - mean_20d) / nullIf(std_20d, 0)) >= 1,      'Bear – Volatile',
        ret > 0 AND abs((ret - mean_20d) / nullIf(std_20d, 0)) >= 2,      'Bull – Outlier',
        ret < 0 AND abs((ret - mean_20d) / nullIf(std_20d, 0)) >= 2,      'Bear – Outlier',
        'Neutral – Typical'
    ) AS regime
FROM z
WHERE rn <= N_OUTPUT
ORDER BY ticker, trade_date DESC