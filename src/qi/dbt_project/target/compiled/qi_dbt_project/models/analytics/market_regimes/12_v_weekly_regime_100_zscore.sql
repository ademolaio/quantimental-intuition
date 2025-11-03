

WITH
    (12)  AS N_WINDOW,   -- rolling window in weeks for z-score stats
    (100) AS N_OUTPUT,   -- keep most recent 100 weeks per ticker
    clean AS (
        /* Use your aggregated weekly table; keep valid prices only */
        SELECT
            ticker,
            week_ending,                    -- last trading day of the ISO week
            adj_close
        FROM market.weekly_prices
        WHERE adj_close > 0
          AND week_ending > toDate('1971-01-01')
    ),
    r AS (
        /* compute simple weekly returns + previous close */
        SELECT
            ticker,
            week_ending,
            adj_close AS close_price,
            lagInFrame(adj_close, 1) OVER (
                PARTITION BY ticker ORDER BY week_ending
            ) AS prev_close,
            multiIf(
                prev_close IS NULL, NULL,
                prev_close = 0,     NULL,
                (adj_close / prev_close) - 1
            ) AS ret
        FROM clean
    ),
    z AS (
        /* rolling 12-week mean/std (exclude current week) */
        SELECT
            ticker,
            week_ending,
            close_price,
            prev_close,
            ret,
            avg(ret) OVER (
                PARTITION BY ticker
                ORDER BY week_ending
                ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
            ) AS mean_12w,
            stddevPop(ret) OVER (
                PARTITION BY ticker
                ORDER BY week_ending
                ROWS BETWEEN N_WINDOW PRECEDING AND 1 PRECEDING
            ) AS std_12w,
            row_number() OVER (PARTITION BY ticker ORDER BY week_ending DESC) AS rn
        FROM r
    )
SELECT
    ticker,
    week_ending                                   AS week_end,
    close_price,
    prev_close,
    (close_price - prev_close)                    AS net_change,
    ret                                           AS pct_change,
    round(ret * 100, 2)                           AS pct_change_pct,
    mean_12w,
    std_12w,
    if(std_12w > 0, (ret - mean_12w) / std_12w, NULL) AS zscore_12w,
    multiIf(
        ret IS NULL OR std_12w IS NULL,                               'N/A',
        abs((ret - mean_12w) / nullIf(std_12w, 0)) < 1,               'Neutral – Typical',
        ret > 0 AND abs((ret - mean_12w) / nullIf(std_12w, 0)) < 2
               AND abs((ret - mean_12w) / nullIf(std_12w, 0)) >= 1,   'Bull – Volatile',
        ret < 0 AND abs((ret - mean_12w) / nullIf(std_12w, 0)) < 2
               AND abs((ret - mean_12w) / nullIf(std_12w, 0)) >= 1,   'Bear – Volatile',
        ret > 0 AND abs((ret - mean_12w) / nullIf(std_12w, 0)) >= 2,  'Bull – Outlier',
        ret < 0 AND abs((ret - mean_12w) / nullIf(std_12w, 0)) >= 2,  'Bear – Outlier',
        'Neutral – Typical'
    ) AS regime
FROM z
WHERE rn <= N_OUTPUT
ORDER BY ticker, week_end DESC