# 🧭 Quantamental Market Monitor

This dashboard presents a **multi-dimensional view of market behavior** — momentum, volatility, participation, and trend structure — built from `market.daily_prices`.  
Each query below captures one *vital sign* of the market and helps traders or analysts interpret where energy, risk, and opportunity are building.

---

## 0️⃣ 52-Week High / Low & Performance
**Purpose:** Identify which tickers are near their 52-week highs or lows.  
**Fields:** `pct_from_low`, `pct_from_high`

**Interpretation:**
- High `pct_from_low` → strong momentum, trend leaders  
- Low `pct_from_low` → depressed, possibly value zones

**Action:**
Use to locate leaders, laggards, or mean-reversion setups.

```sql
WITH addYears(today(), -1) AS cutoff
SELECT
    ticker,
    argMax(short_name, date) AS name,
    max(date)                AS last_date,
    argMax(close, date)      AS last_close,
    maxIf(close, date >= cutoff) AS high_52w,
    minIf(close, date >= cutoff) AS low_52w,
    round(
        100 * (argMax(close, date) - minIf(close, date >= cutoff))
        / nullIf(minIf(close, date >= cutoff), 0), 2
    ) AS pct_from_low,
    round(
        100 * (argMax(close, date) - maxIf(close, date >= cutoff))
        / nullIf(maxIf(close, date >= cutoff), 0), 2
    ) AS pct_from_high
FROM market.daily_prices
WHERE date >= cutoff
GROUP BY ticker
ORDER BY pct_from_low DESC
LIMIT 20;
```

---

## 1️⃣ Top 1-Day % Gainers / Losers
**Purpose:** Track daily extremes — which assets surged or crashed in the last session.  
**Fields:** `pct_change`

**Interpretation:**
- +% → short-term momentum or news catalysts  
- −% → capitulation or panic

**Action:**
Focus daily attention, validate volume or gap confirmations.
```sql
WITH
  d AS (SELECT max(date) AS d FROM market.daily_prices),
  prev_d AS (
    SELECT max(date) AS d
    FROM market.daily_prices
    WHERE date < (SELECT d FROM d)
  )
SELECT
  ticker,
  argMax(short_name, date)                                           AS name,
  (SELECT d FROM d)                                                  AS last_date,
  argMaxIf(close, date, date = (SELECT d FROM d))                    AS close_last,
  argMaxIf(close, date, date = (SELECT d FROM prev_d))               AS close_prev,
  round(100 * (close_last - nullIf(close_prev, 0)) / nullIf(close_prev, 0), 2) AS pct_change
FROM market.daily_prices
WHERE date IN ((SELECT d FROM d), (SELECT d FROM prev_d))
GROUP BY ticker
HAVING close_last IS NOT NULL AND close_prev IS NOT NULL
ORDER BY pct_change DESC
LIMIT 50;
```

---

## 2️⃣ Volume Surge (Today vs 20-Day Avg)
**Purpose:** Detect unusual trading activity relative to recent averages.  
**Fields:** `vol_surge`

**Interpretation:**
- `vol_surge > 2` → double normal volume = institutional attention  
- Combine with price move for conviction

**Action:**
Spot where participation spikes — potential breakouts or reversals.
```sql
WITH d AS (SELECT max(date) AS d FROM market.daily_prices)
SELECT
  ticker,
  argMax(short_name, date)                                             AS name,
  (SELECT d FROM d)                                                    AS last_date,
  argMaxIf(volume, date, date = (SELECT d FROM d))                     AS volume_today,
  avgIf(volume, date >= (SELECT d FROM d) - toIntervalDay(20)
                 AND date <= (SELECT d FROM d))                        AS vol_20d_avg,
  round(volume_today / nullIf(vol_20d_avg, 0), 2)                      AS vol_surge
FROM market.daily_prices
WHERE date >= (SELECT d FROM d) - toIntervalDay(20)
GROUP BY ticker
HAVING volume_today IS NOT NULL AND vol_20d_avg > 0
ORDER BY vol_surge DESC
LIMIT 50;
```

---

## 3️⃣ Gap Up / Gap Down
**Purpose:** Measure sentiment shift between prior close and current open.  
**Fields:** `gap_pct`

**Interpretation:**
- Gap-up = optimism / breakout  
- Gap-down = fear / shakeout

**Action:**
Plan intraday strategies around overnight sentiment changes.
```sql
WITH latest AS (SELECT max(date) d FROM market.daily_prices)
SELECT
  t.ticker,
  argMax(t.short_name, t.date) AS name,
  anyLast(t.date)              AS date,
  anyLast(t.open)              AS open_today,
  anyLast(prev_close)          AS prev_close,
  (open_today - prev_close) / nullIf(prev_close, 0) * 100 AS gap_pct
FROM
(
  SELECT
    ticker, date, short_name, open,
    lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
  FROM market.daily_prices
) AS t
GROUP BY t.ticker
HAVING anyLast(t.date) = (SELECT d FROM latest)
ORDER BY gap_pct DESC
LIMIT 50;
```

---

## 4️⃣ 20-Day vs 60-Day Momentum (Trend Board)
**Purpose:** Compare short-term vs medium-term momentum.  
**Fields:** `ret_20d_pct`, `ret_60d_pct`

**Interpretation:**
- Both positive → confirmed uptrend  
- Diverging → potential trend inflection

**Action:**
Build heatmaps or quadrant plots for trend regime detection.
```sql
WITH latest AS (SELECT max(date) d FROM market.daily_prices)
SELECT
  ticker,
  argMax(short_name, date) AS name,
  anyLast(close)           AS last_close,
  (anyLast(close) - anyLast(close_20d)) / nullIf(anyLast(close_20d), 0) * 100 AS ret_20d_pct,
  (anyLast(close) - anyLast(close_60d)) / nullIf(anyLast(close_60d), 0) * 100 AS ret_60d_pct
FROM
(
  SELECT
    ticker, date, short_name, close,
    lagInFrame(close, 20) OVER (PARTITION BY ticker ORDER BY date) AS close_20d,
    lagInFrame(close, 60) OVER (PARTITION BY ticker ORDER BY date) AS close_60d
  FROM market.daily_prices
) t
GROUP BY ticker
HAVING max(date) = (SELECT d FROM latest)
ORDER BY ret_20d_pct DESC
LIMIT 100;
```

---

## 5️⃣ Distance from 50D & 200D SMA
**Purpose:** Quantify price position relative to key moving averages.  
**Fields:** `dist_sma50_pct`, `dist_sma200_pct`

**Interpretation:**
- Above both → extended  
- Below → basing or breakdown

**Action:**
Use for timing, re-entries, or stop management.
```sql
WITH latest AS (SELECT max(date) d FROM market.daily_prices)
SELECT
  ticker,
  argMax(short_name, date) AS name,
  anyLast(close) AS last_close,
  anyLast(sma50) AS sma50,
  anyLast(sma200) AS sma200,
  (last_close - sma50) / nullIf(sma50, 0) * 100 AS dist_sma50_pct,
  (last_close - sma200)/ nullIf(sma200,0) * 100 AS dist_sma200_pct
FROM
(
  SELECT
    ticker,
    date,
    short_name,
    close,
    avg(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS sma50,
    avg(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200
  FROM market.daily_prices
) s
GROUP BY ticker
HAVING max(date) = (SELECT d FROM latest)
ORDER BY dist_sma50_pct DESC
LIMIT 100;
```

---

## 6️⃣ Market Breadth (% Above 50D / 200D)
**Purpose:** Gauge aggregate market health and participation.  
**Fields:** `pct_above_50d`, `pct_above_200d`

**Interpretation:**
- >70% → broad strength  
- <30% → deterioration

**Action:**
Confirm or fade index-level trends; track structural shifts.
```sql
WITH d AS (SELECT max(date) AS d FROM market.daily_prices)
SELECT
  countIf(last_close > sma50)  / toFloat64(count()) * 100 AS pct_above_50d,
  countIf(last_close > sma200) / toFloat64(count()) * 100 AS pct_above_200d
FROM
(
  SELECT
    ticker,
    date,
    close AS last_close,
    avg(close) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS sma50,
    avg(close) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200,
    row_number() OVER (PARTITION BY ticker ORDER BY date DESC)    AS rn
  FROM market.daily_prices
  WHERE date <= (SELECT d FROM d)
)
WHERE rn = 1;
```

---

## 7️⃣ 52-Week High / Low Scanner (Ranked)
**Purpose:** Rank tickers by distance from 52-week low.  
**Fields:** `pct_from_low`, `pct_from_high`

**Interpretation:**
- High `pct_from_low` = strong new leaders  
- Low `pct_from_low` = deep laggards

**Action:**
Compare sector or region leadership rotation.
```sql
WITH
  (SELECT max(date) FROM market.daily_prices) AS latest,
  addYears(latest, -1) AS cutoff
SELECT
  ticker,
  argMax(short_name, date)               AS name,
  maxIf(close, date >= cutoff)           AS high_52w,
  minIf(close, date >= cutoff)           AS low_52w,
  argMax(close, date)                    AS last_close,
  round(100 * (last_close - low_52w)  / nullIf(low_52w,  0), 2) AS pct_from_low,
  round(100 * (last_close - high_52w) / nullIf(high_52w, 0), 2) AS pct_from_high
FROM market.daily_prices
WHERE date >= cutoff
GROUP BY ticker
ORDER BY pct_from_low DESC
LIMIT 100;
```

---

## 8️⃣ ATR(14) Proxy — Average True Range
**Purpose:** Measure normalized volatility (range intensity).  
**Fields:** `atr14_pct`

**Interpretation:**
- High ATR% = expansion (energy release)  
- Low ATR% = compression (potential breakout)

**Action:**
Calibrate position sizing; find volatility inflection points.
```sql
WITH
  (SELECT max(date) FROM market.daily_prices) AS latest
SELECT
  t.ticker,
  t.name,
  t.last_date AS date,
  t.last_close,
  t.last_atr14,
  round(t.last_atr14 / nullIf(t.last_close, 0) * 100, 2) AS atr14_pct
FROM
(
  /* Stage 2: collapse to the latest row per ticker */
  SELECT
    ticker,
    argMax(short_name, date) AS name,
    max(date)                AS last_date,
    argMax(close,  date)     AS last_close,
    argMax(atr14,  date)     AS last_atr14
  FROM
  (
    /* Stage 1: compute TR and ATR14 per (ticker, date) */
    SELECT
      ticker,
      date,
      short_name,
      close,
      avg(tr) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ) AS atr14
    FROM
    (
      SELECT
        ticker,
        date,
        short_name,
        close,
        high,
        low,
        abs(high - low) AS hl,
        abs(high - lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date)) AS hc,
        abs(low  - lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date)) AS lc,
        greatest(hl, greatest(hc, lc)) AS tr
      FROM market.daily_prices
    )
  )
  GROUP BY ticker
) AS t
WHERE t.last_date = latest
ORDER BY atr14_pct DESC
LIMIT 100;
```

---

## 9️⃣ Inside / Outside Day Tag
**Purpose:** Classify the latest day’s price structure.  
**Tags:** `inside`, `outside`, `normal`

**Interpretation:**
- **Inside day:** compression → breakout potential  
- **Outside day:** expansion → reversal or exhaustion

**Action:**
Screen for breakout setups or volatility transitions.
```sql
WITH
  (SELECT max(date) FROM market.daily_prices) AS latest
SELECT
  t.ticker,
  t.name,
  t.last_date AS date,
  t.open_today,
  t.high_today,
  t.low_today,
  t.close_today,
  case
    when t.high_today > t.prev_high AND t.low_today < t.prev_low then 'outside'
    when t.high_today < t.prev_high AND t.low_today > t.prev_low then 'inside'
    else 'normal'
  end AS day_type
FROM
(
  SELECT
    ticker,
    argMax(short_name, date) AS name,
    max(date)                AS last_date,
    argMax(open, date)       AS open_today,
    argMax(high, date)       AS high_today,
    argMax(low, date)        AS low_today,
    argMax(close, date)      AS close_today,
    argMax(prev_high, date)  AS prev_high,
    argMax(prev_low, date)   AS prev_low
  FROM
  (
    /* compute daily highs/lows + previous-day values */
    SELECT
      ticker,
      date,
      short_name,
      open,
      high,
      low,
      close,
      lagInFrame(high, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_high,
      lagInFrame(low, 1)  OVER (PARTITION BY ticker ORDER BY date) AS prev_low
    FROM market.daily_prices
  )
  GROUP BY ticker
) AS t
WHERE t.last_date = latest
ORDER BY
  case
    when day_type = 'outside' then 1
    when day_type = 'inside'  then 2
    else 3
  end,
  t.ticker
LIMIT 200;
```

---

## 🔟 Realized Vol (30-Day) + 1-Year Percentile (Vol Rank)
**Purpose:** Contextualize realized volatility vs historical range.  
**Fields:** `rv30`, `rv30_rank_1y`

**Interpretation:**
- High rank (>80%) → turbulence, regime change  
- Low rank (<20%) → complacency, volatility may reawaken

**Action:**
Spot where risk is expanding or contracting.
```sql
WITH
  (SELECT max(date) FROM market.daily_prices) AS latest,
  addYears(latest, -1) AS cutoff_1y
SELECT
  s.ticker,
  argMax(s.short_name, s.date) AS name,
  argMax(s.rv30, s.date)       AS rv30,         -- current rv30
  round(
    100 * sumIf(s.rv30 <= cur.curr_rv30, s.date >= cutoff_1y)
        / nullIf(countIf(s.date >= cutoff_1y), 0),
    2
  )                            AS rv30_rank_1y
FROM
(
  -- 1) compute daily returns, then rolling 30d stdev over returns
  SELECT
    b.ticker,
    b.date,
    b.short_name,
    stddevPop(b.ret)
      OVER (PARTITION BY b.ticker ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      * sqrt(252)              AS rv30
  FROM
  (
    SELECT
      ticker,
      date,
      short_name,
      (log(close) - log(lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date))) AS ret
    FROM market.daily_prices
  ) AS b
) AS s
INNER JOIN
(
  -- 2) snapshot each ticker's current rv30 at its last date
  SELECT
    v.ticker,
    max(v.date)          AS last_date,
    argMax(v.rv30, v.date) AS curr_rv30
  FROM
  (
    SELECT
      b.ticker,
      b.date,
      stddevPop(b.ret)
        OVER (PARTITION BY b.ticker ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        * sqrt(252)      AS rv30
    FROM
    (
      SELECT
        ticker,
        date,
        (log(close) - log(lagInFrame(close, 1) OVER (PARTITION BY ticker ORDER BY date))) AS ret
      FROM market.daily_prices
    ) AS b
  ) AS v
  GROUP BY v.ticker
) AS cur USING (ticker)
WHERE s.date >= cutoff_1y
GROUP BY s.ticker
HAVING max(s.date) = latest
ORDER BY rv30_rank_1y DESC
LIMIT 100;
```

---

## 🧩 Thematic Summary

| Theme | Queries       | What They Tell You                                                              |
|------|---------------|---------------------------------------------------------------------------------|
| **Momentum & Strength** | 0, 4, 5, 7    | Trend leaders vs laggards                                                       |
| **Sentiment & Flow** | 1, 2, 3       | Where money & attention concentrate                                             |
| **Market Health** | 6, + 15–17 (these) | Participation & breadth                                                         |
| **Volatility & Regime** | 8, 9, 10      | Risk expansion vs compression                                                   |
| **Macro Regime & Cyclicality** | 11, 12, 13, 14 | Macro trend regime detection Portfolio alignment, and volatility normalization. |

---

## 🧠 How to Read the Dashboard

Think of this as a **trader’s weather map**:
- ☀️ Calm breadth + low ATR = consolidation  
- ⛈️ High volume + gaps + momentum = storm — volatility expanding  
- 🌤️ Rising realized vol + narrow breadth = regime change ahead  

Each query answers one timeless question:
> **“Where is energy building or dissipating in the market right now?”**

---

*Built for the Quantimental Engine — bridging macro structure and micro behavior.*