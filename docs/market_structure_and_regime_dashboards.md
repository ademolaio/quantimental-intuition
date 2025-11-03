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

---

## 1️⃣ Top 1-Day % Gainers / Losers
**Purpose:** Track daily extremes — which assets surged or crashed in the last session.  
**Fields:** `pct_change`

**Interpretation:**
- +% → short-term momentum or news catalysts  
- −% → capitulation or panic

**Action:**
Focus daily attention, validate volume or gap confirmations.

---

## 2️⃣ Volume Surge (Today vs 20-Day Avg)
**Purpose:** Detect unusual trading activity relative to recent averages.  
**Fields:** `vol_surge`

**Interpretation:**
- `vol_surge > 2` → double normal volume = institutional attention  
- Combine with price move for conviction

**Action:**
Spot where participation spikes — potential breakouts or reversals.

---

## 3️⃣ Gap Up / Gap Down
**Purpose:** Measure sentiment shift between prior close and current open.  
**Fields:** `gap_pct`

**Interpretation:**
- Gap-up = optimism / breakout  
- Gap-down = fear / shakeout

**Action:**
Plan intraday strategies around overnight sentiment changes.

---

## 4️⃣ 20-Day vs 60-Day Momentum (Trend Board)
**Purpose:** Compare short-term vs medium-term momentum.  
**Fields:** `ret_20d_pct`, `ret_60d_pct`

**Interpretation:**
- Both positive → confirmed uptrend  
- Diverging → potential trend inflection

**Action:**
Build heatmaps or quadrant plots for trend regime detection.

---

## 5️⃣ Distance from 50D & 200D SMA
**Purpose:** Quantify price position relative to key moving averages.  
**Fields:** `dist_sma50_pct`, `dist_sma200_pct`

**Interpretation:**
- Above both → extended  
- Below → basing or breakdown

**Action:**
Use for timing, re-entries, or stop management.

---

## 6️⃣ Market Breadth (% Above 50D / 200D)
**Purpose:** Gauge aggregate market health and participation.  
**Fields:** `pct_above_50d`, `pct_above_200d`

**Interpretation:**
- >70% → broad strength  
- <30% → deterioration

**Action:**
Confirm or fade index-level trends; track structural shifts.

---

## 7️⃣ 52-Week High / Low Scanner (Ranked)
**Purpose:** Rank tickers by distance from 52-week low.  
**Fields:** `pct_from_low`, `pct_from_high`

**Interpretation:**
- High `pct_from_low` = strong new leaders  
- Low `pct_from_low` = deep laggards

**Action:**
Compare sector or region leadership rotation.

---

## 8️⃣ ATR(14) Proxy — Average True Range
**Purpose:** Measure normalized volatility (range intensity).  
**Fields:** `atr14_pct`

**Interpretation:**
- High ATR% = expansion (energy release)  
- Low ATR% = compression (potential breakout)

**Action:**
Calibrate position sizing; find volatility inflection points.

---

## 9️⃣ Inside / Outside Day Tag
**Purpose:** Classify the latest day’s price structure.  
**Tags:** `inside`, `outside`, `normal`

**Interpretation:**
- **Inside day:** compression → breakout potential  
- **Outside day:** expansion → reversal or exhaustion

**Action:**
Screen for breakout setups or volatility transitions.

---

## 🔟 Realized Vol (30-Day) + 1-Year Percentile (Vol Rank)
**Purpose:** Contextualize realized volatility vs historical range.  
**Fields:** `rv30`, `rv30_rank_1y`

**Interpretation:**
- High rank (>80%) → turbulence, regime change  
- Low rank (<20%) → complacency, volatility may reawaken

**Action:**
Spot where risk is expanding or contracting.

---

## 🧩 Thematic Summary

| Theme | Queries | What They Tell You |
|--------|----------|-------------------|
| **Momentum & Strength** | 0, 4, 5, 7 | Trend leaders vs laggards |
| **Sentiment & Flow** | 1, 2, 3 | Where money & attention concentrate |
| **Market Health** | 6 | Participation & breadth |
| **Volatility & Regime** | 8, 9, 10 | Risk expansion vs compression |

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