# QI Data Warehouse Architecture Blueprint

## 1. Schema Design

### Final Schemas
- **market** — Time-series data (prices, volumes, and performance metrics)
- **fundamentals** — Financial statement and ratio data (income, balance sheet, cash flow, valuation metrics)
- **meta** — Reference data (tickers, sectors, industries, currency, company information)

These schemas create a clear separation of data domains, mirroring institutional data warehouse standards.

---

## 2. Table Engine Strategy

| Schema | Engine | Purpose |
|---------|---------|----------|
| `market.*` | **MergeTree** | Optimized for immutable time-series data. Used for OHLCV and weekly price updates. |
| `fundamentals.*` | **ReplacingMergeTree** | Supports data corrections and restatements (financial reports). |
| `meta.*` | **ReplacingMergeTree** | Allows updates when metadata changes (e.g., sector reclassifications, ticker info). |

**Rationale:**  
- `MergeTree` = high-performance append-only storage.  
- `ReplacingMergeTree` = version-aware storage for mutable datasets.

---

## 3. Partitioning & Ordering Strategy

- **Partition by:** `toYYYYMMDD(date)`  
- **Order by:** `(ticker, date)`  

### Benefits
- Ideal for fine-grained time-series queries.  
- Enables efficient weekly inserts and daily-range queries.  
- Future-proof: can later change to monthly partitions (`toYYYYMM(date)`) if dataset size scales dramatically.

---

## 4. Data Loading Strategy

### Phase 1: Historical Backfill
- **Range:** From **1999-12-31** to present.  
- **Purpose:** Build a complete historical baseline for all tickers.

### Phase 2: Incremental Updates
- **Frequency:** Weekly (once per week).  
- **Purpose:** Append the latest 7 days of data for all tickers.  

This strategy mirrors institutional practices — a one-time backfill, followed by rolling incremental updates.

---

## 5. Naming Conventions

- **Schema, table, and column names:** all lowercase with underscores (`snake_case`)
- Example: `market.daily_prices`, `fundamentals.financials_income`, `meta.tickers`
- Columns like `total_revenue`, `operating_income`, `net_income`

Consistent lowercase naming ensures compatibility across ClickHouse, dbt, Airflow, and Superset.

---

## 6. Summary of Final Design Decisions

| Design Element | Decision | Status |
|-----------------|-----------|--------|
| **Schemas** | `market`, `fundamentals`, `meta` | ✅ Confirmed |
| **Engines** | `MergeTree` for market; `ReplacingMergeTree` for fundamentals & meta | ✅ Confirmed |
| **Partitioning** | `toYYYYMMDD(date)` | ✅ Confirmed |
| **Ordering** | `(ticker, date)` | ✅ Confirmed |
| **Naming Convention** | Lowercase with underscores | ✅ Confirmed |
| **Data Strategy** | One-time historical pull (1999-12-31 → today), then weekly updates | ✅ Confirmed |

---

## 7. Notes

- These design choices prioritize **read performance, clarity, and long-term scalability**.  
- This foundation supports easy integration with **dbt** for transformations and **Airflow** for scheduling.  
- Superset will connect primarily to analytic or dbt-transformed tables for visualization.

