-- placeholder model: market.daily_prices (schema only)
select
    ''  as ticker,                  -- String
    ''  as short_name,              -- String
    toDate('2000-01-01') as date,   -- Date
    toFloat64(0) as open,
    toFloat64(0) as high,
    toFloat64(0) as low,
    toFloat64(0) as close,
    toFloat64(0) as adj_close,
    toUInt64(0) as volume,
    toDateTime64('2000-01-01 00:00:00', 3) as ingested_at,  -- DateTime64(3)
    '' as batch_id,                 -- String (UUID or run id when you load)
    'yfinance' as source            -- LowCardinality(String) at load time
where 1 = 0