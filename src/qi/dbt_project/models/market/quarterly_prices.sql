-- placeholder model: market.quarterly_prices (schema only)
select
    '' as ticker,
    '' as short_name,
    toDate('2000-03-31') as quarter_ending, -- Date (last trading day of quarter)
    toFloat64(0) as open,
    toFloat64(0) as high,
    toFloat64(0) as low,
    toFloat64(0) as close,
    toFloat64(0) as adj_close,
    toUInt64(0) as volume,
    toDate('2000-01-01') as source_max_date,
    toDateTime64('2000-01-01 00:00:00', 3) as built_at
where 1 = 0