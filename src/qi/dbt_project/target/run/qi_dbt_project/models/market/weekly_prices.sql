
  
    
    
    
        
         


        insert into `market`.`weekly_prices`
        ("ticker", "short_name", "week_ending", "open", "high", "low", "close", "adj_close", "volume", "source_max_date", "built_at")-- placeholder model: market.weekly_prices (schema only)
select
    '' as ticker,
    '' as short_name,
    toDate('2000-01-07') as week_ending,  -- Date (last trading day of week)
    toFloat64(0) as open,
    toFloat64(0) as high,
    toFloat64(0) as low,
    toFloat64(0) as close,
    toFloat64(0) as adj_close,
    toUInt64(0) as volume,
    toDate('2000-01-01') as source_max_date,                 -- max daily date used
    toDateTime64('2000-01-01 00:00:00', 3) as built_at        -- when built
where 1 = 0
  