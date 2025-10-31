
  
    
    
    
        
         


        insert into `market`.`monthly_prices`
        ("ticker", "short_name", "month_ending", "open", "high", "low", "close", "adj_close", "volume", "source_max_date", "built_at")-- placeholder model: market.monthly_prices (schema only)
select
    '' as ticker,
    '' as short_name,
    toDate('2000-01-31') as month_ending, -- Date (last trading day of month)
    toFloat64(0) as open,
    toFloat64(0) as high,
    toFloat64(0) as low,
    toFloat64(0) as close,
    toFloat64(0) as adj_close,
    toUInt64(0) as volume,
    toDate('2000-01-01') as source_max_date,
    toDateTime64('2000-01-01 00:00:00', 3) as built_at
where 1 = 0
  