
  
    
    
    
        
         


        insert into `market`.`quarterly_prices__dbt_backup`
        ("ticker", "short_name", "quarter_ending", "open", "high", "low", "close", "adj_close", "volume", "source_max_date", "built_at")-- Real quarterly aggregate: quarter_ending is last trading day of the quarter
select
  ticker,
  argMax(short_name, date)        as short_name,
  max(date)                       as quarter_ending,
  argMin(open,  date)             as open,
  max(high)                       as high,
  min(low)                        as low,
  argMax(close, date)             as close,
  argMax(adj_close, date)         as adj_close,
  sum(volume)                     as volume,
  max(date)                       as source_max_date,
  now()                           as built_at
from market.daily_prices
group by
  ticker,
  toStartOfQuarter(date)
order by ticker, quarter_ending
  