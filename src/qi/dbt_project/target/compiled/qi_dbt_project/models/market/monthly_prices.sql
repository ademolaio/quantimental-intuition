-- Real monthly aggregate: month_ending is last trading day of the month
select
  ticker,
  argMax(short_name, date)        as short_name,
  max(date)                       as month_ending,
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
  toStartOfMonth(date)
order by ticker, month_ending