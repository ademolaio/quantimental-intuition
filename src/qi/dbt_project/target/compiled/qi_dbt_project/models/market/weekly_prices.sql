-- Real weekly aggregate: ISO week groups; week_ending is the last trading day in that week
select
  ticker,
  argMax(short_name, date)        as short_name,       -- name as of last day in week
  max(date)                       as week_ending,      -- last trading day in ISO week
  argMin(open,  date)             as open,             -- first open in week
  max(high)                       as high,             -- highest high in week
  min(low)                        as low,              -- lowest low in week
  argMax(close, date)             as close,            -- last close in week
  argMax(adj_close, date)         as adj_close,        -- last adjusted close in week
  sum(volume)                     as volume,           -- total volume in week
  max(date)                       as source_max_date,  -- provenance
  now()                           as built_at
from market.daily_prices
group by
  ticker,
  toISOYear(date),
  toISOWeek(date)
order by ticker, week_ending