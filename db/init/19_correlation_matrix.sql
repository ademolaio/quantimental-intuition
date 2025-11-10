WITH
  -- Define focal (row) and comparison (column) universes
  ['AAPL','MSFT','NVDA','GOOGL','AMZN','META','TSLA', 'PLTR', 'SOFI', 'NFLX', 'COST', 'BRK-B', 'PEP', 'JNJ','MA','MCD','WMT','LMT','INTU','JPM','UNH','XOM','MCK','GE','IBM','CRM'] AS mag7,
  [
    'AAPL','AMD','AMZN','GOOG','META','MSFT','NVDA','SOFI','TSLA',
    'JNJ','MA','MCD','WMT','LMT','INTU','JPM','UNH','XOM','MCK','GE',
    'COST','BRK-B','DBX','GM','T','K','AAL','F','DAL','GS','MSI',
    'PYPL','PLTR','AVGO','GOOGL','PEP','V','CAT','SHW','IBM','CRM',
    'HD','BA','MMM','DIS','PG','CVX','MRK','CSCO','VZ','KO','HON',
    'AXP','AMGN','TRV','NFLX','NKE','BB','RIVN','LCID','TOST','FUBO',
    'CHPT','SNAP','CHYM','LYFT','CLOV','LMND'
  ] AS universe,

-- Step 1: compute daily log returns per ticker
daily_ret AS (
  SELECT
    ticker,
    date,
    multiIf(
      lagInFrame(adj_close) OVER (PARTITION BY ticker ORDER BY date) IS NULL, NULL,
      lagInFrame(adj_close) OVER (PARTITION BY ticker ORDER BY date) <= 0, NULL,
      adj_close <= 0, NULL,
      log(adj_close / lagInFrame(adj_close) OVER (PARTITION BY ticker ORDER BY date))
    ) AS ret_log
  FROM market.daily_prices
  WHERE adj_close > 0
    AND date >= addMonths(today(), -13)
),

-- Step 2: compute correlations between focal (A) and universe (B)
pair AS (
  SELECT
    a.ticker AS ticker_a,              -- focal asset
    b.ticker AS ticker_b,              -- comparison asset
    corr(a.ret_log, b.ret_log) AS corr_1y,
    count() AS n_obs
  FROM daily_ret AS a
  INNER JOIN daily_ret AS b
    ON a.date = b.date
  WHERE a.ticker IN mag7
    AND b.ticker IN universe
    AND a.ret_log IS NOT NULL
    AND b.ret_log IS NOT NULL
    AND a.date >= addMonths(today(), -12)
  GROUP BY ticker_a, ticker_b
  HAVING n_obs >= 150
),

-- Step 3: pivot pairs into a wide matrix
packed AS (
  SELECT
    ticker_a,
    groupArray(ticker_b)          AS ks,
    groupArray(round(corr_1y, 3)) AS vs
  FROM pair
  GROUP BY ticker_a
)

-- Step 4: produce final output
SELECT
  p.ticker_a,
  arrayElement(p.vs, indexOf(p.ks, 'AAPL'))  AS AAPL,
  arrayElement(p.vs, indexOf(p.ks, 'AMD'))   AS AMD,
  arrayElement(p.vs, indexOf(p.ks, 'AMZN'))  AS AMZN,
  arrayElement(p.vs, indexOf(p.ks, 'GOOG'))  AS GOOG,
  arrayElement(p.vs, indexOf(p.ks, 'META'))  AS META,
  arrayElement(p.vs, indexOf(p.ks, 'MSFT'))  AS MSFT,
  arrayElement(p.vs, indexOf(p.ks, 'NVDA'))  AS NVDA,
  arrayElement(p.vs, indexOf(p.ks, 'SOFI'))  AS SOFI,
  arrayElement(p.vs, indexOf(p.ks, 'TSLA'))  AS TSLA,
  arrayElement(p.vs, indexOf(p.ks, 'JNJ'))   AS JNJ,
  arrayElement(p.vs, indexOf(p.ks, 'MA'))    AS MA,
  arrayElement(p.vs, indexOf(p.ks, 'MCD'))   AS MCD,
  arrayElement(p.vs, indexOf(p.ks, 'WMT'))   AS WMT,
  arrayElement(p.vs, indexOf(p.ks, 'LMT'))   AS LMT,
  arrayElement(p.vs, indexOf(p.ks, 'INTU'))  AS INTU,
  arrayElement(p.vs, indexOf(p.ks, 'JPM'))   AS JPM,
  arrayElement(p.vs, indexOf(p.ks, 'UNH'))   AS UNH,
  arrayElement(p.vs, indexOf(p.ks, 'XOM'))   AS XOM,
  arrayElement(p.vs, indexOf(p.ks, 'MCK'))   AS MCK,
  arrayElement(p.vs, indexOf(p.ks, 'GE'))    AS GE,
  arrayElement(p.vs, indexOf(p.ks, 'COST'))  AS COST,
  arrayElement(p.vs, indexOf(p.ks, 'BRK-B')) AS "BRK-B",
  arrayElement(p.vs, indexOf(p.ks, 'PEP'))   AS PEP,
  arrayElement(p.vs, indexOf(p.ks, 'IBM'))   AS IBM,
  arrayElement(p.vs, indexOf(p.ks, 'CRM'))   AS CRM
FROM packed AS p
ORDER BY p.ticker_a