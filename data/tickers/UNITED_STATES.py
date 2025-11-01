# data/tickers/UNITED_STATES.py

def _uniq(seq):
    return list(dict.fromkeys(seq))

NYSE = [
    "JNJ","LLY","MA","MCD","WMT","LMT","JPM","UNH","XOM","MCK","GE","BRK-B",
    "GM","T","K","F","DAL","GS","MSI","V","CAT","SHW","IBM","HD","BA",
    "MMM","DIS","PG","CVX","MRK","VZ","KO","AXP","TRV","NKE","BB",
    "TOST","FUBO","CHPT","SNAP","PLTR","LMND",
    "CRM","ORCL","NOW","SHOP",
]

NASDAQ = [
    "AAPL","MSFT","AMZN","GOOGL","GOOG","META","NVDA","TSLA",
    "AVGO","AMD","INTC","AMAT","LRCX","QCOM",
    "ADBE","INTU",
    "NFLX","PEP","SBUX","COST","PYPL",
    "AMGN","GILD","REGN",
    "CSX","ADP","MAR",
    "QQQ","IEI","HON"
]

ARCX = [
    "SPY","IWM","DIA","XLB","XLC","XLE","XLF","XLI","XLK","XLP","XLRE","XLU","XLV","XLY",
    "XHB","GLD","SLV","UNG","DBA","DBC","USO","CPER","SGOV","SHY","IEF","TLT",
]

CBOE = ["^VIX"]

ALL_US = _uniq(NYSE + NASDAQ + ARCX + CBOE)

# Optional: a reverse lookup (first match wins because of order)
EXCHANGE_MAP = {}
for t in NYSE:   EXCHANGE_MAP.setdefault(t, "NYSE")
for t in NASDAQ: EXCHANGE_MAP.setdefault(t, "NASDAQ")
for t in ARCX:   EXCHANGE_MAP.setdefault(t, "ARCX")
for t in CBOE: EXCHANGE_MAP.setdefault(t, "CBOE")