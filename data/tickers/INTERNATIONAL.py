# data/tickers/INTERNATIONAL.py
# data/tickers/INTERNATIONAL.py
def _uniq(seq):
    return list(dict.fromkeys(seq))

NASDAQ_COPENHAGEN = ["NOVO-B.CO", "DANSKE.CO", "CARL-B.CO", "VWS.CO"]
NASDAQ_HELSINKI   = ["NOKIA.HE", "KNEBV.HE", "UPM.HE", "OUT1V.HE", "FORTUM.HE"]
NASDAQ_ICELAND    = []  # Yahoo doesn’t provide SIMINN or BRIM
OSLO_BORS         = ["EQNR.OL", "NHY.OL", "TEL.OL", "ORK.OL", "DNB.OL"]
NASDAQ_STOCKHOLM  = ["VOLV-B.ST", "ERIC-B.ST", "ATCO-A.ST", "SAND.ST"]
XETRA             = ["SAP.DE", "SIE.DE", "VOW.DE", "ALV.DE", "DTE.DE"]
EURONEXT_PARIS    = ["MC.PA", "OR.PA"]
SIX               = ["NESN.SW", "ROG.SW"]
TSE               = ["7203.T", "6758.T", "9432.T"]
KRX               = ["005930.KS"]
ASX               = ["BHP.AX"]
HKEX              = ["0700.HK"]
NSE_INDIA         = ["RELIANCE.NS"]

ALL_INTL = _uniq(
    NASDAQ_COPENHAGEN + NASDAQ_HELSINKI + NASDAQ_ICELAND +
    OSLO_BORS + NASDAQ_STOCKHOLM + XETRA + EURONEXT_PARIS +
    SIX + TSE + KRX + ASX + HKEX + NSE_INDIA
)

# data/tickers/INTERNATIONAL.py
EXCHANGE_MAP_INTL = {}
for t in NASDAQ_COPENHAGEN: EXCHANGE_MAP_INTL.setdefault(t, "NASDAQ COPENHAGEN")
for t in NASDAQ_HELSINKI:   EXCHANGE_MAP_INTL.setdefault(t, "NASDAQ HELSINKI")
for t in NASDAQ_ICELAND:    EXCHANGE_MAP_INTL.setdefault(t, "NASDAQ ICELAND")
for t in OSLO_BORS:         EXCHANGE_MAP_INTL.setdefault(t, "OSLO BØRS")
for t in NASDAQ_STOCKHOLM:  EXCHANGE_MAP_INTL.setdefault(t, "NASDAQ STOCKHOLM")
for t in XETRA:             EXCHANGE_MAP_INTL.setdefault(t, "XETRA")
for t in EURONEXT_PARIS:    EXCHANGE_MAP_INTL.setdefault(t, "EURONEXT PARIS")
for t in SIX:               EXCHANGE_MAP_INTL.setdefault(t, "SIX SWISS")
for t in TSE:               EXCHANGE_MAP_INTL.setdefault(t, "TSE (JP)")
for t in KRX:               EXCHANGE_MAP_INTL.setdefault(t, "KRX")
for t in ASX:               EXCHANGE_MAP_INTL.setdefault(t, "ASX")
for t in HKEX:              EXCHANGE_MAP_INTL.setdefault(t, "HKEX")
for t in NSE_INDIA:         EXCHANGE_MAP_INTL.setdefault(t, "NSE INDIA")