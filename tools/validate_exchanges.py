# tools/validate_exchanges.py
from __future__ import annotations
import re
import yfinance as yf


# --- Import your lists
from data.tickers.UNITED_STATES import NYSE, NASDAQ, ARCX, CBOE
from data.tickers.INTERNATIONAL import (
    NASDAQ_COPENHAGEN,
    NASDAQ_HELSINKI,
    NASDAQ_ICELAND,
    OSLO_BORS,
    NASDAQ_STOCKHOLM,
    XETRA,
    EURONEXT_PARIS,
    SIX,
    TSE,
    KRX,
    ASX,
    HKEX,
    NSE_INDIA,
)


# --- Known Yahoo anomalies (they return the wrong exchange but are correct listings)
ANOMALY_OK = {
    "PLTR": "NYSE",  # Yahoo shows NASDAQGS but it's listed on NYSE
    "SHOP": "NYSE",  # Yahoo shows NASDAQGS but it's dual-listed (NYSE/TSX)
}

# --- Expected exchange “fingerprints” (keywords we’ll look for in Yahoo’s fields)
# Yahoo returns either short codes (e.g., NMS, NYQ, PCX, NSI) or full names.
EXPECT_MAP = {
    "NYSE": {"NYSE", "NYQ", "NEW YORK STOCK EXCHANGE"},
    "NASDAQ": {"NASDAQ", "NMS", "NASDAQGS", "NASDAQGM", "NASDAQCM"},
    "ARCX": {"ARCA", "NYSE ARCA", "NYSEARCA", "PCX", "NASDAQGM"},
    "CBOE": {"CBOE", "CHICAGO OPTIONS", "CHICAGO BOARD OPTIONS EXCHANGE"},

    "NASDAQ_COPENHAGEN": {"COPENHAGEN", "CSE"},
    "NASDAQ_HELSINKI": {"HELSINKI", "HEL"},
    "NASDAQ_ICELAND": {"ICELAND"},
    "OSLO_BORS": {"OSLO", "OSE", "EURONEXT OSLO", "OSLO BORS", "OSL"},
    "NASDAQ_STOCKHOLM": {"STOCKHOLM", "STO"},
    "XETRA": {"XETRA"},
    "EURONEXT_PARIS": {"PARIS", "EURONEXT PARIS", "PAR"},
    "SIX": {"SWISS", "SIX", "SWX"},
    "TSE": {"TOKYO", "TSE"},
    "KRX": {"KOREA", "KRX", "KOE", "KSE", "KSC"},
    "ASX": {"ASX", "AUSTRALIAN"},
    "HKEX": {"HONG KONG", "HKEX", "HKG", "HKSE"},
    "NSE_INDIA": {"NSE", "NSEI", "NSI", "INDIA"},
}

# --- Build the expectation dict {ticker: group}
EXPECT: dict[str, str] = {}
EXPECT |= {t: "NYSE" for t in NYSE}
EXPECT |= {t: "NASDAQ" for t in NASDAQ}
EXPECT |= {t: "ARCX" for t in ARCX}
EXPECT |= {t: "CBOE" for t in CBOE}

EXPECT |= {t: "NASDAQ_COPENHAGEN" for t in NASDAQ_COPENHAGEN}
EXPECT |= {t: "NASDAQ_HELSINKI" for t in NASDAQ_HELSINKI}
EXPECT |= {t: "NASDAQ_ICELAND" for t in NASDAQ_ICELAND}
EXPECT |= {t: "OSLO_BORS" for t in OSLO_BORS}
EXPECT |= {t: "NASDAQ_STOCKHOLM" for t in NASDAQ_STOCKHOLM}
EXPECT |= {t: "XETRA" for t in XETRA}
EXPECT |= {t: "EURONEXT_PARIS" for t in EURONEXT_PARIS}
EXPECT |= {t: "SIX" for t in SIX}
EXPECT |= {t: "TSE" for t in TSE}
EXPECT |= {t: "KRX" for t in KRX}
EXPECT |= {t: "ASX" for t in ASX}
EXPECT |= {t: "HKEX" for t in HKEX}
EXPECT |= {t: "NSE_INDIA" for t in NSE_INDIA}

def normalize(s: str | None) -> str:
    if not s:
        return ""
    s = s.upper()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

_cache = {}
def actual_exchange(t):
    if t in _cache: return _cache[t]
    try:
        info = yf.Ticker(t).info
    except Exception:
        _cache[t] = ""
        return ""
    val = normalize(info.get("fullExchangeName") or info.get("exchange"))
    _cache[t] = val
    return val

def matches_expected(actual: str, expected_group: str) -> bool:
    if not actual:
        return False
    needles = EXPECT_MAP.get(expected_group, set())
    return any(n in actual for n in needles)

if __name__ == "__main__":
    problems = 0
    for t, group in sorted(EXPECT.items()):
        act = actual_exchange(t)

        # Handle known Yahoo anomalies
        if t in ANOMALY_OK and group == ANOMALY_OK[t] and "NASDAQGS" in act.upper():
            continue

        if not matches_expected(act, group):
            print(f"[WARN] {t:>10}: expected {group:<20} | got '{act or 'UNKNOWN'}'")
            problems += 1
    if problems == 0:
        print("All tickers match expected exchanges ✅")
    else:
        print(f"\n{problems} mismatches found.")