from __future__ import annotations
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone

def _today_plus_one_str() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")


def fetch_short_name(ticker: str) -> str:
    """
    Best-effort display name from Yahoo. Falls back to longName, then symbol.
    """
    try:
        info = yf.Ticker(ticker).info  # may be slow but includes names
        name = info.get("shortName") or info.get("longName") or info.get("symbol")
        return name or ticker
    except Exception:
        return ticker

def fetch_daily_history(ticker: str, start: str = "1999-12-31", end: str | None = None) -> pd.DataFrame:
    """
    Robust daily history fetch for 1d bars with fallbacks:
    1) Ticker.history(period='max')
    2) download(period='max')
    3) download(start, end=today+1)
    Returns DataFrame indexed by Datetime with columns:
    Open, High, Low, Close, Adj Close, Volume
    """
    # 1) fast, reliable for many symbols
    try:
        df = yf.Ticker(ticker).history(period="max", interval="1d", auto_adjust=False)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # 2) legacy path
    try:
        df = yf.download(ticker, period="max", interval="1d", auto_adjust=False, progress=False, threads=False)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # 3) explicit date range; end must be > last trading day, so use today+1
    try:
        end = end or _today_plus_one_str()
        df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False, threads=False)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # Nothing worked
    return pd.DataFrame()