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


def _today_plus_one_str() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

def fetch_daily_history(ticker: str, start: str | None = None, end: str | None = None,
                        force_range: bool = False) -> pd.DataFrame:
    """
    Return 1d OHLCV. If force_range=True, ALWAYS fetch exactly [start, end] using yf.download.
    Otherwise try fast paths but clip to [start, end] if provided.
    Normalizes:
      - tz-aware index -> UTC naive
      - MultiIndex columns from yf.download -> single level: ['Open','High','Low','Close','Adj Close','Volume']
    """
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        # If yf returned MultiIndex columns (e.g., ('Adj Close','SPY')), drop the ticker level
        if isinstance(df.columns, pd.MultiIndex):
            # keep the first level with field names: 'Open','High','Low','Close','Adj Close','Volume'
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        # Ensure expected field names exist; some paths may title-case differently
        rename_map = {
            "Adj Close": "Adj Close",
            "Adj close": "Adj Close",
            "AdjClose": "Adj Close",
            "Open": "Open", "High": "High", "Low": "Low", "Close": "Close", "Volume": "Volume",
        }
        df = df.rename(columns=rename_map)

        # Normalize index to tz-naive UTC date-times
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)

        # Keep only the columns we expect; ignore anything extra
        keep = [c for c in ["Open","High","Low","Close","Adj Close","Volume"] if c in df.columns]
        return df[keep] if keep else pd.DataFrame()

    def _clip(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        s = pd.Timestamp(start) if start else None
        e = pd.Timestamp(end) if end else None
        # make end inclusive by adding +1 day for slicing
        e_inclusive = (e + pd.Timedelta(days=1)) if e is not None else None

        idx = pd.to_datetime(df.index)
        mask = pd.Series(True, index=df.index)
        if s is not None:
            mask &= idx >= s
        if e_inclusive is not None:
            mask &= idx < e_inclusive
        return df.loc[mask]

    def _fetch_range(tkr: str, s: str | None, e: str | None) -> pd.DataFrame:
        e = e or _today_plus_one_str()
        df = yf.download(
            tkr, start=s, end=e, interval="1d",
            auto_adjust=False, progress=False, threads=False
        )
        return _clip(_normalize(df))

    if force_range:
        # Always bound the window
        return _fetch_range(ticker, start or "1999-12-31", end)

    # Fast path
    try:
        df = yf.Ticker(ticker).history(period="max", interval="1d", auto_adjust=False)
        df = _clip(_normalize(df))
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # Fallbacks
    try:
        df = yf.download(ticker, period="max", interval="1d",
                         auto_adjust=False, progress=False, threads=False)
        return _clip(_normalize(df))
    except Exception:
        pass

    try:
        return _fetch_range(ticker, start, end)
    except Exception:
        return pd.DataFrame()