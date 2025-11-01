from __future__ import annotations
import pandas as pd

DAILY_COLUMNS = [
    "ticker", "short_name", "date",
    "open", "high", "low", "close", "adj_close", "volume",
    "ingested_at", "batch_id", "source",
]

def _empty_series(dtype: str, index) -> pd.Series:
    return pd.Series([None] * len(index), index=index, dtype=dtype)

def _ensure_series(obj, index, dtype: str) -> pd.Series:
    """
    Make sure we always handle a 1-D Series with the given index/dtype.
    Handles scalars, numpy arrays, Index objects, DataFrames (takes first col), etc.
    """
    if isinstance(obj, pd.Series):
        s = obj
    elif isinstance(obj, pd.DataFrame):
        # Take the first column if a DataFrame sneaks in (duplicate names / wide shapes)
        s = obj.iloc[:, 0] if obj.shape[1] else _empty_series(dtype, index)
    else:
        # Anything else (scalar, list, array, Index, etc.)
        s = pd.Series(obj, index=index) if obj is not None else _empty_series(dtype, index)

    # Align index and cast
    if not s.index.equals(index):
        s = s.reindex(index)
    try:
        return s.astype(dtype)
    except Exception:
        return pd.to_numeric(s, errors="coerce").astype(dtype)

def to_daily_prices_df(
    ticker: str,
    short_name: str,
    raw: pd.DataFrame,
    ingested_at,
    batch_id: str,
    source: str = "yfinance",
) -> pd.DataFrame:
    """
    Convert yfinance DF (Open/High/Low/Close/Adj Close/Volume, DatetimeIndex)
    → contract columns with correct dtypes. Robust to missing/odd-shaped inputs.
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    # Normalize expected columns
    df = raw.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    ).copy()

    # Flatten any MultiIndex columns defensively
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["__".join([str(x) for x in tup if x is not None]) for tup in df.columns]

    # Build a fresh frame with the exact columns we want
    out = pd.DataFrame(index=df.index)

    # Date (from index)
    idx = out.index
    out["date"] = pd.to_datetime(idx).tz_localize(None).date

    # Price columns (force Series, coerce to float64)
    for c in ["open", "high", "low", "close", "adj_close"]:
        out[c] = _ensure_series(df[c] if c in df.columns else None, out.index, "float64")

    # Volume (uint64)
    vol = _ensure_series(df["volume"] if "volume" in df.columns else None, out.index, "float64")
    out["volume"] = pd.to_numeric(vol, errors="coerce").fillna(0).astype("uint64")

    # Metadata
    out["ticker"] = ticker
    out["short_name"] = short_name or ticker
    out["ingested_at"] = ingested_at
    out["batch_id"] = batch_id
    out["source"] = source

    # Reorder columns
    out = out[
        [
            "ticker", "short_name", "date",
            "open", "high", "low", "close", "adj_close", "volume",
            "ingested_at", "batch_id", "source",
        ]
    ]

    # Drop rows with no date, and drop rows where *all* price fields are NaN
    out = out.dropna(subset=["date"])
    out = out.dropna(how="all", subset=["open", "high", "low", "close", "adj_close"]).reset_index(drop=True)

    return out