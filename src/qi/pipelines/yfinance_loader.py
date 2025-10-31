from __future__ import annotations
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd

from qi.extract.yf_client import fetch_daily_history, fetch_short_name
from qi.transform.market_normalize import to_daily_prices_df
from qi.load.clickhouse_io import delete_window, insert_daily_prices

def _utc_now() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))

def backfill_arcx(tickers: Iterable[str], start: str = "1999-12-31") -> None:
    """
    One-time historical load for ARCX tickers.
    """
    batch_id = str(uuid.uuid4())
    for t in tickers:
        short = fetch_short_name(t)
        raw = fetch_daily_history(t, start=start, end=None)
        df = to_daily_prices_df(t, short, raw, ingested_at=_utc_now(), batch_id=batch_id)

        if df.empty:
            print(f"[backfill] {t}: no data")
            continue

        # Clean slate for this ticker/date range
        d1 = df["date"].min().strftime("%Y-%m-%d")
        d2 = df["date"].max().strftime("%Y-%m-%d")
        delete_window(t, d1, d2)
        insert_daily_prices(df)
        print(f"[backfill] {t}: {len(df):,} rows from {d1} to {d2}")

def refresh_arcx(tickers: Iterable[str], lookback_days: int = 21) -> None:
    """
    Weekly refresh: reload rolling window to cover holidays/dividends/splits.
    """
    batch_id = str(uuid.uuid4())
    end_dt = datetime.now(timezone.utc).date()
    start_dt = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    d1 = start_dt.strftime("%Y-%m-%d")
    d2 = end_dt.strftime("%Y-%m-%d")

    for t in tickers:
        short = fetch_short_name(t)
        raw = fetch_daily_history(t, start=d1, end=d2)
        df = to_daily_prices_df(t, short, raw, ingested_at=_utc_now(), batch_id=batch_id)

        # delete & insert the overlap window even if df is empty (safe no-op on insert)
        delete_window(t, d1, d2)
        if df.empty:
            print(f"[refresh] {t}: no rows in {d1}..{d2}")
            continue
        insert_daily_prices(df)
        print(f"[refresh] {t}: {len(df):,} rows in {d1}..{d2}")