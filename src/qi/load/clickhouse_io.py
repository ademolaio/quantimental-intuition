from __future__ import annotations
import pandas as pd
from typing import Iterable
from qi.config import make_clickhouse_client

DAILY_TABLE = "market.daily_prices"

def delete_window(ticker: str, start_date: str, end_date: str) -> None:
    """
    Hard-delete an overlap window for idempotent reloads.
    Dates must be 'YYYY-MM-DD'.
    """
    client = make_clickhouse_client()
    q = f"""
        ALTER TABLE {DAILY_TABLE}
        DELETE WHERE ticker = %(ticker)s AND date BETWEEN %(d1)s AND %(d2)s
    """
    client.command(q, parameters={"ticker": ticker, "d1": start_date, "d2": end_date})

def insert_daily_prices(df: pd.DataFrame) -> None:
    """
    Bulk insert DataFrame into market.daily_prices using HTTP client.
    """
    if df is None or df.empty:
        return
    client = make_clickhouse_client()
    # Column order must match table
    column_order = [
        "ticker", "short_name", "date",
        "open", "high", "low", "close", "adj_close", "volume",
        "ingested_at", "batch_id", "source",
    ]
    # clickhouse-connect will map pandas dtypes reasonably; ensure date/time are proper
    client.insert_df(table=DAILY_TABLE, df=df[column_order], column_names=column_order)