# src/qi/pipelines/refresh_fundamentals.py
from __future__ import annotations
import time
from datetime import datetime
from typing import Iterable, Literal
import os
import sys
import pandas as pd
import yfinance as yf
import clickhouse_connect

from data.tickers.UNITED_STATES import ALL_US as US_TICKERS
from data.tickers.INTERNATIONAL import ALL_INTL as INTL_TICKERS

Period = Literal["Q","A"]

def _melt_fin_df(df: pd.DataFrame, ticker: str, period: Period, currency: str, rename_map: dict[str,str]|None=None) -> pd.DataFrame:
    """
    yfinance DataFrame: rows=metrics, cols=fiscal dates (Timestamp)
    -> long: ticker, fiscal_date, period, metric, value, currency
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker","fiscal_date","period","metric","value","currency"])
    # Rows = metrics, Columns = datetimes
    df = df.copy()
    if rename_map:
        df.index = [rename_map.get(i, i) for i in df.index]
    long = (
        df.reset_index()
          .melt(id_vars="index", var_name="fiscal_date", value_name="value")
          .rename(columns={"index":"metric"})
    )
    long["ticker"] = ticker
    long["period"] = period
    long["currency"] = currency or ""
    # ensure date
    long["fiscal_date"] = pd.to_datetime(long["fiscal_date"]).dt.date
    # drop NAs
    long = long.dropna(subset=["value"])
    # reorder
    long["source"] = "yfinance"
    long["loaded_at"] = pd.Timestamp.utcnow()
    return long[["ticker","fiscal_date","period","metric","value","currency","source", "loaded_at"]]

def _fetch_all_frames(ticker: str):
    t = yf.Ticker(ticker)
    info = t.info or {}
    currency = (info.get("currency") or info.get("financialCurrency") or "")  # YF varies
    # Income
    inc_a = t.financials           # annual income statement
    inc_q = t.quarterly_financials
    # Balance
    bs_a  = t.balance_sheet
    bs_q  = t.quarterly_balance_sheet
    # Cashflow
    cf_a  = t.cashflow
    cf_q  = t.quarterly_cashflow
    return currency, inc_a, inc_q, bs_a, bs_q, cf_a, cf_q

def _insert_df(client, table: str, df: pd.DataFrame):
    if df.empty:
        return
    client.insert_df(table, df)


def _uniq(seq):
    return list(dict.fromkeys(seq))

def run_refresh(tickers: Iterable[str] = None, sleep_s: float = 0.7):
    """
       Refresh latest fundamentals for a list of tickers.
       If no list is provided, run for BOTH US and INTL.
       """
    if tickers is None:
        tickers = _uniq(list(US_TICKERS) + list(INTL_TICKERS))
    else:
        tickers = _uniq(list(tickers))

    client = clickhouse_connect.get_client(
        host=os.getenv("CH_HOST","localhost"),
        port=int(os.getenv("CH_PORT","8123")),
        username=os.getenv("CLICKHOUSE_USER","default"),
        password=os.getenv("CLICKHOUSE_PASSWORD",""),
        database=os.getenv("CLICKHOUSE_DB","default"),
        send_receive_timeout=60,
    )

    for i, tk in enumerate(tickers, 1):
        try:
            currency, inc_a, inc_q, bs_a, bs_q, cf_a, cf_q = _fetch_all_frames(tk)

            df_inc_a = _melt_fin_df(inc_a, tk, "A", currency)
            df_inc_q = _melt_fin_df(inc_q, tk, "Q", currency)
            df_bs_a  = _melt_fin_df(bs_a,  tk, "A", currency)
            df_bs_q  = _melt_fin_df(bs_q,  tk, "Q", currency)
            df_cf_a  = _melt_fin_df(cf_a,  tk, "A", currency)
            df_cf_q  = _melt_fin_df(cf_q,  tk, "Q", currency)

            # Income
            frames = [df_inc_a, df_inc_q]
            frames = [f for f in frames if f is not None and not f.empty]
            if frames:
                _insert_df(client, "fundamentals.income_statement", pd.concat(frames, ignore_index=True))

            # Balance
            frames = [df_bs_a, df_bs_q]
            frames = [f for f in frames if f is not None and not f.empty]
            if frames:
                _insert_df(client, "fundamentals.balance_sheet", pd.concat(frames, ignore_index=True))

            # Cashflow
            frames = [df_cf_a, df_cf_q]
            frames = [f for f in frames if f is not None and not f.empty]
            if frames:
                _insert_df(client, "fundamentals.cashflow_statement", pd.concat(frames, ignore_index=True))

            if i % 25 == 0:
                print(f"[funds] {i}/{len(tickers)} …")
        except Exception as e:
            print(f"[funds][WARN] {tk}: {e}")
        time.sleep(sleep_s)