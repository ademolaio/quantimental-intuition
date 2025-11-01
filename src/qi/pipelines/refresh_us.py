from __future__ import annotations
from data.tickers.UNITED_STATES import ALL_US
from src.qi.pipelines.yfinance_loader import refresh_tickers

def run_refresh():
    """
    Unified entrypoint used by both Airflow and manual CLI.
    Refreshes all ARCX tickers with a 21-day overlap.
    """
    tickers = list(dict.fromkeys(ALL_US))
    for ticker in tickers:
        refresh_tickers(ticker)
    refresh_tickers(tickers, lookback_days=21)

if __name__ == "__main__":
    run_refresh()