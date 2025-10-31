from __future__ import annotations
from data.tickers.ARCX import LISTINGS
from qi.pipelines.yfinance_loader import refresh_arcx

def run_refresh():
    """
    Unified entrypoint used by both Airflow and manual CLI.
    Refreshes all ARCX tickers with a 21-day overlap.
    """
    tickers = list(dict.fromkeys(LISTINGS))
    refresh_arcx(tickers, lookback_days=21)

if __name__ == "__main__":
    run_refresh()