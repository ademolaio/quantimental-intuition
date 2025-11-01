from __future__ import annotations
from data.tickers.INTERNATIONAL import ALL_INTL
from src.qi.pipelines.yfinance_loader import backfill_tickers

def main():
    # de-dupe, keep order
    tickers = list(dict.fromkeys(ALL_INTL))
    backfill_tickers(tickers, start="1999-12-31")

if __name__ == "__main__":
    main()