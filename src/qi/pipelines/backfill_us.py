from __future__ import annotations
from data.tickers.UNITED_STATES import ALL_US
from src.qi.pipelines.yfinance_loader import backfill_tickers

def main():
    # de-dupe, keep order
    tickers = list(dict.fromkeys(ALL_US))
    backfill_tickers(tickers, start="1999-12-31")

if __name__ == "__main__":
    main()