from __future__ import annotations
from data.tickers.ARCX import LISTINGS
from qi.pipelines.yfinance_loader import backfill_arcx

def main():
    # de-dupe, keep order
    tickers = list(dict.fromkeys(LISTINGS))
    backfill_arcx(tickers, start="1999-12-31")

if __name__ == "__main__":
    main()