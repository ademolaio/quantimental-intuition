from __future__ import annotations
from data.tickers.ARCX import LISTINGS
from qi.pipelines.yfinance_loader import refresh_arcx

def main():
    tickers = list(dict.fromkeys(LISTINGS))
    refresh_arcx(tickers, lookback_days=21)

if __name__ == "__main__":
    main()