# data/tickers/__init__.py
from .UNITED_STATES import ALL_US, EXCHANGE_MAP as US_EXCHANGE_MAP
from .INTERNATIONAL import ALL_INTL

ALL_TICKERS = list(dict.fromkeys(ALL_US + ALL_INTL))

def get_exchange(ticker: str) -> str | None:
    return US_EXCHANGE_MAP.get(ticker)