import logging
from functools import lru_cache
from cachetools import cached, TTLCache
import yfinance as yf
import requests

logger = logging.getLogger("dashboard.services.yahoo")

yf_session = requests.Session()
yf_session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
)

# Caches
info_cache = TTLCache(maxsize=100, ttl=3600)
finance_cache = TTLCache(maxsize=100, ttl=3600)


@lru_cache(maxsize=100)
def get_yf_ticker(symbol: str):
    return yf.Ticker(symbol, session=yf_session)


@cached(cache=info_cache)
def get_cached_info(symbol: str):
    try:
        return get_yf_ticker(symbol).info
    except Exception as e:
        logger.error(f"Yahoo Finance info error for {symbol}: {e}")
        return None


@cached(cache=finance_cache)
def get_cached_financials(symbol: str):
    try:
        return get_yf_ticker(symbol).financials
    except Exception as e:
        logger.error(f"Yahoo Finance financials error for {symbol}: {e}")
        return None
