# scripts/crypto_meta.py

import requests
import logging
from typing import List, Dict
import sys, os

logger = logging.getLogger(__name__)
# add project root to sys.path for local script imports
# sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))
# from .crypto_pipeline import _CRYPTO_ID_MAP  
# from data_pipeline.crypto_pipeline import _CRYPTO_ID_MAP
# Map simple symbols to CoinGecko IDs (override as needed)
_CRYPTO_ID_MAP = {
    'btc': 'bitcoin',
    'eth': 'ethereum',
    'ltc': 'litecoin',
    'xrp': 'ripple',
    # add any others you need...
}

_COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"

def load_crypto_meta(pairs, vs_currency="usd"):
    """
    Fetch market cap (and other meta) for each pair without pulling in crypto_pipeline.
    """
    id_map = {}
    for pair in pairs:
        symbol = pair.split("-")[0].lower()
        coin_id = _CRYPTO_ID_MAP.get(symbol, symbol)
        id_map[coin_id] = pair

    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(id_map),
        "order": "market_cap_desc",
        "per_page": len(id_map),
        "page": 1,
        "sparkline": "false"
    }

    try:
        resp = requests.get(_COINGECKO_MARKETS_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch crypto meta: {e}")
        # fallback to Nones
        return {pair: {"market_cap": None} for pair in pairs}

    result = {}
    for coin in data:
        pair = id_map.get(coin["id"])
        if pair:
            result[pair] = {
                "market_cap": coin.get("market_cap"),
                "current_price": coin.get("current_price"),
                "name": coin.get("name"),
                "symbol": coin.get("symbol"),
            }

    # fill in any missing
    for pair in pairs:
        result.setdefault(pair, {"market_cap": None})

    return result

def _yf_fallback(pair: str) -> Dict:
    """
    Use yfinance to pull market cap if CoinGecko fails.
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker(pair)
        info = ticker.info or {}
        mcap = info.get('marketCap')
        return {
            'id': pair.lower(),
            'market_cap': mcap
        }
    except Exception as e:
        logger.error(f"yfinance fallback failed for {pair}: {e}")
        return {
            'id': pair.lower(),
            'market_cap': None
        }


if __name__ == "__main__":
    # quick test
    logging.basicConfig(level=logging.INFO)
    meta = load_crypto_meta(["BTC-USD", "ETH-USD"])
    for p, d in meta.items():
        print(f"{p}: id={d['id']}  market_cap=${d['market_cap']:,}")
