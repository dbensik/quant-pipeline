import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from curl_cffi import requests

# --- Centralized Configuration Import ---
from config.settings import (
    CACHE_DIR,
    CACHE_EXPIRY_HOURS,
    URL_COINGECKO_API,
    URL_DOWJONES_WIKIPEDIA,
    URL_NASDAQ100_WIKIPEDIA,
    URL_SP500_WIKIPEDIA,
)

# Setup logger for this module
logger = logging.getLogger(__name__)


class UniverseFetcher:
    """
    A class to fetch, NORMALIZE, and cache constituents for various financial universes.
    It handles caching, network requests, and parsing in a structured way.
    """

    # It's a static data structure used internally by this class's logic.
    CRYPTO_NORMALIZATION_MAP = {
        "BITCOIN": "BTC-USD",
        "BTC": "BTC-USD",
        "ETHEREUM": "ETH-USD",
        "ETH": "ETH-USD",
        "SOLANA": "SOL-USD",
        "SOL": "SOL-USD",
        "XRP": "XRP-USD",
        "CARDANO": "ADA-USD",
        "ADA": "ADA-USD",
        "DOGECOIN": "DOGE-USD",
        "DOGE": "DOGE-USD",
        "TETHER": "USDT-USD",
        "USDT": "USDT-USD",
        "BINANCECOIN": "BNB-USD",
        "BNB": "BNB-USD",
    }

    def __init__(
        self, cache_dir: str = CACHE_DIR, cache_expiry_hours: int = CACHE_EXPIRY_HOURS
    ):
        """Initializes the fetcher."""
        self.cache_path = Path(cache_dir)
        self.cache_expiry = timedelta(hours=cache_expiry_hours)
        self.session = requests.Session(impersonate="chrome")
        self.cache_path.mkdir(parents=True, exist_ok=True)

    # --- NEW: A single entry point for the dashboard to call ---
    def run(self, source: str) -> Tuple[List[str], Dict[str, Any]]:
        """
        Primary method to fetch a universe by name. It normalizes all tickers.

        Args:
            source: The name of the universe to fetch (e.g., 'S&P 500', 'Top Crypto').

        Returns:
            A tuple containing a list of unique, normalized tickers and a dictionary
            of associated metadata (e.g., sectors or market caps).
        """
        fetch_map = {
            "S&P 500": self.get_sp500,
            "Dow Jones": self.get_dow_jones,
            "Nasdaq 100": self.get_nasdaq_100,
            "Top Crypto": self.get_top_crypto_pairs,
        }
        fetch_function = fetch_map.get(source)
        if not fetch_function:
            logger.warning(f"Unknown universe source requested: '{source}'")
            return [], {}

        raw_tickers, metadata = fetch_function()

        # Normalize every ticker and then get a unique set
        normalized_tickers = [self._normalize_ticker(t) for t in raw_tickers]
        unique_tickers = sorted(list(set(normalized_tickers)))

        logger.info(
            f"Source '{source}': Fetched {len(raw_tickers)} raw tickers, returning {len(unique_tickers)} unique normalized tickers."
        )
        # Note: Metadata is not normalized/filtered here, as it's a simple lookup table.
        return unique_tickers, metadata

    def _normalize_ticker(self, ticker: str) -> str:
        """
        Normalizes a ticker to a standard format using the class's map.
        - Equities (e.g., 'AAPL') are uppercased.
        - Crypto names/symbols (e.g., 'bitcoin', 'eth') are mapped to 'SYMBOL-USD'.
        """
        ticker_upper = str(ticker).upper()

        # Check if it's a crypto that needs mapping
        if ticker_upper in self.CRYPTO_NORMALIZATION_MAP:
            return self.CRYPTO_NORMALIZATION_MAP[ticker_upper]

        # Check if it's already in a standard crypto format
        if "-" in ticker_upper and ticker_upper.endswith(("-USD", "-USDT")):
            return ticker_upper

        # Assume it's an equity or other standard ticker
        return ticker_upper

    def _get_cached_or_fetch(
        self, cache_filename: str, fetch_func: Callable[[], Optional[Dict]]
    ) -> Optional[Dict]:
        """A generic method to handle caching logic."""
        cache_file = self.cache_path / cache_filename
        if cache_file.exists():
            file_mod_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            if datetime.now() - file_mod_time < self.cache_expiry:
                try:
                    logger.info(f"Loading constituents from cache: '{cache_filename}'")
                    return json.loads(cache_file.read_text())
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(
                        f"Could not read cache file '{cache_filename}': {e}. Re-fetching."
                    )
            else:
                logger.info(f"Cache file '{cache_filename}' is expired. Re-fetching.")
        data = fetch_func()
        if data:
            try:
                cache_file.write_text(json.dumps(data, indent=4))
                logger.info(f"Saved constituents to cache: '{cache_filename}'")
            except IOError as e:
                logger.error(f"Could not write to cache file '{cache_filename}': {e}")
        return data

    def _parse_wiki_table(
        self, url: str, match_keyword: str, ticker_col: str, sector_col: str
    ) -> Optional[Dict]:
        """A robust, reusable function to parse constituent tables from Wikipedia."""
        try:
            response_text = self.session.get(url).text
            html_io = io.StringIO(response_text)
            tables = pd.read_html(html_io, match=match_keyword)
            df = tables[0]
            df.rename(
                columns={ticker_col: "Ticker", sector_col: "Sector"}, inplace=True
            )
            df["Ticker"] = df["Ticker"].str.replace(".", "-", regex=False)
            df["Ticker"] = df["Ticker"].str.replace("NYSE:\s*", "", regex=True)
            tickers = df["Ticker"].tolist()
            sectors = (
                pd.Series(df.Sector.values, index=df.Ticker).to_dict()
                if "Sector" in df
                else {}
            )
            logger.info(
                f"✅ Successfully fetched {len(tickers)} constituents from {url.split('/')[-1]}."
            )
            return {"tickers": tickers, "sectors": sectors}
        except Exception as e:
            logger.exception(f"❌ Failed to fetch or parse table from {url}: {e}")
            return None

    def _fetch_and_parse(
        self,
        cache_filename: str,
        fetch_func: Callable,
        primary_key: str,
        secondary_key: str,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """A helper method that encapsulates the fetch-cache-parse pattern."""
        data = self._get_cached_or_fetch(cache_filename, fetch_func)
        if not data:
            return [], {}
        return data.get(primary_key, []), data.get(secondary_key, {})

    def get_sp500(self) -> Tuple[List[str], Dict[str, str]]:
        """Public method to get S&P 500 constituents."""
        return self._fetch_and_parse(
            "sp500_constituents.json",
            lambda: self._parse_wiki_table(
                URL_SP500_WIKIPEDIA, "Security", "Symbol", "GICS Sector"
            ),
            "tickers",
            "sectors",
        )

    def get_dow_jones(self) -> Tuple[List[str], Dict[str, str]]:
        """Public method to get Dow Jones Industrial Average constituents."""
        return self._fetch_and_parse(
            "dowjones_constituents.json",
            lambda: self._parse_wiki_table(
                URL_DOWJONES_WIKIPEDIA, "Company", "Symbol", "Industry"
            ),
            "tickers",
            "sectors",
        )

    def get_nasdaq_100(self) -> Tuple[List[str], Dict[str, str]]:
        """Public method to get NASDAQ-100 constituents."""
        return self._fetch_and_parse(
            "nasdaq100_constituents.json",
            lambda: self._parse_wiki_table(
                URL_NASDAQ100_WIKIPEDIA, "Ticker", "Ticker", "GICS Sector"
            ),
            "tickers",
            "sectors",
        )

    def get_top_crypto_pairs(
        self, vs_currency: str = "usd", top_n: int = 100
    ) -> Tuple[List[str], Dict[str, float]]:
        """Public method to get top N crypto pairs by market cap."""
        cache_filename = f"crypto_top_{top_n}_{vs_currency}.json"
        return self._fetch_and_parse(
            cache_filename,
            lambda: self._fetch_top_crypto(vs_currency, top_n),
            "tickers",
            "market_caps",
        )

    def _fetch_top_crypto(self, vs_currency: str, top_n: int) -> Optional[Dict]:
        """Private method with the logic to fetch crypto data from CoinGecko."""
        logger.info(f"Fetching top {top_n} crypto pairs from CoinGecko...")
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": top_n,
            "page": 1,
            "sparkline": "false",
        }
        try:
            response = self.session.get(URL_COINGECKO_API, params=params, timeout=15)
            response.raise_for_status()
            api_data = response.json()
            if not api_data:
                logger.warning("CoinGecko API returned no data.")
                return None
            # The API returns the 'symbol' (e.g., 'btc') which we use as the raw ticker
            tickers = [item["symbol"] for item in api_data]
            market_caps = {
                item["symbol"]: item.get("market_cap", 0) for item in api_data
            }
            logger.info(f"✅ Successfully fetched {len(tickers)} crypto pairs.")
            return {"tickers": tickers, "market_caps": market_caps}
        except requests.errors.RequestsError as e:
            logger.error(f"❌ Failed to fetch crypto data from CoinGecko: {e}")
            return None
        except Exception as e:
            logger.exception(
                f"❌ An unexpected error occurred while processing crypto data: {e}"
            )
            return None
