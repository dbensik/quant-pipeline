import logging
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Centralized Configuration Import ---
from config.settings import (
    URL_COINGECKO_API,
    DOWJONES_EXPECTED_COUNT,
    URL_DOWJONES_CONSTITUENTS,
    URL_NASDAQ100_WIKIPEDIA,
    URL_SP500_WIKIPEDIA,
)

logger = logging.getLogger(__name__)


class DynamicUniverse:
    """
    A class to fetch dynamic stock and crypto universes from various web sources.
    It provides a single interface to access multiple ticker lists.
    """

    def __init__(self, timeout: int = 10):
        """
        Initializes the DynamicUniverse fetcher.

        Args:
            timeout (int): The timeout in seconds for web requests.
        """
        self.session = requests.Session()
        # Wikipedia returns 403 to requests' default User-Agent, so every
        # constituent fetch failed and returned [] — which callers could not
        # distinguish from "this index is empty". Identifying the client is
        # what their robot policy asks for.
        self.session.headers.update(
            {
                "User-Agent": (
                    "quant-pipeline/1.0 (research tool; "
                    "https://github.com/dbensik/quant-pipeline)"
                )
            }
        )
        self.timeout = timeout
        # A mapping of source keys to their respective fetch methods.
        # Aliases included deliberately. api/routers/ingest.py advertised
        # "dow_jones" and "top_100_crypto" — names taken from the private
        # method names rather than these keys — so those two sources silently
        # returned [] and surfaced as a 503 "came back empty".
        self._source_map = {
            "sp500": self._fetch_sp500_tickers,
            "dowjones": self._fetch_dow_jones_tickers,
            "dow_jones": self._fetch_dow_jones_tickers,
            "nasdaq100": self._fetch_nasdaq100_tickers,
            "crypto": self._fetch_top_100_crypto_tickers,
            "top_100_crypto": self._fetch_top_100_crypto_tickers,
        }

    def get_tickers(self, source: str) -> List[str]:
        """
        Public method to get tickers from a specified source.

        Args:
            source (str): The source to fetch from.
                          Supported: 'sp500', 'dowjones', 'nasdaq100', 'crypto'.

        Returns:
            List[str]: A list of ticker symbols, or an empty list on failure.
        """
        fetch_function = self._source_map.get(source.lower())
        if fetch_function:
            logger.info(f"Fetching dynamic universe for source: '{source}'...")
            return fetch_function()
        else:
            logger.warning(f"Unsupported dynamic universe source: '{source}'")
            return []

    def _fetch_sp500_tickers(self) -> List[str]:
        """Fetches the list of S&P 500 tickers from Wikipedia."""
        try:
            response = self.session.get(URL_SP500_WIKIPEDIA, timeout=self.timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"id": "constituents"})
            if not table:
                logger.error(
                    "Could not find the constituents table on the S&P 500 Wikipedia page."
                )
                return []

            tickers = [
                row.find("td").text.strip()
                for row in table.find_all("tr")[1:]
                if row.find("td")
            ]
            logger.info(f"Successfully fetched {len(tickers)} S&P 500 tickers.")
            return tickers
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch S&P 500 tickers: {e}")
            return []
        except Exception as e:
            logger.error(f"An error occurred while parsing S&P 500 tickers: {e}")
            return []

    def _fetch_dow_jones_tickers(self) -> List[str]:
        """
        Fetch the 30 DJIA constituents.

        NOT from Wikipedia. That page carried a components table until about
        2026-08-16 and then became a navbox with no table at all, so the scrape
        returned empty every morning for ten days. Because universe snapshots
        cannot be backdated, those ten index-days are permanently lost. The
        source moved rather than the parsing breaking, so a column tweak would
        not have helped.

        Returns [] on ANY problem, which the caller treats as a failed source
        and refuses to record — an empty snapshot would assert the index has no
        members, which is worse than no snapshot at all.
        """
        try:
            # storage_options, because pd.read_html makes its own request and
            # never sees self.session's headers.
            tables = pd.read_html(
                URL_DOWJONES_CONSTITUENTS, storage_options=self.session.headers
            )
            dow_table = next((tbl for tbl in tables if "Symbol" in tbl.columns), None)
            if dow_table is None:
                logger.error(
                    "Could not find a table with a 'Symbol' column at %s. The "
                    "source page has changed shape — this needs a human, not a "
                    "retry.",
                    URL_DOWJONES_CONSTITUENTS,
                )
                return []

            tickers = [
                str(ticker).split(":")[-1].strip().upper()
                for ticker in dow_table["Symbol"].tolist()
                if str(ticker).strip() and str(ticker).lower() != "nan"
            ]

            # The Dow is 30 stocks by definition, so anything else means the
            # scrape picked up the wrong table or a partial one. A SHORT read is
            # the dangerous case: unlike an empty result it looks like a healthy
            # snapshot, and would quietly drop constituents from point-in-time
            # membership with nothing to indicate it.
            if len(tickers) != DOWJONES_EXPECTED_COUNT:
                logger.error(
                    "Dow Jones scrape returned %d tickers, expected %d (%s). "
                    "Refusing to report a constituent list that cannot be right.",
                    len(tickers),
                    DOWJONES_EXPECTED_COUNT,
                    URL_DOWJONES_CONSTITUENTS,
                )
                return []

            logger.info(f"Successfully fetched {len(tickers)} Dow Jones tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Could not fetch Dow Jones tickers: {e}")
            return []

    def _fetch_nasdaq100_tickers(self) -> List[str]:
        """Scrapes the Wikipedia page for NASDAQ-100 constituents."""
        try:
            tables = pd.read_html(
                URL_NASDAQ100_WIKIPEDIA, storage_options=self.session.headers
            )
            nasdaq_table = next(
                (tbl for tbl in tables if "Ticker" in tbl.columns), None
            )
            if nasdaq_table is None:
                logger.error(
                    "Could not find a table with 'Ticker' column for NASDAQ-100."
                )
                return []

            tickers = nasdaq_table["Ticker"].tolist()
            logger.info(f"Successfully fetched {len(tickers)} NASDAQ-100 tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Could not fetch NASDAQ-100 tickers: {e}")
            return []

    def _fetch_top_100_crypto_tickers(self) -> List[str]:
        """Fetches the top 100 cryptocurrencies by market cap from CoinGecko."""
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": "false",
        }
        try:
            response = self.session.get(
                URL_COINGECKO_API, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            tickers = []
            for item in data:
                symbol = item.get("symbol", "").upper()
                if not symbol:
                    continue
                clean_symbol = symbol.split("-")[0].split(" ")[0]
                tickers.append(f"{clean_symbol}-USD")

            logger.info(f"Successfully fetched {len(tickers)} crypto tickers.")
            return tickers
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch crypto tickers from CoinGecko: {e}")
            return []
        except Exception as e:
            logger.error(f"An error occurred while parsing crypto tickers: {e}")
            return []
