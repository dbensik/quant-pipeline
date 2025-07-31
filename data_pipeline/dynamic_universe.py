import logging
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Centralized Configuration Import ---
from config.settings import (
    URL_COINGECKO_API,
    URL_DOWJONES_WIKIPEDIA,
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
        self.timeout = timeout
        # A mapping of source keys to their respective fetch methods.
        self._source_map = {
            "sp500": self._fetch_sp500_tickers,
            "dowjones": self._fetch_dow_jones_tickers,
            "nasdaq100": self._fetch_nasdaq100_tickers,
            "crypto": self._fetch_top_100_crypto_tickers,
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
        """Scrapes the Wikipedia page for Dow Jones Industrial Average constituents."""
        try:
            tables = pd.read_html(URL_DOWJONES_WIKIPEDIA)
            dow_table = next((tbl for tbl in tables if "Symbol" in tbl.columns), None)
            if dow_table is None:
                logger.error(
                    "Could not find a table with 'Symbol' column for Dow Jones."
                )
                return []

            tickers = [
                str(ticker).split(":")[-1].strip()
                for ticker in dow_table["Symbol"].tolist()
            ]
            logger.info(f"Successfully fetched {len(tickers)} Dow Jones tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Could not fetch Dow Jones tickers: {e}")
            return []

    def _fetch_nasdaq100_tickers(self) -> List[str]:
        """Scrapes the Wikipedia page for NASDAQ-100 constituents."""
        try:
            tables = pd.read_html(URL_NASDAQ100_WIKIPEDIA)
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
