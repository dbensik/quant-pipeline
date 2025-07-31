from abc import ABC, abstractmethod

import pandas as pd


class BaseDataFetcher(ABC):
    """
    Abstract base class for all data fetchers.

    Defines a consistent interface for fetching time-series data from various
    sources (e.g., yfinance, Alpaca, a private database). This allows the main
    pipeline to be source-agnostic.
    """

    @abstractmethod
    def fetch_data(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        granularity: str = "1d",
    ) -> pd.DataFrame:
        """
        Fetches historical data for a list of tickers.

        Args:
            tickers (list[str]): The list of symbols to fetch.
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
            granularity (str): The data interval (e.g., '1d' for daily, '1h' for hourly).

        Returns:
            pd.DataFrame: A DataFrame in long format with columns [Date, Ticker, Open, High, Low, Close, Volume].
                          The 'Date' column should be a timezone-aware DatetimeIndex.
        """
        pass
