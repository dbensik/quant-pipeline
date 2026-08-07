import logging
import sqlite3
from typing import Dict, List

import pandas as pd

# --- Centralized Configuration Import ---
from config.settings import DB_PATH, DB_PRICE_TABLE

# Setup logger for this module
logger = logging.getLogger(__name__)


def _inclusive_end(end_date: str) -> str:
    """
    Extend a date-only `end_date` to the end of that day.

    `Timestamp` is stored as the TEXT 'YYYY-MM-DD HH:MM:SS', so SQLite compares
    it lexically. Against a date-only bound that makes
    '2023-03-31 00:00:00' <= '2023-03-31' evaluate to False, silently dropping
    the final day of every requested range — the most recent bar, for any query
    ending today. Callers pass date-only strings throughout the dashboard, so
    normalise here rather than at each call site.
    """
    return f"{end_date} 23:59:59" if len(end_date.strip()) == 10 else end_date


class PriceDataHandler:
    """
    A dedicated class for fetching historical price data from the database.
    It acts as the bridge between the application's data needs and the price table.
    """

    def __init__(self, db_path: str = DB_PATH):
        """Initializes the handler with the path to the database."""
        self.db_path = db_path
        # Schema changed to price_data_daily
        self.table_name = "price_data_daily"

    def get_prices(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetches historical 'Close' price data for a list of tickers.

        Returns:
            A pandas DataFrame where the index is the date and each column
            represents the 'Close' price for a ticker.
        """
        if not tickers:
            return pd.DataFrame()

        placeholders = ", ".join("?" for _ in tickers)
        sql = f"""
        SELECT Timestamp, Ticker, "Close"
        FROM {self.table_name}
        WHERE Ticker IN ({placeholders})
        AND Timestamp BETWEEN ? AND ?
        ORDER BY Timestamp ASC;
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                params = tickers + [start_date, _inclusive_end(end_date)]
                df = pd.read_sql_query(
                    sql, conn, params=params, index_col="Timestamp", parse_dates=["Timestamp"]
                )

            if df.empty:
                logger.warning(
                    f"No 'Close' price data found for tickers {tickers} in the given date range."
                )
                return pd.DataFrame()

            price_df = df.pivot(columns="Ticker", values="Close")
            return price_df
        except Exception as e:
            logger.exception(
                f"An unexpected error occurred while fetching close prices: {e}"
            )
            return pd.DataFrame()

    def get_full_data_for_tickers(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetches full OHLCV data for a list of tickers and returns a dictionary of DataFrames.
        This is used by the backtesting and screening modules.
        """
        if not tickers:
            return {}

        placeholders = ", ".join("?" for _ in tickers)
        sql = f"""
        SELECT *
        FROM {self.table_name}
        WHERE Ticker IN ({placeholders})
        AND Timestamp BETWEEN ? AND ?
        ORDER BY Timestamp ASC;
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                params = tickers + [start_date, _inclusive_end(end_date)]
                df = pd.read_sql_query(
                    sql, conn, params=params, index_col="Timestamp", parse_dates=["Timestamp"]
                )

            if df.empty:
                logger.warning(
                    f"No full OHLCV data found for tickers {tickers} in the given date range."
                )
                return {}

            # Split the single DataFrame into a dictionary of DataFrames, one per ticker
            data_dict = {
                ticker: group.drop(columns="Ticker")
                for ticker, group in df.groupby("Ticker")
            }
            return data_dict
        except Exception as e:
            logger.exception(
                f"An unexpected error occurred while fetching full data: {e}"
            )
            return {}
