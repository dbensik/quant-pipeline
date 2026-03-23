import logging
import sqlite3
from typing import Protocol

import numpy as np
import pandas as pd

from config.settings import DB_NORMALIZED_TABLE, DB_PRICE_TABLE

logger = logging.getLogger(__name__)


class PriceRepository(Protocol):
    def get_prices(self) -> pd.DataFrame:
        """Retrieve raw price data."""
        ...

    def save_normalized(self, df: pd.DataFrame) -> None:
        """Save normalized price data."""
        ...


class SqlitePriceRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.price_table = DB_PRICE_TABLE
        self.norm_table = DB_NORMALIZED_TABLE

    def get_prices(self) -> pd.DataFrame:
        logger.info(f"Reading data from '{self.price_table}' for normalization.")
        return pd.read_sql(
            f"SELECT Timestamp, Ticker, Close FROM {self.price_table}",
            self.conn,
            parse_dates=["Timestamp"],
        )

    def save_normalized(self, df: pd.DataFrame) -> None:
        logger.info(
            f"Writing normalized data to '{self.norm_table}'. This will replace the existing table."
        )
        df.to_sql(self.norm_table, self.conn, if_exists="replace", index=False)


class TimeSeriesNormalizer:
    """
    Handles the normalization of time series data.
    Now depends on PriceRepository abstraction (DIP).
    """

    def __init__(self, repository: PriceRepository):
        """
        Initializes the normalizer with a price repository.

        Args:
            repository: An implementation of PriceRepository.
        """
        self.repository = repository

    def normalize_all_tickers(self):
        """
        Reads all price data, normalizes it by ticker, and writes it to the repository.
        Normalization is done by dividing each price series by its first value, then multiplying by 100.
        """
        try:
            # Select only the columns needed for normalization
            df = self.repository.get_prices()

            if df.empty:
                logger.warning("Price data table is empty. Nothing to normalize.")
                return

            # Use groupby().transform('first') to get the first 'Close' value for each ticker
            # This efficiently broadcasts the first value to all rows of the group.
            df["FirstValue"] = df.groupby("Ticker")["Close"].transform("first")

            # Normalize the 'Close' price using vectorized operations for speed.
            # We use np.where to handle the potential division by zero safely.
            df["Normalized"] = np.where(
                df["FirstValue"] != 0, (df["Close"] / df["FirstValue"]) * 100, 0.0
            )

            # Select and rename columns for the final table
            normalized_df = df[["Timestamp", "Ticker", "Normalized"]].copy()

            # Replace the entire table to ensure data is always fresh and correct
            self.repository.save_normalized(normalized_df)
            logger.info("✅ Normalization complete.")

        except Exception as e:
            logger.exception(f"❌ An error occurred during data normalization: {e}")
