import logging
import sqlite3

import pandas as pd

from config.settings import DB_NORMALIZED_TABLE, DB_PRICE_TABLE

logger = logging.getLogger(__name__)


class TimeSeriesNormalizer:
    """
    Handles the normalization of time series data in the database.
    """

    def __init__(self, conn: sqlite3.Connection):
        """
        Initializes the normalizer with a database connection.

        Args:
            conn: An active sqlite3 database connection.
        """
        self.conn = conn
        self.price_table = DB_PRICE_TABLE
        self.norm_table = DB_NORMALIZED_TABLE

    def normalize_all_tickers(self):
        """
        Reads all price data, normalizes it by ticker, and writes it to a
        new table. Normalization is done by dividing each price series by its
        first value, then multiplying by 100.
        """
        logger.info(f"Reading data from '{self.price_table}' for normalization.")
        try:
            # Select only the columns needed for normalization
            df = pd.read_sql(
                f"SELECT Date, Ticker, Close FROM {self.price_table}",
                self.conn,
                parse_dates=["Date"],
            )
            if df.empty:
                logger.warning("Price data table is empty. Nothing to normalize.")
                return

            # Use groupby().transform('first') to get the first 'Close' value for each ticker
            # This efficiently broadcasts the first value to all rows of the group.
            df["FirstValue"] = df.groupby("Ticker")["Close"].transform("first")

            # Normalize the 'Close' price, handling potential division by zero
            df["Normalized"] = df.apply(
                lambda row: (
                    (row["Close"] / row["FirstValue"]) * 100
                    if row["FirstValue"] != 0
                    else 0
                ),
                axis=1,
            )

            # Select and rename columns for the final table
            normalized_df = df[["Date", "Ticker", "Normalized"]].copy()

            logger.info(
                f"Writing normalized data to '{self.norm_table}'. This will replace the existing table."
            )
            # Replace the entire table to ensure data is always fresh and correct
            normalized_df.to_sql(
                self.norm_table, self.conn, if_exists="replace", index=False
            )
            logger.info("✅ Normalization complete.")

        except Exception as e:
            logger.exception(f"❌ An error occurred during data normalization: {e}")
