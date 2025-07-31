import logging
import sqlite3

import pandas as pd
import yfinance as yf

# Each module gets its own logger.
logger = logging.getLogger(__name__)


class FundamentalPipeline:
    """
    A pipeline for fetching and storing fundamental data like market cap,
    P/E ratio, etc., for a universe of tickers.
    """

    @staticmethod
    def create_table(conn: sqlite3.Connection):
        """Creates the fundamental_data table if it doesn't exist."""
        logger.info("Checking and creating 'fundamental_data' table if needed...")
        try:
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fundamental_data (
                        Ticker TEXT PRIMARY KEY,
                        MarketCap REAL,
                        TrailingPE REAL,
                        ForwardPE REAL,
                        EnterpriseValue REAL,
                        BookValue REAL,
                        FOREIGN KEY (Ticker) REFERENCES universe_metadata (Ticker)
                    );
                """
                )
            logger.info("'fundamental_data' table is ready.")
        except sqlite3.Error as e:
            logger.exception(f"Failed to create 'fundamental_data' table: {e}")
            raise

    @staticmethod
    def fetch_and_write_fundamentals(tickers: list[str], conn: sqlite3.Connection):
        """
        Fetches fundamental data for a list of tickers and writes it to the database.
        """
        logger.info(f"Starting fundamental data fetch for {len(tickers)} tickers.")
        fundamental_data = []

        for ticker_symbol in tickers:
            try:
                ticker_obj = yf.Ticker(ticker_symbol)
                info = ticker_obj.info

                # Extract data safely, providing None as a default
                data_point = {
                    "Ticker": ticker_symbol,
                    "MarketCap": info.get("marketCap"),
                    "TrailingPE": info.get("trailingPE"),
                    "ForwardPE": info.get("forwardPE"),
                    "EnterpriseValue": info.get("enterpriseValue"),
                    "BookValue": info.get("bookValue"),
                }
                fundamental_data.append(data_point)
                logger.debug(
                    f"Successfully fetched fundamental data for {ticker_symbol}."
                )

            except Exception as e:
                # Log a warning for individual failures but continue the loop
                logger.warning(
                    f"Could not fetch or process fundamental data for {ticker_symbol}: {e}"
                )
                continue

        if not fundamental_data:
            logger.warning(
                "No fundamental data was successfully fetched. Aborting database write."
            )
            return

        # Convert to DataFrame and write to the database
        df = pd.DataFrame(fundamental_data)
        logger.info(
            f"Preparing to write fundamental data for {len(df)} tickers to the database."
        )

        try:
            data_to_insert = list(df.itertuples(index=False, name=None))
            with conn:
                cursor = conn.cursor()
                cursor.executemany(
                    """
                    INSERT INTO fundamental_data (Ticker, MarketCap, TrailingPE, ForwardPE, EnterpriseValue, BookValue)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(Ticker) DO UPDATE SET
                        MarketCap = excluded.MarketCap,
                        TrailingPE = excluded.TrailingPE,
                        ForwardPE = excluded.ForwardPE,
                        EnterpriseValue = excluded.EnterpriseValue,
                        BookValue = excluded.BookValue;
                """,
                    data_to_insert,
                )
            logger.info(
                f"✅ Successfully wrote/updated fundamental data for {len(df)} tickers."
            )
        except sqlite3.Error as e:
            # Log the full exception traceback for database errors
            logger.exception(f"❌ Failed to write fundamental data to database: {e}")
