import logging
import sqlite3

import pandas as pd
import yfinance as yf
from curl_cffi import requests

from config.settings import DB_PRICE_DATA_COLUMNS
from .data_enricher import DataEnricher

logger = logging.getLogger(__name__)


class CryptoPipeline:
    """
    A pipeline for fetching, enriching, and processing cryptocurrency data.
    It correctly handles the conversion from crypto symbols (e.g., 'btc')
    to the format required by Yahoo Finance (e.g., 'BTC-USD').
    """

    def __init__(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        session: requests.Session,
    ):
        """
        Initializes the CryptoPipeline.

        Args:
            tickers: A list of crypto symbols (e.g., ['btc', 'eth']).
                     This should come from the 'symbol' field of the CoinGecko API.
            start_date: The start date for data fetching (YYYY-MM-DD).
            end_date: The end date for data fetching (YYYY-MM-DD).
            session: A requests.Session object.
        """
        if not isinstance(tickers, list) or not tickers:
            raise ValueError("A non-empty list of tickers must be provided.")

        # Store the original symbols and create the Yahoo Finance-compatible tickers
        self.base_tickers = tickers
        self.yahoo_tickers = []
        for ticker in self.base_tickers:
            t_upper = ticker.upper()
            if not t_upper.endswith("-USD"):
                self.yahoo_tickers.append(f"{t_upper}-USD")
            else:
                self.yahoo_tickers.append(t_upper)
        self.start_date = start_date
        self.end_date = end_date
        self.session = session

    def fetch_batch_data(self) -> pd.DataFrame:
        """
        Fetches historical crypto data, enriches it with calculated metrics,
        and returns a complete, analysis-ready DataFrame.

        Returns:
            A pandas DataFrame with enriched historical data for all tickers.
            The 'Ticker' column will contain the Yahoo Finance format (e.g., 'BTC-USD').
        """
        logger.info(
            f"Starting batch fetch for {len(self.yahoo_tickers)} crypto tickers..."
        )

        # --- 1. Fetch Raw Crypto Data ---
        try:
            # Use the correctly formatted yahoo_tickers list
            data_wide = yf.download(
                tickers=self.yahoo_tickers,
                start=self.start_date,
                end=self.end_date,
                progress=False,
                threads=True,
                auto_adjust=True,  # Recommended to simplify data by removing Adj. Close
            )
        except Exception as e:
            logger.error(f"An error occurred during yfinance download for crypto: {e}")
            return pd.DataFrame()

        if data_wide.empty:
            logger.warning("yfinance returned an empty DataFrame for crypto tickers.")
            return pd.DataFrame()

        # --- 2. Reshape data from wide to long format ---
        # The 'Ticker' level in the multi-index will now correctly be 'BTC-USD', etc.
        data_long = (
            data_wide.stack(level=1, future_stack=True)
            .rename_axis(["Date", "Ticker"])
            .reset_index()
        )

        # --- 3. Fetch Crypto Benchmark Data (Bitcoin) ---
        logger.info("Fetching crypto benchmark data (BTC-USD) for beta calculation...")
        try:
            btc_data = yf.download(
                "BTC-USD", start=self.start_date, end=self.end_date, auto_adjust=True
            )
            if btc_data.empty:
                logger.warning(
                    "Could not download benchmark BTC-USD data. Beta will not be calculated."
                )
                benchmark_returns = None
            else:
                benchmark_returns = btc_data["Close"].pct_change()
        except Exception as e:
            logger.error(f"Failed to fetch benchmark BTC-USD data: {e}")
            benchmark_returns = None

        # --- 4. Enrich Data with Calculated Metrics ---
        logger.info("Enriching fetched crypto data with calculated metrics...")
        enricher = DataEnricher(benchmark_returns=benchmark_returns)
        data_to_enrich = data_long.set_index("Date")
        enriched_df = enricher.enrich_data(data_to_enrich)

        # Final cleanup and column selection
        final_df = enriched_df.reset_index()

        existing_cols = [
            col for col in DB_PRICE_DATA_COLUMNS if col in final_df.columns
        ]

        logger.info(
            f"✅ Successfully fetched and enriched data for {final_df['Ticker'].nunique()} crypto tickers."
        )

        return final_df[existing_cols]

    @staticmethod
    def write_universe(tickers: list[str], conn: sqlite3.Connection) -> None:
        """
        Writes the crypto universe metadata to the database using a robust
        "upsert" method. It stores the tickers in the Yahoo Finance format
        (e.g., 'BTC-USD') for consistency with the price data table.

        Args:
            tickers: List of crypto symbols (e.g., ['btc', 'eth']).
            conn: An active sqlite3 database connection.
        """
        logger.info("Preparing to write crypto universe metadata...")
        try:
            # Convert base symbols to the consistent Yahoo Finance format for storage
            yahoo_tickers = [f"{ticker.upper()}-USD" for ticker in tickers]

            universe_df = pd.DataFrame({"Ticker": yahoo_tickers})
            universe_df["Sector"] = "Cryptocurrency"
            universe_df["AssetType"] = "Crypto"

            data_to_insert = list(universe_df.itertuples(index=False, name=None))

            cursor = conn.cursor()
            cursor.executemany(
                """
                               INSERT INTO universe_metadata (Ticker, Sector, AssetType)
                               VALUES (?, ?, ?)
                               ON CONFLICT(Ticker) DO UPDATE SET Sector    = excluded.Sector,
                                                                 AssetType = excluded.AssetType;
                               """,
                data_to_insert,
            )
            conn.commit()

            logger.info(
                f"✅ Successfully wrote/updated metadata for {len(universe_df)} crypto tickers."
            )
        except Exception as e:
            logger.exception(f"❌ Failed to write crypto universe metadata: {e}")
