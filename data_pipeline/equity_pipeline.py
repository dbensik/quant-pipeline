import logging
import sqlite3

import pandas as pd
import yfinance as yf
from curl_cffi import requests

from config.settings import DB_PRICE_DATA_COLUMNS
from .data_enricher import DataEnricher

logger = logging.getLogger(__name__)


class EquityPipeline:
    """
    A pipeline for fetching, enriching, and processing equity data from Yahoo Finance.
    """

    def __init__(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        session: requests.Session,
    ):
        """
        Initializes the EquityPipeline.

        Args:
            tickers: A list of equity ticker symbols.
            start_date: The start date for data fetching (YYYY-MM-DD).
            end_date: The end date for data fetching (YYYY-MM-DD).
            session: A requests.Session object. Note: yfinance uses its own HTTP client,
                     so this session is not directly used for fetching but is kept for API consistency.
        """
        if not isinstance(tickers, list) or not tickers:
            raise ValueError("A non-empty list of tickers must be provided.")

        # Improvement: Sanitize tickers to match yfinance format (e.g., BRK.B -> BRK-B)
        self.tickers = [ticker.replace(".", "-") for ticker in tickers]
        self.start_date = start_date
        self.end_date = end_date
        self.session = session

    def fetch_batch_data(self) -> pd.DataFrame:
        """
        Fetches historical price data, enriches it with calculated metrics
        (volatility, beta, etc.), and returns a complete, analysis-ready DataFrame.

        Returns:
            A pandas DataFrame with enriched historical data for all tickers,
            formatted and ready for database insertion.
        """
        logger.info(f"Starting batch fetch for {len(self.tickers)} equity tickers...")

        # --- 1. Fetch Raw Equity Data ---
        try:
            data_wide = yf.download(
                self.tickers,
                start=self.start_date,
                end=self.end_date,
                progress=False,
                auto_adjust=True,
            )
        except Exception as e:
            logger.error(f"An error occurred during yfinance download: {e}")
            return pd.DataFrame()

        if data_wide.empty:
            logger.warning(
                "yfinance returned an empty DataFrame for tickers. Check tickers and date range."
            )
            return pd.DataFrame()

        # --- 2. Reshape data from wide to long format ---
        data_long = (
            data_wide.stack(level=1, future_stack=True)
            .rename_axis(["Date", "Ticker"])
            .reset_index()
        )

        # --- 3. Fetch Benchmark Data for Enrichment ---
        logger.info("Fetching benchmark data (SPY) for beta calculation...")
        try:
            spy_data = yf.download(
                "SPY", start=self.start_date, end=self.end_date, auto_adjust=True
            )
            if spy_data.empty:
                logger.warning(
                    "Could not download benchmark SPY data. Beta will not be calculated."
                )
                benchmark_returns = None
            else:
                # Improvement: Add fill_method=None to silence future warnings
                benchmark_returns = spy_data["Close"].pct_change(fill_method=None)
        except Exception as e:
            logger.error(f"Failed to fetch benchmark SPY data: {e}")
            benchmark_returns = None

        # --- 4. Enrich Data with Calculated Metrics ---
        logger.info("Enriching fetched data with calculated metrics...")
        enricher = DataEnricher(benchmark_returns=benchmark_returns)
        # The enricher expects a DataFrame indexed by Date with a 'Ticker' column
        data_to_enrich = data_long.set_index("Date")
        enriched_df = enricher.enrich_data(data_to_enrich)

        # Final cleanup and column selection
        final_df = enriched_df.reset_index()

        # Select only the columns that actually exist in the dataframe
        existing_cols = [
            col for col in DB_PRICE_DATA_COLUMNS if col in final_df.columns
        ]

        logger.info(
            f"✅ Successfully fetched and enriched data for {final_df['Ticker'].nunique()} tickers."
        )

        return final_df[existing_cols]

    @staticmethod
    def write_universe(
        tickers: list[str], sectors: dict, conn: sqlite3.Connection
    ) -> None:
        """
        Writes the equity universe metadata to the database using a robust
        "upsert" method that is compatible with SQLite.

        Args:
            tickers: List of equity tickers.
            sectors: A dictionary mapping tickers to their sectors.
            conn: An active sqlite3 database connection.
        """
        logger.info("Preparing to write equity universe metadata...")
        try:
            # 1. Prepare the data in a DataFrame
            universe_df = pd.DataFrame({"Ticker": tickers})
            universe_df["Sector"] = universe_df["Ticker"].apply(
                lambda ticker: sectors.get(ticker, "Unknown")
            )
            universe_df["AssetType"] = "Equity"

            # Log any tickers for which a sector could not be found. This is crucial for debugging.
            unknown_sectors = universe_df[universe_df["Sector"] == "Unknown"]
            if not unknown_sectors.empty:
                logger.warning(
                    f"Could not find sector for the following tickers, defaulting to 'Unknown': {unknown_sectors['Ticker'].tolist()}"
                )

            # 2. Convert the DataFrame to a list of tuples for insertion
            data_to_insert = list(universe_df.itertuples(index=False, name=None))

            # 3. Use a parameterized executemany for a safe and efficient upsert
            cursor = conn.cursor()
            cursor.executemany(
                """
                               INSERT INTO universe_metadata (Ticker, Sector, AssetType)
                               VALUES (?, ?, ?)
                               ON CONFLICT(Ticker) DO UPDATE SET
                                   Sector = excluded.Sector,
                                   AssetType = excluded.AssetType;
                               """,
                data_to_insert,
            )
            conn.commit()

            logger.info(
                f"✅ Successfully wrote/updated metadata for {len(universe_df)} equity tickers."
            )
        except Exception as e:
            logger.exception(f"❌ Failed to write equity universe metadata: {e}")
