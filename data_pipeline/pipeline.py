import logging

import pandas as pd

from config.settings import DB_PRICE_DATA_COLUMNS
from .base_fetcher import BaseDataFetcher
from .data_enricher import DataEnricher

logger = logging.getLogger(__name__)


class DataPipeline:
    """
    A generic, configurable pipeline for fetching, enriching, and processing
    time-series data from any source.
    """

    def __init__(
        self,
        fetcher: BaseDataFetcher,
        tickers: list[str],
        start_date: str,
        end_date: str,
        granularity: str = "1d",
        benchmark_ticker: str = "SPY",
    ):
        """
        Initializes the generic DataPipeline.

        Args:
            fetcher: A concrete instance of a DataFetcher (e.g., YFinanceFetcher).
            tickers: A list of ticker symbols.
            start_date: The start date for data fetching (YYYY-MM-DD).
            end_date: The end date for data fetching (YYYY-MM-DD).
            granularity: The data interval (e.g., '1d', '1h').
            benchmark_ticker: The ticker to use for benchmark calculations (e.g., 'SPY', 'BTC-USD').
        """
        self.fetcher = fetcher
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.granularity = granularity
        self.benchmark_ticker = benchmark_ticker

    def execute(self) -> pd.DataFrame:
        """
        Executes the full data processing pipeline: fetch, enrich, and format.

        Returns:
            A pandas DataFrame with enriched historical data, ready for the database.
        """
        logger.info(
            f"Executing data pipeline for {len(self.tickers)} tickers with '{self.granularity}' granularity."
        )

        # 1. Fetch Raw Data using the provided fetcher
        data_long = self.fetcher.fetch_data(
            self.tickers, self.start_date, self.end_date, self.granularity
        )
        if data_long.empty:
            logger.warning("Fetcher returned an empty DataFrame. Halting pipeline.")
            return pd.DataFrame()

        # 2. Fetch Benchmark Data for Enrichment
        logger.info(
            f"Fetching benchmark data ({self.benchmark_ticker}) for enrichment..."
        )
        benchmark_df = self.fetcher.fetch_data(
            [self.benchmark_ticker], self.start_date, self.end_date, self.granularity
        )
        if benchmark_df.empty:
            logger.warning(
                f"Could not download benchmark {self.benchmark_ticker} data. Beta will not be calculated."
            )
            benchmark_returns = None
        else:
            # Ensure single-column for pct_change
            benchmark_close = benchmark_df.set_index("Date")["Close"]
            benchmark_returns = benchmark_close.pct_change()

        # 3. Enrich Data with Calculated Metrics
        logger.info("Enriching fetched data with calculated metrics...")
        enricher = DataEnricher(
            benchmark_returns=benchmark_returns, granularity=self.granularity
        )
        data_to_enrich = data_long.set_index("Date")
        enriched_df = enricher.enrich_data(data_to_enrich)

        # 4. Final cleanup and column selection
        final_df = enriched_df.reset_index()

        # This part can be improved further by making columns dynamic based on enrichment
        existing_cols = [
            col for col in DB_PRICE_DATA_COLUMNS if col in final_df.columns
        ]
        # Ensure the core columns are always present if they exist
        core_cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
        final_cols = core_cols + [col for col in existing_cols if col not in core_cols]

        # Filter to only columns that actually exist in the final dataframe
        final_cols = [col for col in final_cols if col in final_df.columns]

        logger.info(
            f"✅ Successfully executed pipeline for {final_df['Ticker'].nunique()} tickers."
        )
        return final_df[final_cols]
