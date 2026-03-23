import logging
import sys
import traceback
from pathlib import Path

import pandas as pd

# --- Setup Project Path and Logging ---
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Mock/Assume these imports from your project structure ---
try:
    from data_pipeline.equity_price_fetcher import EquityPriceFetcher
    from data_pipeline.data_enricher import DataEnricher
except ImportError:
    logger.error(
        "Could not import necessary pipeline components. "
        "Please ensure file paths and class names are correct."
    )
    # As a fallback, create a plausible fetcher if one isn't found
    import yfinance as yf

    class EquityPriceFetcher:
        """A plausible yfinance fetcher for diagnostic purposes."""

        def fetch(self, tickers: list, start: str, end: str) -> pd.DataFrame:
            df = yf.download(tickers, start=start, end=end)
            if df.empty:
                return df
            # yfinance returns wide format for multiple tickers; convert to long
            if len(tickers) > 1:
                df = df.stack().reset_index()
                df = df.rename(columns={"level_1": "Ticker"})
            else:  # for a single ticker, the columns are not multi-level
                df.reset_index(inplace=True)
                df["Ticker"] = tickers[0]
            df.set_index("Date", inplace=True)
            return df

    logger.info("Using a fallback EquityPriceFetcher for diagnostics.")


def run_diagnostic():
    """
    Runs a diagnostic test on the data pipeline, saving the output
    of the enrichment stage to a CSV for inspection.
    """
    TICKERS_TO_TEST = ["AAPL", "MSFT"]
    START_DATE = "2023-01-01"
    END_DATE = "2023-03-31"
    OUTPUT_CSV_PATH = "diagnostic_output.csv"

    logger.info("--- Starting Pipeline Diagnostic ---")

    try:
        # 1. Fetch raw price data
        logger.info(
            f"Fetching raw data for {TICKERS_TO_TEST} from {START_DATE} to {END_DATE}..."
        )
        fetcher = EquityPriceFetcher()
        raw_df = fetcher.fetch(tickers=TICKERS_TO_TEST, start=START_DATE, end=END_DATE)
        if raw_df.empty:
            logger.error("Fetching raw data failed or returned an empty DataFrame.")
            return
        logger.info(f"✅ Successfully fetched raw data. Shape: {raw_df.shape}")

        # 2. Fetch benchmark data for enrichment
        logger.info("Fetching benchmark data (SPY) for enrichment...")
        spy_df = fetcher.fetch(tickers=["SPY"], start=START_DATE, end=END_DATE)
        if spy_df.empty:
            logger.warning(
                "Could not fetch SPY benchmark data. Beta will not be calculated."
            )
            benchmark_returns = None
        else:
            benchmark_returns = spy_df["Close"].pct_change()
            logger.info("✅ Benchmark data fetched.")

        # 3. Enrich the data
        logger.info("Enriching data...")
        enricher = DataEnricher(benchmark_returns=benchmark_returns)
        enriched_df = enricher.enrich_data(raw_df.copy())  # Pass a copy to be safe

        if enriched_df.empty:
            logger.error("Enrichment resulted in an empty DataFrame.")
            return
        logger.info("✅ Data enrichment complete.")

        # 4. Inspect the final DataFrame before it would go to the database
        print("\n" + "=" * 50)
        logger.info("INSPECTING FINAL DATAFRAME")
        print("=" * 50)
        logger.info(f"Shape: {enriched_df.shape}")
        logger.info(f"Index type: {type(enriched_df.index)}")
        logger.info("Index has NaNs: " + str(enriched_df.index.hasnans))

        print("\nDataFrame Info:")
        enriched_df.info()

        print("\nDataFrame Head:")
        print(enriched_df.head())

        print("\nDataFrame Tail:")
        print(enriched_df.tail())

        # 5. Save to CSV for external inspection
        logger.info(f"Saving final DataFrame to '{OUTPUT_CSV_PATH}' for inspection...")
        enriched_df.to_csv(OUTPUT_CSV_PATH)
        print("=" * 50)
        logger.info(f"✅ Diagnostic complete. Please inspect '{OUTPUT_CSV_PATH}'.")
        print("=" * 50)

    except Exception as e:
        logger.error(f"An error occurred during the diagnostic run: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    run_diagnostic()
