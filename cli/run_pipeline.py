import argparse
import logging
import sqlite3
from datetime import datetime

# --- Project Imports ---
from config.settings import DB_PATH, DEFAULT_START_DATE
from dashboard_app.database_manager import DatabaseManager
from data_pipeline.crypto_pipeline import CryptoPipeline
from data_pipeline.equity_pipeline import EquityPipeline
from data_pipeline.fundamental_pipeline import FundamentalPipeline

# Configure logging to show timestamp, level, and message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """
    Orchestrates the entire data pipeline process, from discovering the
    universe of assets to fetching and storing their data.
    """

    def __init__(self, conn: sqlite3.Connection):
        """
        Initializes the orchestrator with a database connection.
        """
        self.conn = conn
        self.db_manager = DatabaseManager(db_path=None, conn=self.conn)
        # Ensure all necessary tables exist before running.
        self._setup_database()

    def _setup_database(self):
        """
        Ensures all necessary tables are created by their respective managers.
        This is a key part of the new design.
        """
        logger.info("Ensuring all necessary database tables exist...")
        self.db_manager.create_tables()  # Handles price_data, universe_metadata, etc.
        FundamentalPipeline.create_table(self.conn)
        logger.info("Database schema is ready.")

    def run(self, full_backfill: bool = False):
        """
        Executes the full data pipeline workflow in a clear, linear fashion.
        """
        logger.info("🚀 Starting main data pipeline run...")

        # 1. Determine the date range for fetching data.
        if full_backfill:
            start_date = DEFAULT_START_DATE
            logger.info(f"Performing FULL BACKFILL from start date: {start_date}.")
        else:
            # The DatabaseManager is now responsible for this logic.
            start_date = self.db_manager.get_latest_date()
            logger.info(f"Performing INCREMENTAL UPDATE from last date: {start_date}.")
        end_date = datetime.now().strftime("%Y-%m-%d")

        # 2. Get the list of assets to process directly from the database.
        equities = self.db_manager.get_tickers_by_asset_type("Equity")
        cryptos = self.db_manager.get_tickers_by_asset_type("Crypto")

        logger.info(
            f"Discovered tickers in database: {len(equities)} equities, {len(cryptos)} cryptos."
        )

        # 3. Run the Equity Price Pipeline.
        if equities:
            logger.info("--- Starting Equity Price Pipeline ---")
            equity_pipeline = EquityPipeline(equities, start_date, end_date, None)
            equity_data = equity_pipeline.fetch_batch_data()
            if not equity_data.empty:
                self.db_manager.write_price_data(equity_data)
            logger.info("--- Equity Price Pipeline Complete ---")

        # 4. Run the Crypto Price Pipeline.
        if cryptos:
            logger.info("--- Starting Crypto Price Pipeline ---")
            crypto_pipeline = CryptoPipeline(cryptos, start_date, end_date, None)
            crypto_data = crypto_pipeline.fetch_batch_data()
            if not crypto_data.empty:
                self.db_manager.write_price_data(crypto_data)
            logger.info("--- Crypto Price Pipeline Complete ---")

        # 5. Run the Fundamental Data Pipeline (only for equities).
        if equities:
            logger.info("--- Starting Fundamental Data Pipeline ---")
            FundamentalPipeline.fetch_and_write_fundamentals(
                tickers=equities, conn=self.conn
            )
            logger.info("--- Fundamental Data Pipeline Complete ---")

        logger.info("✅ Main data pipeline run completed successfully!")


def main():
    """Main entry point for the command-line interface."""
    parser = argparse.ArgumentParser(description="Data Pipeline Orchestrator")
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Perform a full backfill of all data from the default start date.",
    )
    args = parser.parse_args()

    conn = None
    try:
        # The connection is created once and passed to the orchestrator.
        conn = sqlite3.connect(DB_PATH)
        orchestrator = PipelineOrchestrator(conn)
        orchestrator.run(full_backfill=args.full_backfill)
    except sqlite3.Error as e:
        logger.exception(f"A database error occurred: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
