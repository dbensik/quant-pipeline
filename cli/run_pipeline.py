import argparse
import logging
import sqlite3
from config.settings import DB_PATH
from data_pipeline.pipeline_orchestrator import PipelineOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


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
