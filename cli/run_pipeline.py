# cli/run_pipeline.py
import logging
from data_pipeline.data_pipeline import DataPipeline
from alpha_models.mean_reversion import MeanReversionStrategy
from backtesting.backtester import Backtester
import matplotlib.pyplot as plt
import sys
import pandas as pd

# Configure logging for tracking pipeline execution and issues
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_data_pipeline(ticker, start_date, end_date, db_path, table_name):
    """
    Runs the data pipeline for a given ticker.
    Includes fetching, cleaning, and saving data to the database.
    """
    try:
        pipeline = DataPipeline(ticker, start_date, end_date)
        pipeline.fetch_data()
        pipeline.clean_data()
        pipeline.save_data(db_path=db_path, table_name=table_name)
        return pipeline
    except Exception as e:
        logger.error(f"Pipeline failed for {ticker}: {e}")
        return None

def main():
    """
    Main function to run the data pipeline, signal generation,
    and backtesting for a list of tickers.
    """
    tickers = ["SPY", "AAPL", "MSFT"]  # List of tickers to process
    start_date = "2018-01-01"
    end_date = "2024-12-31"
    db_path = "../quant_pipeline.db"
    table_name = "price_data"

    for ticker in tickers:
        logger.info(f"Running pipeline for {ticker}...")
        pipeline = run_data_pipeline(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            db_path=db_path,
            table_name=table_name
        )

        # If pipeline failed, skip to the next ticker
        if pipeline is None:
            continue

        data = pipeline.data

        try:
            # Query to get average closing price for the ticker
            query = f"SELECT AVG(Close) AS avg_close FROM {table_name} WHERE Ticker = '{ticker}'"
            result = pipeline.query_data(query, db_path=db_path)
            avg_close = result.iloc[0]['avg_close']
            logger.info(f"{ticker} - Average closing price: {avg_close:.2f}")
        except Exception as e:
            logger.error(f"Query failed for {ticker}: {e}")
            continue

        try:
            # Generate trading signals using mean reversion
            strategy = MeanReversionStrategy(window=20, threshold=0.05)
            signals = strategy.generate_signals(data)
            logger.info(f"{ticker} - Signals generated.")

            # Log number of signals
            num_buy = (signals == 1).sum()
            num_sell = (signals == -1).sum()
            logger.info(f"{ticker} - Number of Buy Signals: {num_buy}")
            logger.info(f"{ticker} - Number of Sell Signals: {num_sell}")

            # Run the backtest and print performance metrics
            backtester = Backtester(data, signals)
            portfolio = backtester.run_backtest()
            backtester.print_performance()

            # Plot portfolio performance
            plt.figure(figsize=(10,6))
            plt.plot(portfolio.index, portfolio, label=f"{ticker} Portfolio Value")
            plt.title(f"Backtested Portfolio Value - {ticker}")
            plt.xlabel("Date")
            plt.ylabel("Portfolio Value")
            plt.legend()

            # Only display plot if not in Jupyter/IPython
            if 'ipykernel' not in sys.modules:
                plt.show()

        except Exception as e:
            logger.error(f"Backtesting failed for {ticker}: {e}")

    logger.info("Pipeline run complete!")

if __name__ == "__main__":
    main()
