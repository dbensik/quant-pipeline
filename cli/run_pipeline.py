import logging
import time
from data_pipeline.equity_pipeline import EquityPipeline
from data_pipeline.crypto_pipeline import CryptoPipeline
from alpha_models.mean_reversion import MeanReversionStrategy
from backtesting.backtester import Backtester
from data_pipeline.time_series_normalizer import TimeSeriesNormalizer
import matplotlib.pyplot as plt
import sys
import pandas as pd
from curl_cffi import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Define tickers and crypto pairs to fetch
    tickers = ["SPY", "GME"]
    crypto_pairs = ["BTC-USD", "ETH-USD"]
    start_date = "2018-01-01"
    end_date = "2024-12-31"
    db_path = "../quant_pipeline.db"
    table_name = "price_data"
    normalized_table_name = "price_data_normalized"

    # Create a spoofed session to impersonate a real browser
    session = requests.Session(impersonate="chrome")

    # --- EQUITY PIPELINE ---
    equity_pipeline = EquityPipeline(ticker=None, start_date=start_date, end_date=end_date, session=session)
    try:
        equity_pipeline.fetch_batch_data(tickers, db_path=db_path, table_name=table_name)
        logger.info("Equity data fetch succeeded.")
    except Exception as e:
        logger.error(f"Equity batch data fetch failed: {e}")

    # --- CRYPTO PIPELINE ---
    crypto_pipeline = CryptoPipeline(pairs=crypto_pairs, start_date=start_date, end_date=end_date)
    try:
        crypto_pipeline.fetch_batch_data(db_path=db_path, table_name=table_name)
        logger.info("Crypto data fetch succeeded.")
    except Exception as e:
        logger.error(f"Crypto batch data fetch failed: {e}")



    # --- NORMALIZATION ---
    # After fetching equity data:
    equity_dfs = {}

    for ticker in tickers:
        try:
            query = f"SELECT * FROM {table_name} WHERE Ticker = '{ticker}'"
            df = equity_pipeline.query_data(query, db_path=db_path)
            equity_dfs[ticker] = df
        except Exception as e:
            logger.warning(f"Failed to query equity data for {ticker}: {e}")

    # After fetching crypto data:
    crypto_dfs = {}

    for pair in crypto_pairs:
        try:
            query = f"SELECT * FROM {table_name} WHERE Ticker = '{pair}'"
            df = crypto_pipeline.query_data(query, db_path=db_path)
            crypto_dfs[pair] = df
        except Exception as e:
            logger.warning(f"Failed to query crypto data for {pair}: {e}")



    #normalizer = TimeSeriesNormalizer(db_path=db_path, table_name=table_name)
    try:
        normalized = TimeSeriesNormalizer.normalize_from_cli({**equity_dfs, **crypto_dfs})
        logger.info(f"Normalized data saved to table '{normalized_table_name}'")
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        return

    # --- SELECT DEFAULT SYMBOL FOR TESTING ---
    try:
        if tickers:
            default_symbol = tickers[0]
        elif crypto_pairs:
            default_symbol = crypto_pairs[0]
        else:
            raise IndexError("No tickers or crypto pairs provided.")

        equity_pipeline.ticker = default_symbol
        equity_pipeline.data = equity_pipeline.query_data(
            f"SELECT * FROM {table_name} WHERE Ticker = '{default_symbol}'", db_path=db_path)
        logger.info(f"Loaded data for: {default_symbol}")
    except IndexError as e:
        logger.error(e)
        return
    except Exception as e:
        logger.error(f"Failed to query data for {default_symbol}: {e}")
        return

    data = equity_pipeline.data

    # --- AVERAGE CLOSING PRICE ---
    query = f"SELECT AVG(Close) AS avg_close FROM {table_name} WHERE Ticker = '{default_symbol}'"
    try:
        result = equity_pipeline.query_data(query, db_path=db_path)
        avg_close = result.iloc[0].get('avg_close', None)
        if avg_close is not None:
            logger.info(f"Average closing price for {default_symbol}: {avg_close}")
        else:
            logger.warning("Average closing price is missing from the query result.")
    except Exception as e:
        logger.error(f"Failed to execute average price query: {e}")
        return

    # --- STRATEGY SIGNAL GENERATION ---
    strategy = MeanReversionStrategy(window=20, threshold=0.05)
    signals = strategy.generate_signals(data)
    logger.info("Signals generated.")

    # Log signal statistics
    num_buy = (signals == 1).sum()
    num_sell = (signals == -1).sum()
    logger.info(f"Number of Buy Signals: {num_buy}")
    logger.info(f"Number of Sell Signals: {num_sell}")

    # --- BACKTESTING ---
    backtester = Backtester(data, signals)
    portfolio = backtester.run_backtest()
    backtester.print_performance()

    # --- VISUALIZATION ---
    plt.figure(figsize=(10,6))
    plt.plot(portfolio.index, portfolio, label="Portfolio Value", color="navy")
    plt.title("Backtested Portfolio Value", fontsize=14)
    plt.xlabel("Date")
    plt.ylabel("Portfolio Value ($)")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend()

    # Display plot only outside of notebook environments
    if 'ipykernel' not in sys.modules:
        plt.show()

    logger.info("Pipeline run complete!")

if __name__ == "__main__":
    main()
