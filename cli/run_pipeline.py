#!/usr/bin/env python3
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

# Configure logging to include timestamps and log levels
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            'run_pipeline.log', maxBytes=5e6, backupCount=3
        )
    ]
)
logger = logging.getLogger(__name__)


def main():
    # --- CONFIGURATION ---
    tickers = ["SPY", "GME"]
    crypto_pairs = ["BTC-USD", "ETH-USD"]
    start_date = "2018-01-01"
    end_date = "2024-12-31"
    db_path = "../quant_pipeline.db"
    raw_table = "price_data"
    norm_table = "price_data_normalized"

    # Create a spoofed HTTP session for equity fetch
    session = requests.Session(impersonate="chrome")

    # --- EQUITY DATA FETCH ---
    equity_pipeline = EquityPipeline(
        #ticker=None,
        start_date=start_date,
        end_date=end_date,
        session=session
    )
    try:
        equity_pipeline.fetch_batch_data(
            tickers, db_path=db_path, table_name=raw_table
        )
        logger.info("✅ Equity data fetched & saved.")
    except Exception as e:
        logger.error(f"Equity batch fetch failed: {e}")
        return

    # --- CRYPTO DATA FETCH ---
    # Initialize without pairs; fetch_batch_data will accept them
    crypto_pipeline = CryptoPipeline(
        start_date=start_date,
        end_date=end_date
    )
    try:
        crypto_pipeline.fetch_batch_data(
            crypto_pairs, db_path=db_path, table_name=raw_table
        )
        logger.info("✅ Crypto data fetched & saved.")
    except Exception as e:
        logger.error(f"Crypto batch fetch failed: {e}")
        return

    # --- LOAD RAW DATA FOR NORMALIZATION ---
    equity_dfs, crypto_dfs = {}, {}
    for ticker in tickers:
        try:
            df = equity_pipeline.query_data(
                f"SELECT Date, Close FROM {raw_table} WHERE Ticker='{ticker}'",
                db_path=db_path
            )
            equity_dfs[ticker] = df
        except Exception as e:
            logger.warning(f"Failed to load raw for {ticker}: {e}")

    for pair in crypto_pairs:
        try:
            df = crypto_pipeline.query_data(
                f"SELECT Date, Close FROM {raw_table} WHERE Ticker='{pair}'",
                db_path=db_path
            )
            crypto_dfs[pair] = df
        except Exception as e:
            logger.warning(f"Failed to load raw for {pair}: {e}")

    # --- NORMALIZATION ---
    try:
        norm_series = TimeSeriesNormalizer.normalize_from_cli({**equity_dfs, **crypto_dfs})
        logger.info(f"✅ Normalization complete for: {list(norm_series.keys())}")
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        return

    # --- SELECT DEFAULT SYMBOL FOR BACKTEST ---
    # default_symbol = tickers[0] if tickers else crypto_pairs[0]
    # try:
    #     equity_pipeline.ticker = default_symbol
    #     equity_pipeline.data = equity_pipeline.query_data(
    #         f"SELECT * FROM {raw_table} WHERE Ticker='{default_symbol}'",
    #         db_path=db_path
    #     )
    #     logger.info(f"Loaded raw data for {default_symbol}: {len(equity_pipeline.data)} rows.")
    # except Exception as e:
    #     logger.error(f"Failed to load data for backtest: {e}")
    #     return

    # # --- BACKTEST & METRICS ---
    # strategy = MeanReversionStrategy(window=20, threshold=0.05)
    # signals = strategy.generate_signals(equity_pipeline.data)
    # buys = (signals == 1).sum()
    # sells = (signals == -1).sum()
    # logger.info(f"Signals: BUY={buys}, SELL={sells}")

    # backtester = Backtester(equity_pipeline.data, signals)
    # portfolio = backtester.run_backtest()
    # backtester.print_performance()

    # # --- PLOT RESULTS ---
    # plt.figure(figsize=(10, 6))
    # plt.plot(portfolio.index, portfolio, color="navy")
    # plt.title(f"Backtested Portfolio Value: {default_symbol}")
    # plt.xlabel("Date"); plt.ylabel("Portfolio Value ($)")
    # plt.grid(alpha=0.4)
    # if 'ipykernel' not in sys.modules:
    #     plt.show()

    logger.info("🎉 Pipeline run complete!")


if __name__ == "__main__":
    main()
