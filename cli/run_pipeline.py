#!/usr/bin/env python3
import logging
import logging.handlers
import time
import sys, os
# add project root to sys.path for local script imports
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))
from data_pipeline.equity_pipeline import EquityPipeline
from data_pipeline.crypto_pipeline import CryptoPipeline
from data_pipeline.time_series_normalizer import TimeSeriesNormalizer
from scripts.init_db import initialize_database
from scripts.constituents import load_sp500, load_top_crypto_pairs
from scripts.crypto_meta import load_crypto_meta
from curl_cffi import requests
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3



# Configure logging: console + rotating file
LOG_FILE = os.path.join(os.path.dirname(__file__), 'run_pipeline.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
    ]
)
logger = logging.getLogger(__name__)


def main():
    # --- CONFIGURATION ---
    # tickers = ["SPY", "GME"]
    # crypto_pairs = ["BTC-USD", "ETH-USD"]
    start_date = "2018-01-01"
    end_date = "2024-12-31"
    db_path = os.path.abspath(os.path.join(__file__, '..', '..', 'quant_pipeline.db'))
    raw_table = "price_data"

    # Equities:
    eq_tickers, eq_sectors = load_sp500()

    # Cryptos:
    crypto_pairs, crypto_caps = load_top_crypto_pairs(vs_currency="usd", top_n=50)

    # Initialize database schema (drops + recreates tables)
    initialize_database()

    # --- EQUITY PIPELINE ---
    equity = EquityPipeline(
        tickers=eq_tickers,
        start_date=start_date,
        end_date=end_date,
        session=requests.Session(impersonate="chrome"),
        db_path=db_path,
        table_name=raw_table
    )
    try:
        equity.fetch_batch_data()
        # logger.info("✅ Equity data fetched & saved.")
    except Exception as e:
        # logger.error(f"Equity batch fetch failed: {e}")
        return

    # --- CRYPTO PIPELINE ---
    crypto = CryptoPipeline(
        pairs=crypto_pairs,
        start_date=start_date,
        end_date=end_date,
        session=requests.Session(impersonate="chrome"),
        db_path=db_path,
        table_name=raw_table
    )
    try:
        crypto.fetch_batch_data()
        # logger.info("✅ Crypto data fetched & saved.")
    except Exception as e:
        # logger.error(f"Crypto batch fetch failed: {e}")
        return

    equity.write_universe(eq_tickers, eq_sectors)
    crypto.write_universe(crypto_pairs, crypto_caps)

    # --- LOAD RAW FOR NORMALIZATION ---
    all_series = {}
    for symbol in eq_tickers:
        df = equity.query_data(ticker=symbol)
        logger.info(f"Loaded raw equity '{symbol}': {df.shape[0]} rows, cols={list(df.columns)}")
        all_series[symbol] = df[['Close']]
    for pair in crypto_pairs:
        df = crypto.query_data(ticker=pair)
        logger.info(f"Loaded raw crypto '{pair}': {df.shape[0]} rows, cols={list(df.columns)}")
        all_series[pair] = df[['Close']]

    # --- NORMALIZATION ---
    try:
        norm = TimeSeriesNormalizer.normalize_from_cli(all_series)
        logger.info(f"✅ Normalization complete for: {list(norm.keys())}")
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        return

    logger.info("🎉 Pipeline run complete!")


if __name__ == "__main__":
    main()
