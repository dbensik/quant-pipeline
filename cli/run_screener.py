#!/usr/bin/env python3
import argparse
import sys, os
import logging
import sqlite3
import pandas as pd

# add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..','..')))
from screeners.momentum import filter_by_momentum

# --- logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(name)s │ %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def inspect_table(db_path: str, table: str):
    """Show schema (column names) and first few rows of a table."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # get column names
    cur.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]
    logger.debug(f"⎡{table}⎤ columns: {cols}")

    # print first 5 rows
    try:
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 5;", conn, parse_dates=['Date'])
        logger.debug(f"⎡{table}⎤ sample rows:\n{df}")
    except Exception as e:
        logger.warning(f"Could not fetch sample rows: {e}")
    finally:
        conn.close()
    return cols

def main():
    parser = argparse.ArgumentParser(
        description="Run momentum screener against your normalized price table"
    )
    parser.add_argument("--db-path","-d", type=str, default="../quant_pipeline.db")
    parser.add_argument("--table","-t", default="price_data_normalized")
    parser.add_argument("--lookback-days","-l", type=int, default=90)
    parser.add_argument("--threshold","-r", type=float, default=0.1)
    args = parser.parse_args()

    logger.debug(f"CLI args: {args!r}")

    cols = inspect_table(args.db_path, args.table)

    # fail fast if normalized column is missing
    required_cols = {'Ticker','Date','Close_Normalized','Normalized'}
    if not (required_cols & set(cols)):
        logger.error(
            "❌ Your table is missing any normalized column! "
            f"Found: {cols}. Expected one of: {['Close_Normalized','Normalized']}")
        sys.exit(1)

    try:
        tickers = filter_by_momentum(
            db_path=args.db_path,
            table=args.table,
            lookback_days=args.lookback_days,
            threshold=args.threshold
        )
        logger.info(f"Momentum filter passed {len(tickers)} tickers.")
    except Exception:
        logger.exception("Screener failed during SQL execution")
        sys.exit(1)

    if not tickers:
        print("No tickers passed the momentum filter.")
    else:
        print("Tickers passing momentum filter:")
        for t in tickers:
            print(f"  • {t}")

if __name__ == "__main__":
    main()
