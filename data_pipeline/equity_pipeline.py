import yfinance as yf
import pandas as pd
import sqlite3
import logging
import warnings
from scripts.constituents import load_sp500

logger = logging.getLogger(__name__)

class EquityPipeline:
    def __init__(self, tickers, start_date, end_date, session=None, db_path='../quant_pipeline.db', table_name='price_data'):
        """
        Parameters:
            tickers (list[str]): List of equity tickers to fetch.
            start_date (str): ISO start date, e.g. '2018-01-01'.
            end_date (str): ISO end date, e.g. '2024-12-31'.
            session: optional requests session for yfinance.
            db_path (str): Path to sqlite database.
            table_name (str): Table in which to store raw price data.
        """
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.session = session
        self.db_path = db_path
        self.table_name = table_name

    @staticmethod
    def get_equity_market_cap(ticker: str) -> float | None:
        t = yf.Ticker(ticker)
        return t.fast_info.get("market_cap") or t.info.get("marketCap")

    def fetch_single(self, ticker):
        """
        Download raw OHLCV data for one ticker.
        Returns a DataFrame indexed by Date with columns ['Open','High','Low','Close','Volume'].
        """
        logger.info(f"Fetching data for ticker: {ticker}")
        warnings.filterwarnings("ignore", category=FutureWarning)
        df = yf.download(tickers=ticker,
                         start=self.start_date,
                         end=self.end_date,
                         session=self.session,
                         progress=False,
                         auto_adjust=True)
        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()
        # Ensure datetime index
        df.index = pd.to_datetime(df.index)
        # Flatten MultiIndex cols if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df[['Open','High','Low','Close','Volume']]

    def fetch_batch_data(self):
        """
        Fetch, clean, and save data for all tickers in one run.
        """
        # Reset table before writing
        # conn = sqlite3.connect(self.db_path)
        # conn.execute(f"DROP TABLE IF EXISTS {self.table_name};")
        # conn.close()
        # logger.info(f"Resetting table '{self.table_name}' in {self.db_path}")

        for ticker in self.tickers:
            try:
                df = self.fetch_single(ticker)
                if df.empty:
                    continue
                cleaned = self.clean_data(df)
                self.save_data(cleaned, ticker)
            except Exception as e:
                logger.error(f"Failed to process {ticker}: {e}")
        logger.info("✅ Equity data fetched & saved.")

    def clean_data(self, df):
        """
        Drop NaNs, remove duplicate dates, and ensure valid index.
        """
        before = len(df)
        df = df.dropna()
        df = df[~df.index.duplicated(keep='first')]
        after = len(df)
        logger.info(f"Cleaned {before-after} NaNs/dupes; {after} remain for this ticker.")
        # Validate index
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Index is not a DatetimeIndex.")
        return df

    def save_data(self, df, ticker):
        """
        Append cleaned DataFrame to SQLite with a 'Date' column and 'Ticker'.
        """
        out = df.copy().reset_index()
        out.rename(columns={'index': 'Date'}, inplace=True)
        out['Ticker'] = ticker
        # Write to SQL
        conn = sqlite3.connect(self.db_path)
        out.to_sql(self.table_name, conn, if_exists='append', index=False)
        conn.close()
        logger.info(f"Saved {ticker}: {len(out)} rows to '{self.table_name}'")

    def query_data(self, ticker=None):
        """
        Query raw data for a ticker or full table. Returns DataFrame indexed by Date.
        """
        q = f"SELECT * FROM {self.table_name}"
        if ticker:
            q += f" WHERE Ticker = '{ticker}'"
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(q, conn, parse_dates=['Date'])
        conn.close()
        if 'Date' in df.columns:
            df.set_index('Date', inplace=True)
        return df

    def write_universe(self, eq_tickers, eq_sectors):
        """Load S&P500 constituents, get their sectors+market_caps, write to `assets`."""
        eq_tickers, eq_sectors = load_sp500()
        rows = []
        for t in eq_tickers:
            rows.append({
                "Ticker": t,
                "AssetClass": "Equity",
                "Sector": eq_sectors.get(t),
                "MarketCap": self.get_equity_market_cap(t)
            })
        df_eq = pd.DataFrame(rows)
        
        # persist
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
          CREATE TABLE IF NOT EXISTS assets (
            Ticker TEXT PRIMARY KEY,
            AssetClass TEXT NOT NULL,
            Sector TEXT,
            MarketCap REAL
          );
        """)
        conn.execute(f"DELETE FROM assets")
        df_eq.to_sql("assets", conn, if_exists="append", index=False)
        conn.close()
        logger.info("✅ Written equity universe to DB")
