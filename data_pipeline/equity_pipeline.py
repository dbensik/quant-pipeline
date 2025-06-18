import yfinance as yf
import pandas as pd
import sqlite3
import logging
import time

class EquityPipeline:
    def __init__(self, ticker, start_date, end_date, session):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.data = None
        self.session = session

    def fetch_data(self, session=None):
        print(f"Fetching data for {self.ticker} from {self.start_date} to {self.end_date}...")
        self.data = yf.download(self.ticker, start=self.start_date, end=self.end_date)
        print("Columns returned:", self.data.columns)
        return self.data

    def clean_data(self):
        if self.data is None:
            raise ValueError("No data found. Run fetch_data() first.")
        print("Cleaning data...")
        self.data.dropna(inplace=True)
        self.data = self.data[['Open', 'High', 'Low', 'Close', 'Volume']]
        self.validate_data()
        print("Columns after cleaning:", self.data.columns)
        return self.data

    def save_data(self, db_path='../quant_pipeline.db', table_name='price_data'):
        if self.data is None:
            raise ValueError("No data to save. Run fetch_data() and clean_data() first.")

        if isinstance(self.data.columns, pd.MultiIndex):
            self.data.columns = self.data.columns.get_level_values('Price')

        self.data = self.data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        self.data = self.data.reset_index()

        print(f"Saving data to database: {db_path} in table: {table_name} ...")
        conn = sqlite3.connect(db_path)
        self.data.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        print(f"Data saved to {db_path} in table '{table_name}'.")

    def query_data(self, query, db_path='../quant_pipeline.db'):
        print(f"Running query: {query}")
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def validate_data(self):
        if self.data is None:
            raise ValueError("No data to validate.")
        if self.data.isnull().any().any():
            raise ValueError("Data contains null values after cleaning.")
        if not isinstance(self.data.index, pd.DatetimeIndex):
            raise ValueError("Index is not a DatetimeIndex.")
        print("Data validation passed.")

    def fetch_batch_data(self, tickers, db_path, table_name, session=None):
        logger = logging.getLogger(__name__)
        for ticker in tickers:
            try:
                logger.info(f"Attempting to fetch: {ticker}")
                self.ticker = ticker
                data = yf.download(ticker, start=self.start_date, end=self.end_date, session=self.session)
                if data.empty:
                    logger.warning(f"No data returned for {ticker}")
                    continue
                self.data = data
                self.clean_data()
                self.save_data(db_path=db_path, table_name=table_name)
            except Exception as e:
                logger.error(f"Failed to process {ticker}: {e}")
