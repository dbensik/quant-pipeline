import logging
import pandas as pd
import requests
import sqlite3
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class CryptoPipeline:
    def __init__(self, pairs, start_date, end_date):
        self.pairs = pairs
        self.start_date = start_date
        self.end_date = end_date
        self.data = None

    def fetch_batch_data(self, db_path, table_name):
        """
        Fetch and store data for multiple crypto pairs.
        """
        for pair in self.pairs:
            try:
                logger.info(f"Fetching crypto data for: {pair}")
                data = self.fetch_data(pair)
                if data is not None and not data.empty:
                    data = self.clean_data(data)
                    self.validate_data(data)
                    data['Ticker'] = pair
                    self.save_data(data, db_path, table_name)
                    time.sleep(1)  # Throttle requests to avoid rate-limiting
                else:
                    logger.warning(f"No data returned for {pair}")
            except Exception as e:
                logger.error(f"Failed to fetch data for {pair}: {e}")

    def fetch_data(self, pair):
        """
        Fetch historical market data for a given crypto pair using CoinGecko API.
        """
        base_url = "https://api.coingecko.com/api/v3/coins/{}/market_chart"
        symbol = pair.split("-")[0].lower()
        url = base_url.format(symbol)

        params = {
            'vs_currency': 'usd',
            'days': 'max',
            'interval': 'daily'
        }

        response = requests.get(url, params=params)
        if response.status_code == 429:
            raise Exception("Rate limit exceeded. Please wait and retry.")
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}")

        data = response.json()
        if 'prices' not in data:
            raise Exception("Invalid data format from API")

        df = pd.DataFrame(data['prices'], columns=['timestamp', 'Close'])
        df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('Date', inplace=True)
        df.drop('timestamp', axis=1, inplace=True)

        return df

    def clean_data(self, df):
        """
        Clean the fetched crypto data by sorting, removing duplicates, and filtering by date.
        """
        df = df.sort_index()
        df = df[~df.index.duplicated(keep='first')]
        df = df.loc[(df.index >= self.start_date) & (df.index <= self.end_date)]
        df = df.dropna()
        return df

    def validate_data(self, df):
        """
        Validate cleaned data to ensure it has the required format and contents.
        """
        if df.empty:
            raise ValueError("Data is empty after cleaning.")
        if df.isnull().any().any():
            raise ValueError("Data contains null values after cleaning.")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Index is not a DatetimeIndex.")
        if 'Close' not in df.columns:
            raise ValueError("'Close' column missing from data.")
        logger.info("Data validation passed.")

    def save_data(self, data, db_path, table_name):
        """
        Save the cleaned and validated crypto data to an SQLite database.
        """
        data = data.reset_index()
        conn = sqlite3.connect(db_path)
        data.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        logger.info(f"Saved data for {data['Ticker'].iloc[0]} to {table_name}")

    def query_data(self, query, db_path):
        """
        Executes a SQL query on the database and returns the result as a pandas DataFrame.
        """
        logger.info(f"Running query: {query}")
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
