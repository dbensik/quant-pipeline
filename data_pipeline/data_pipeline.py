import yfinance as yf
import pandas as pd
import sqlite3
import logging

logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self, ticker, start_date, end_date):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.data = None

    def fetch_data(self):
        logger.info(f"Fetching data for {self.ticker} from {self.start_date} to {self.end_date}...")
        self.data = yf.download(self.ticker, start=self.start_date, end=self.end_date)
        logger.info(f"Columns returned: {self.data.columns.tolist()}")
        return self.data

    def clean_data(self):
        if self.data is None:
            raise ValueError("No data found. Run fetch_data() first.")

        logger.info("Cleaning data...")
        self.data.dropna(inplace=True)

        # Select relevant columns; adjust as needed
        self.data = self.data[['Open', 'High', 'Low', 'Close', 'Volume']]

        self.validate_data()
        logger.info(f"Columns after cleaning: {self.data.columns.tolist()}")
        return self.data

    def validate_data(self):
        """
        Ensures data integrity after cleaning.
        Raises ValueError if any validation condition fails.
        """
        if self.data is None:
            raise ValueError("No data to validate.")
        if self.data.isnull().any().any():
            raise ValueError("Data contains null values after cleaning.")
        if not isinstance(self.data.index, pd.DatetimeIndex):
            raise ValueError("Index is not a DatetimeIndex.")
        logger.info("Data validation passed.")

    def save_data(self, db_path='../quant_pipeline.db', table_name='price_data'):
        """
        Saves the cleaned data to an SQLite database.
        """
        if self.data is None:
            raise ValueError("No data to save. Run fetch_data() and clean_data() first.")

        if isinstance(self.data.columns, pd.MultiIndex):
            self.data.columns = self.data.columns.get_level_values(0)

        self.data['Ticker'] = self.ticker
        self.data = self.data.reset_index()  # Make 'Date' a column

        logger.info(f"Saving data to database: {db_path}, table: {table_name}...")
        conn = sqlite3.connect(db_path)
        self.data.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Data saved to {db_path} in table '{table_name}'.")

    def query_data(self, query, db_path='../quant_pipeline.db'):
        """
        Executes a SQL query on the database and returns the result as a pandas DataFrame.

        Parameters:
            query (str): The SQL query to execute.
            db_path (str): Path to the SQLite database file.

        Returns:
            pd.DataFrame: The query result.
        """
        logger.info(f"Running query: {query}")
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
