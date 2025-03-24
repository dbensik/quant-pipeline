import yfinance as yf
import pandas as pd
import sqlite3

class DataPipeline:
    def __init__(self, ticker, start_date, end_date):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.data = None

    def fetch_data(self):
        print(f"Fetching data for {self.ticker} from {self.start_date} to {self.end_date}...")
        self.data = yf.download(self.ticker, start=self.start_date, end=self.end_date)
        print("Columns returned:", self.data.columns)
        return self.data

    def clean_data(self):
        if self.data is None:
            raise ValueError("No data found. Run fetch_data() first.")
        print("Cleaning data...")
        self.data.dropna(inplace=True)
        # Select relevant columns; adjust as needed
        self.data = self.data[['Open', 'High', 'Low', 'Close', 'Volume']]
        return self.data

    def save_data(self, db_path='../quant_pipeline.db', table_name='price_data'):
        """
        Saves the cleaned data to an SQLite database.
        """
        if self.data is None:
            raise ValueError("No data to save. Run fetch_data() and clean_data() first.")
        
        # Add Ticker column to support multiple tickers
        self.data['Ticker'] = self.ticker
        
        print(f"Saving data to database: {db_path} in table: {table_name} ...")
        # Connect to (or create) the SQLite database
        conn = sqlite3.connect(db_path)
        # Save data using 'replace' (overwrite) or 'append' to add new rows
        self.data.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data saved to {db_path} in table '{table_name}'.")