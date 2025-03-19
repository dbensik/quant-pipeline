# data_pipeline.py

import yfinance as yf
import pandas as pd

class DataPipeline:
    def __init__(self, ticker, start_date, end_date):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.data = None

    def fetch_data(self):
        print(f"Fetching new data for {self.ticker} from {self.start_date} to {self.end_date}...")
        self.data = yf.download(self.ticker, start=self.start_date, end=self.end_date)
        # Print the columns that were returned
        print("Columns returned from data source:", self.data.columns)
        return self.data

    def clean_data(self):
        if self.data is None:
            raise ValueError("No data found. Run fetch_data() first.")
        print("Cleaning data...")
        self.data.dropna(inplace=True)
        self.data = self.data[['Open', 'High', 'Low', 'Close', 'Volume']]
        return self.data

    def save_data(self, path='../data/raw_data.csv'):
        if self.data is None:
            raise ValueError("No data to save. Run fetch_data() and clean_data() first.")
        print(f"Saving data to {path}...")
        self.data.to_csv(path)
