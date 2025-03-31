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
        print("Columns after cleaning:", self.data.columns)
        return self.data

    def save_data(self, db_path='../quant_pipeline.db', table_name='price_data'):
        """
        Saves the cleaned data to an SQLite database.
        """
        if self.data is None:
            raise ValueError("No data to save. Run fetch_data() and clean_data() first.")
        
        # Flatten MultiIndex columns if present
        if isinstance(self.data.columns, pd.MultiIndex):
            # Option: use only the first level (e.g., 'Price') for column names
            self.data.columns = self.data.columns.get_level_values('Price')
        
        # Add Ticker column to support multiple tickers
        self.data['Ticker'] = self.ticker

        # Reset index so that the Date index becomes a regular column
        self.data = self.data.reset_index()  # 'Date' becomes a column here
        
        print(f"Saving data to database: {db_path} in table: {table_name} ...")
        # Connect to (or create) the SQLite database
        conn = sqlite3.connect(db_path)
        # Save data using 'replace' (overwrite) or 'append' to add new rows
        self.data.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data saved to {db_path} in table '{table_name}'.")

    
    def query_data(self, query, db_path='../quant_pipeline.db'):
        """
        Executes a SQL query on the database and returns the result as a pandas DataFrame.
        
        Parameters:
          query (str): The SQL query to execute.
          db_path (str): Path to the SQLite database file.
        
        Returns:
          pd.DataFrame: The query result.
        """
        print(f"Running query: {query}")
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(query, conn)
        conn.close()
        return df