# eda.py
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

class EDA:
    def __init__(self, filepath=None, parse_dates=['Date'], index_col='Date'):
        """
        Initialize the EDA module.
        - If a filepath is provided, use it to load data from a CSV.
        - Otherwise, you can load data from the database.
        """
        self.filepath = filepath
        self.parse_dates = parse_dates
        self.index_col = index_col
        self.data = None

    def load_data(self):
        """
        Load data from a CSV file.
        """
        if not self.filepath:
            raise ValueError("File path not provided.")
        self.data = pd.read_csv(self.filepath, parse_dates=self.parse_dates, index_col=self.index_col)
        return self.data

    def load_data_from_db(self, db_path='quant_pipeline.db', table_name='price_data'):
        """
        Load data from an SQLite database.
        
        Parameters:
            db_path (str): Path to the SQLite database file.
            table_name (str): Name of the table to load data from.
            
        Returns:
            pd.DataFrame: The loaded data.
        """
        conn = sqlite3.connect(db_path)
        self.data = pd.read_sql(f"SELECT * FROM {table_name}", conn, parse_dates=self.parse_dates, index_col=self.index_col)
        conn.close()
        return self.data

    def clean_data(self):
        """
        Clean the data by dropping rows with missing values.
        """
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data() or load_data_from_db() first.")
        self.data.dropna(inplace=True)
        return self.data

    def compute_summary_stats(self):
        """
        Compute and return summary statistics for the data.
        """
        if self.data is None:
            raise ValueError("Data not loaded.")
        return self.data.describe()

    def add_moving_average(self, window=20):
        """
        Add a moving average column to the DataFrame.
        """
        if self.data is None:
            raise ValueError("Data not loaded.")
        ma_column = f'MA_{window}'
        self.data[ma_column] = self.data['Close'].rolling(window=window).mean()
        return self.data

    def plot_time_series(self, column='Close', title="Price Over Time"):
        """
        Plot the specified column as a time series.
        """
        if self.data is None:
            raise ValueError("Data not loaded.")
        plt.figure(figsize=(10,6))
        plt.plot(self.data.index, self.data[column], label=column)
        plt.title(title)
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.legend()
        plt.show()

class MLDataProvider:
    """
    Placeholder class to provide data for machine learning models from the database.
    """
    def __init__(self, db_path='quant_pipeline.db', table_name='price_data', parse_dates=['Date'], index_col='Date'):
        self.db_path = db_path
        self.table_name = table_name
        self.parse_dates = parse_dates
        self.index_col = index_col

    def get_data(self):
        """
        Retrieve data from the specified SQLite database and table.
        
        Returns:
            pd.DataFrame: The loaded data.
        """
        conn = sqlite3.connect(self.db_path)
        data = pd.read_sql(f"SELECT * FROM {self.table_name}", conn, parse_dates=self.parse_dates, index_col=self.index_col)
        conn.close()
        return data

# Example usage (for debugging/testing purposes)
if __name__ == "__main__":
    # Example: Loading data from a CSV file
    try:
        eda_instance = EDA(filepath="../data/sample_data.csv")
        data_csv = eda_instance.load_data()
        eda_instance.clean_data()
        print("Summary statistics from CSV:")
        print(eda_instance.compute_summary_stats())
        eda_instance.add_moving_average(window=20)
        eda_instance.plot_time_series(title="CSV Data: Closing Price")
    except Exception as e:
        print("CSV load error:", e)
    
    # Example: Loading data from the SQLite database
    try:
        eda_instance_db = EDA()
        data_db = eda_instance_db.load_data_from_db(db_path="../quant_pipeline.db", table_name="price_data")
        print("Data loaded from DB:")
        print(data_db.head())
    except Exception as e:
        print("DB load error:", e)
    
    # Example: Using the MLDataProvider placeholder
    try:
        ml_provider = MLDataProvider(db_path="../quant_pipeline.db", table_name="price_data")
        ml_data = ml_provider.get_data()
        print("Data from MLDataProvider:")
        print(ml_data.head())
    except Exception as e:
        print("MLDataProvider error:", e)