import pandas as pd
import sqlite3
from sklearn.preprocessing import StandardScaler
import argparse

class TimeSeriesNormalizer:
    def __init__(self, db_path, table_name='price_data'):
        self.db_path = db_path
        self.table_name = table_name

    def load_data(self, tickers):
        """
        Load price data for given tickers from the database.

        Parameters:
            tickers (list): List of ticker symbols to load.

        Returns:
            pd.DataFrame: Combined price data for all tickers.
        """
        conn = sqlite3.connect(self.db_path)
        placeholders = ', '.join('?' for _ in tickers)
        query = f"SELECT * FROM {self.table_name} WHERE Ticker IN ({placeholders})"
        df = pd.read_sql(query, conn, params=tickers)
        conn.close()

        # Ensure datetime format and sorting
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(['Date', 'Ticker'], inplace=True)
        return df

    def normalize_close_prices(self, df):
        """
        Normalize the 'Close' price using z-score for each ticker.

        Parameters:
            df (pd.DataFrame): Price data including 'Close' and 'Ticker'.

        Returns:
            pd.DataFrame: DataFrame with an added 'Close_Normalized' column.
        """
        result = []
        for ticker, group in df.groupby('Ticker'):
            group = group.copy()
            scaler = StandardScaler()
            group['Close_Normalized'] = scaler.fit_transform(group[['Close']])
            result.append(group)

        normalized_df = pd.concat(result).sort_values(['Date', 'Ticker'])
        return normalized_df

    def get_normalized_close_data(self, tickers):
        """
        Public method to get normalized close prices for specified tickers.

        Parameters:
            tickers (list): List of tickers.

        Returns:
            pd.DataFrame: Normalized data.
        """
        raw_df = self.load_data(tickers)
        normalized_df = self.normalize_close_prices(raw_df)
        return normalized_df

    def normalize_and_save(self, tickers, normalized_table_name='price_data_normalized'):
        """
        Normalize close prices and save to a new table in the same database.

        Parameters:
            tickers (list): List of tickers to normalize.
            normalized_table_name (str): Table name to store normalized data.
        """
        normalized_df = self.get_normalized_close_data(tickers)
        conn = sqlite3.connect(self.db_path)
        normalized_df.to_sql(normalized_table_name, conn, if_exists='replace', index=False)
        conn.close()

    
    def normalize_from_cli(df_dict):
        normalized = {}
        base = min(df.index.min() for df in df_dict.values())
        end = max(df.index.max() for df in df_dict.values())
        idx = pd.date_range(start=base, end=end, freq='D')

        for name, df in df_dict.items():
            df = df.reindex(idx).ffill().dropna()
            normalized[name] = (df['Close'] - df['Close'].iloc[0]) / df['Close'].iloc[0]
        return normalized


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize close prices in SQLite DB.")
    parser.add_argument('--db_path', type=str, required=True, help='Path to SQLite database.')
    parser.add_argument('--tickers', type=str, required=True, help='Comma-separated list of tickers.')
    parser.add_argument('--output_table', type=str, default='price_data_normalized', help='Name of output table.')

    args = parser.parse_args()
    tickers = [ticker.strip() for ticker in args.tickers.split(',')]

    normalizer = TimeSeriesNormalizer(args.db_path)
    normalizer.normalize_and_save(tickers, normalized_table_name=args.output_table)

    print(f"Normalized data for {tickers} saved to table '{args.output_table}'.")
