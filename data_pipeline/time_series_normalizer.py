import pandas as pd
import sqlite3
from sklearn.preprocessing import StandardScaler
import argparse
import logging

logger = logging.getLogger(__name__)

class TimeSeriesNormalizer:
    def __init__(self, db_path, table_name='price_data'):
        self.db_path = db_path
        self.table_name = table_name

    def load_data(self, tickers):
        """
        Load price data for given tickers from the database.
        """
        conn = sqlite3.connect(self.db_path)
        placeholders = ', '.join('?' for _ in tickers)
        query = f"SELECT * FROM {self.table_name} WHERE Ticker IN ({placeholders})"
        df = pd.read_sql(query, conn, params=tickers, parse_dates=['Date'])
        conn.close()

        df.sort_values(['Date', 'Ticker'], inplace=True)
        return df

    def normalize_close_prices(self, df):
        """
        Compute z-score normalization of 'Close' for each ticker.
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
        raw_df = self.load_data(tickers)
        norm_df = self.normalize_close_prices(raw_df)
        return norm_df

    def normalize_and_save(self, tickers, normalized_table_name='price_data_normalized'):
        """
        Normalize close prices and save to a new table with columns Date, Ticker, Normalized.
        """
        norm_df = self.get_normalized_close_data(tickers)
        # rename for screener compatibility
        if 'Close_Normalized' in norm_df.columns:
            norm_df['Normalized'] = norm_df['Close_Normalized']
        # prepare output DataFrame
        out_df = norm_df.reset_index()[['Date', 'Ticker', 'Normalized']]

        conn = sqlite3.connect(self.db_path)
        out_df.to_sql(normalized_table_name, conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Saved normalized data for tickers {tickers} to table '{normalized_table_name}' (rows: {len(out_df)})")

    @staticmethod
    def normalize_from_cli(df_dict):
        """
        Normalize closing prices for a dictionary of DataFrames indexed by date.
        Returns dict of series named 'Normalized'.
        """
        normalized = {}
        if not df_dict:
            raise ValueError("Input data dictionary is empty.")
        base = min(df.index.min() for df in df_dict.values() if not df.empty)
        end = max(df.index.max() for df in df_dict.values() if not df.empty)
        logger.info(f"Normalizing: base={base}, end={end}")
        idx = pd.date_range(start=base, end=end, freq='D')
        for name, df in df_dict.items():
            logger.info(f"{name!r} → columns={list(df.columns)}, rows={df.shape[0]}")
            if df.shape[0] == 0:
                logger.warning(f"No rows for {name!r}; skipping.")
                continue
            if 'Close' not in df.columns:
                logger.warning(f"No ‘Close’ column for {name!r}; skipping.")
                continue

            tmp = df.copy().reindex(idx).ffill().infer_objects(copy=False).dropna()
            if tmp.empty:
                logger.warning(f"After reindex/ffill, no data for {name!r}; skipping.")
                continue

            normalized[name] = (tmp['Close'] - tmp['Close'].iloc[0]) / tmp['Close'].iloc[0]

        return normalized

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize close prices in SQLite DB.")
    parser.add_argument('--db_path', type=str, required=True, help='Path to SQLite database.')
    parser.add_argument('--tickers', type=str, required=True, help='Comma-separated list of tickers.')
    parser.add_argument('--output_table', type=str, default='price_data_normalized', help='Name of output table.')
    args = parser.parse_args()
    tickers = [t.strip() for t in args.tickers.split(',')]
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    normalizer = TimeSeriesNormalizer(args.db_path)
    normalizer.normalize_and_save(tickers, normalized_table_name=args.output_table)
    print(f"Normalized data for {tickers} saved to table '{args.output_table}'.")

# import pandas as pd
# import sqlite3
# from sklearn.preprocessing import StandardScaler
# import argparse
# import logging

# logger = logging.getLogger(__name__)

# class TimeSeriesNormalizer:
#     def __init__(self, db_path, table_name='price_data'):
#         self.db_path = db_path
#         self.table_name = table_name

#     def load_data(self, tickers):
#         """
#         Load price data for given tickers from the database.

#         Parameters:
#             tickers (list): List of ticker symbols to load.

#         Returns:
#             pd.DataFrame: Combined price data for all tickers.
#         """
#         conn = sqlite3.connect(self.db_path)
#         placeholders = ', '.join('?' for _ in tickers)
#         query = f"SELECT * FROM {self.table_name} WHERE Ticker IN ({placeholders})"
#         df = pd.read_sql(query, conn, params=tickers)
#         conn.close()

#         # Ensure datetime format and sorting
#         df['Date'] = pd.to_datetime(df['Date'])
#         df.sort_values(['Date', 'Ticker'], inplace=True)
#         return df

#     def normalize_close_prices(self, df):
#         """
#         Normalize the 'Close' price using z-score for each ticker.

#         Parameters:
#             df (pd.DataFrame): Price data including 'Close' and 'Ticker'.

#         Returns:
#             pd.DataFrame: DataFrame with an added 'Close_Normalized' column.
#         """
#         result = []
#         for ticker, group in df.groupby('Ticker'):
#             group = group.copy()
#             scaler = StandardScaler()
#             group['Close_Normalized'] = scaler.fit_transform(group[['Close']])
#             result.append(group)

#         normalized_df = pd.concat(result).sort_values(['Date', 'Ticker'])
#         return normalized_df

#     def get_normalized_close_data(self, tickers):
#         """
#         Public method to get normalized close prices for specified tickers.

#         Parameters:
#             tickers (list): List of tickers.

#         Returns:
#             pd.DataFrame: Normalized data.
#         """
#         raw_df = self.load_data(tickers)
#         normalized_df = self.normalize_close_prices(raw_df)
#         return normalized_df

#     def normalize_and_save(self, tickers, normalized_table_name='price_data_normalized'):
#         """
#         Normalize close prices and save to a new table in the same database.

#         Parameters:
#             tickers (list): List of tickers to normalize.
#             normalized_table_name (str): Table name to store normalized data.
#         """
#         normalized_df = self.get_normalized_close_data(tickers)
#         conn = sqlite3.connect(self.db_path)
#         normalized_df.to_sql(normalized_table_name, conn, if_exists='replace', index=False)
#         conn.close()



#     @staticmethod
#     def normalize_from_cli(df_dict):
#         """
#         Normalize closing prices for a dict of DataFrames (indexed by date).
#         """
#         # 1. drop any completely empty DataFrames
#         df_dict = {k:v for k,v in df_dict.items() if not v.empty and 'Close' in v.columns}
#         if not df_dict:
#             raise ValueError("No valid input series.")

#         # 2. For each, drop duplicate dates and ensure datetime index
#         for name, df in df_dict.items():
#             df.index = pd.to_datetime(df.index)
#             df_dict[name] = df.loc[~df.index.duplicated(keep='first')]

#         # 3. Compute the **intersection** of all date‐indices (business days)
#         common_idx = None
#         for df in df_dict.values():
#             biz = df.index.to_series().dt.normalize().asfreq('B').index
#             common_idx = biz if common_idx is None else common_idx.intersection(biz)
#         if common_idx.empty:
#             logger.error("No common business‐day dates across tickers!")
#             return {}

#         # 4. Reindex each to only those common business days, forward‐fill, then normalize
#         normalized = {}
#         logger.info(f"Normalizing on {len(common_idx)} business days: {common_idx.min()} → {common_idx.max()}")
#         for name, df in df_dict.items():
#             s = df['Close'].reindex(common_idx).ffill().dropna()
#             if s.empty:
#                 logger.warning(f"{name}: empty after reindex/ffill")
#                 continue
#             normalized[name] = (s - s.iloc[0]) / s.iloc[0]

#         return normalized


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Normalize close prices in SQLite DB.")
#     parser.add_argument('--db_path', type=str, required=True, help='Path to SQLite database.')
#     parser.add_argument('--tickers', type=str, required=True, help='Comma-separated list of tickers.')
#     parser.add_argument('--output_table', type=str, default='price_data_normalized', help='Name of output table.')

#     args = parser.parse_args()
#     tickers = [ticker.strip() for ticker in args.tickers.split(',')]

#     normalizer = TimeSeriesNormalizer(args.db_path)
#     normalizer.normalize_and_save(tickers, normalized_table_name=args.output_table)

#     print(f"Normalized data for {tickers} saved to table '{args.output_table}'.")
