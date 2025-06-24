import logging
import pandas as pd
import sqlite3
import requests

logger = logging.getLogger(__name__)

# Map simple symbols to CoinGecko IDs (override as needed)
_CRYPTO_ID_MAP = {
    'btc': 'bitcoin',
    'eth': 'ethereum',
    # add more mappings here
}

class CryptoPipeline:
    def __init__(self, pairs, start_date, end_date, session=None,
                 db_path='../quant_pipeline.db', table_name='price_data'):
        """
        Parameters:
            pairs (list[str]): List of crypto pairs (e.g., ['BTC-USD', 'ETH-USD']).
            start_date (str): ISO start date, e.g. '2018-01-01'.
            end_date (str): ISO end date, e.g. '2024-12-31'.
            session: optional requests/session for fallback APIs.
            db_path (str): Path to sqlite database.
            table_name (str): Table in which to store raw price data.
        """
        self.pairs = pairs
        self.start_date = start_date
        self.end_date = end_date
        self.session = session
        self.db_path = db_path
        self.table_name = table_name

    def fetch_data(self, pair):
        """
        Download daily 'Close' price series for a crypto pair.
        First attempts CoinGecko, then yfinance fallback.
        Returns a DataFrame indexed by Date with a 'Close' column.
        """
        symbol = pair.split('-')[0].lower()
        coin_id = _CRYPTO_ID_MAP.get(symbol, symbol)
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        params = {'vs_currency': 'usd', 'days': 'max', 'interval': 'daily'}

        # Try CoinGecko
        try:
            logger.info(f"Fetching {pair} from CoinGecko (id={coin_id})")
            resp = requests.get(url, params=params, headers={'User-Agent': 'quant-pipeline/1.0'}, timeout=10)
            resp.raise_for_status()
            prices = resp.json().get('prices', [])
            if not prices:
                raise ValueError("Empty price list from CoinGecko")
            df = pd.DataFrame(prices, columns=['timestamp', 'Close'])
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.set_index('Date')[['Close']]
            return df
        except Exception as cg_err:
            logger.warning(f"CoinGecko failed for {pair}: {cg_err}. Falling back to yfinance.")
        
        # Fallback to yfinance
        try:
            import yfinance as yf
            logger.info(f"Fetching {pair} from yfinance fallback")
            yf_df = yf.download(pair, start=self.start_date, end=self.end_date, progress=False)
            if yf_df.empty:
                logger.warning(f"yfinance returned no data for {pair}")
                return pd.DataFrame()
            yf_df.index = pd.to_datetime(yf_df.index)
            if isinstance(yf_df.columns, pd.MultiIndex):
                yf_df.columns = yf_df.columns.get_level_values(0)
            return yf_df[['Close']].copy()
        except Exception as yf_err:
            logger.error(f"yfinance fallback failed for {pair}: {yf_err}")
            return pd.DataFrame()

    def clean_data(self, df, pair):
        """
        Clean the DataFrame: drop NaNs, remove duplicates, filter by date range.
        """
        before = len(df)
        df = df.dropna()
        df = df[~df.index.duplicated(keep='first')]
        df = df.loc[self.start_date:self.end_date]
        logger.info(f"Cleaned {pair}: {before}→{len(df)} rows for {self.start_date}–{self.end_date}")
        return df

    def validate_data(self, df, pair):
        """
        Ensure df has data, no nulls, and a DateTimeIndex.
        """
        if df.empty:
            raise ValueError(f"DataFrame empty after cleaning for {pair}.")
        if df.isnull().values.any():
            raise ValueError(f"Null values present after cleaning for {pair}.")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError(f"Index not datetime for {pair}.")
        logger.info(f"Validated data for {pair}.")

    def save_data(self, df, pair):
        """
        Append cleaned DataFrame to SQLite with standardized columns.
        """
        self.validate_data(df, pair)
        out = df.reset_index().rename(columns={'Date': 'Date'})
        out['Ticker'] = pair
        # Ensure OHLCV schema consistency
        for col in ('Open','High','Low','Volume'):
            if col not in out:
                out[col] = None
        cols = ['Date','Ticker','Open','High','Low','Close','Volume']
        out = out[cols]

        conn = sqlite3.connect(self.db_path)
        out.to_sql(self.table_name, conn, if_exists='append', index=False)
        conn.close()
        logger.info(f"Saved {pair}: {len(out)} rows to '{self.table_name}'")

    def fetch_batch_data(self):
        """
        Fetch, clean, validate, and save all crypto pairs.
        """
        # Reset table
        # conn = sqlite3.connect(self.db_path)
        # conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        # conn.close()
        # logger.info(f"Resetting table '{self.table_name}' in {self.db_path}")

        for pair in self.pairs:
            try:
                raw = self.fetch_data(pair)
                if raw.empty:
                    continue
                cleaned = self.clean_data(raw, pair)
                self.save_data(cleaned, pair)
            except Exception as e:
                logger.error(f"Failed to process {pair}: {e}")
        logger.info("✅ Crypto data fetched & saved.")

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
