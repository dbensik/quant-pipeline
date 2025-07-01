import logging
import pandas as pd
import sqlite3
import requests
import warnings
from scripts.crypto_meta import load_crypto_meta
from scripts.crypto_meta import _CRYPTO_ID_MAP

logger = logging.getLogger(__name__)


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
        import requests

    def get_crypto_market_caps(ids: list[str]) -> dict[str,float]:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        resp = requests.get(url, params={
          "vs_currency":"usd",
          "ids":",".join(ids),
          "sparkline":False
        }, timeout=10)
        resp.raise_for_status()
        return {c["id"]: c["market_cap"] for c in resp.json()}


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
            warnings.filterwarnings("ignore", category=FutureWarning)
            yf_df = yf.download(pair, start=self.start_date, end=self.end_date, progress=False, auto_adjust=True)
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

    def write_universe(self, crypto_pairs, crypto_caps):
        # crypto_meta = load_crypto_meta(self.pairs)
        rows = []
        for t in crypto_pairs:
            rows.append({
                "Ticker": t,
                "AssetClass": "Crypto",
                "Sector": None,
                "MarketCap": crypto_caps.get(t)
            })
        df_crypto = pd.DataFrame(rows)
        # df_crypto = pd.DataFrame([{
        #     'Ticker': pair,
        #     'AssetClass': 'Crypto',
        #     'Sector': None,
        #     # 'MarketCap': crypto_meta.get(pair, {}).get('market_cap')
        #     'MarketCap':.get(t)
        # } for pair in crypto_pairs])

        # 3) combine and persist
        # df_assets = pd.concat([df_eq, df_crypto], ignore_index=True)  # if df_eq available here

        conn = sqlite3.connect(self.db_path)
        # df_assets.to_sql('assets', conn, if_exists='replace', index=False)
        conn.execute("""
          CREATE TABLE IF NOT EXISTS assets (
            Ticker TEXT PRIMARY KEY,
            AssetClass TEXT NOT NULL,
            Sector TEXT,
            MarketCap REAL
          );
        """)
        df_crypto.to_sql("assets", conn, if_exists="append", index=False)
        conn.close()
        logger.info("✅ Written crypto universe to DB")

