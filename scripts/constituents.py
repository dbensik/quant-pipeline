import pandas as pd
import logging
import requests

logger = logging.getLogger(__name__)


def load_sp500():
    """
    Fetches the current list of S&P 500 constituents and their sectors
    by scraping the Wikipedia page.

    Returns:
        tickers (list of str): List of ticker symbols (e.g. ['AAPL', 'MSFT', ...]).
        sectors (dict): Mapping from ticker symbol to GICS Sector (e.g. {'AAPL':'Information Technology', ...}).

    Raises:
        Exception if scraping/parsing fails.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        # read_html returns a list of DataFrames; the first table contains the constituents
        tables = pd.read_html(url)
        df = tables[0]

        # Ensure expected columns exist
        if 'Symbol' not in df.columns or 'GICS Sector' not in df.columns:
            raise ValueError(f"Unexpected table format on {url}")

        tickers = df['Symbol'].tolist()
        sectors = df.set_index('Symbol')['GICS Sector'].to_dict()
        logger.info(f"Loaded {len(tickers)} S&P 500 tickers from Wikipedia.")
        return tickers, sectors

    except Exception as e:
        logger.error(f"Failed to load S&P 500 constituents: {e}")
        raise

def load_top_crypto_pairs(vs_currency: str = "usd", top_n: int = 100):
    """
    Fetch the top-N coins by market cap from CoinGecko and
    return:
      - a list of symbols in the form ["BTC-USD","ETH-USD",...]
      - a dict mapping each pair → market_cap (as float)

    Params:
      vs_currency: quote currency (almost always 'usd')
      top_n: how many of the top market-cap coins to fetch
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": top_n,
        "page": 1,
        "sparkline": False,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch top {top_n} cryptos: {e}")
        return [], {}

    pairs = []
    market_caps = {}
    for coin in data:
        sym = coin["symbol"].upper()
        pair = f"{sym}-{vs_currency.upper()}"
        pairs.append(pair)
        market_caps[pair] = float(coin.get("market_cap", 0) or 0)

    return pairs, market_caps

if __name__ == "__main__":
    # Quick test
    tickers, sectors = load_sp500()
    print(f"Loaded {len(tickers)} tickers")
    print("First 5:", tickers[:5])
    print("Example sector mapping:", {tickers[0]: sectors[tickers[0]]})
