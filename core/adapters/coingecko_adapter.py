import logging
from datetime import datetime, timezone
from typing import List

import requests

from core.models import Asset, MarketDataRecord, OHLCV, Timestamp

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.coingecko.com/api/v3"


def fetch(
    coin_ids: List[str],
    vs_currency: str = "usd",
    days: int = 90,
) -> List[MarketDataRecord]:
    """
    Fetch OHLCV crypto data from the CoinGecko public API.

    Args:
        coin_ids:    CoinGecko coin IDs, e.g. ['bitcoin', 'ethereum'].
        vs_currency: Quote currency (default 'usd').
        days:        Number of historical days to fetch (default 90).

    Returns:
        List of MarketDataRecord, one per (coin, date) row.
    """
    records: List[MarketDataRecord] = []

    for coin_id in coin_ids:
        url = f"{_BASE_URL}/coins/{coin_id}/ohlc"
        params = {"vs_currency": vs_currency, "days": days}

        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            rows = resp.json()  # [[timestamp_ms, open, high, low, close], ...]
        except Exception as e:
            logger.error(f"CoinGecko request failed for {coin_id}: {e}")
            continue

        symbol = coin_id.upper()
        for row in rows:
            ts_ms, open_, high, low, close = row
            ts = Timestamp(utc=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))
            records.append(
                MarketDataRecord(
                    asset=Asset(
                        symbol=symbol,
                        asset_class="crypto",
                        source="coingecko",
                        metadata={"vs_currency": vs_currency},
                    ),
                    ohlcv=OHLCV(
                        open=float(open_),
                        high=float(high),
                        low=float(low),
                        close=float(close),
                        volume=0.0,  # CoinGecko OHLC endpoint does not return volume
                        timestamp=ts,
                    ),
                )
            )

    logger.info(f"coingecko_adapter: fetched {len(records)} records.")
    return records