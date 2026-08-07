import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
import yfinance as yf

from core.models import Asset, MarketDataRecord, OHLCV, Timestamp

logger = logging.getLogger(__name__)


def fetch(
    symbols: List[str],
    start_date: str,
    end_date: str,
    interval: str = "1d",
) -> List[MarketDataRecord]:
    """
    Fetch OHLCV equity data from Yahoo Finance.

    Args:
        symbols:    Ticker symbols, e.g. ['AAPL', 'MSFT'].
        start_date: Start date in YYYY-MM-DD format.
        end_date:   End date in YYYY-MM-DD format.
        interval:   yfinance interval string (default '1d').

    Returns:
        List of MarketDataRecord, one per (symbol, date) row.
    """
    clean = [s.replace(".", "-") for s in symbols]

    try:
        raw = yf.download(
            tickers=clean,
            start=start_date,
            end=end_date,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        logger.error(f"yfinance download failed: {e}")
        return []

    if raw.empty:
        logger.warning("yfinance returned an empty DataFrame.")
        return []

    # Normalise to long format (Date, Ticker)
    if isinstance(raw.columns, pd.MultiIndex):
        long = (
            raw.stack(level=1, future_stack=True)
            .rename_axis(["Date", "Ticker"])
            .reset_index()
        )
    else:
        long = raw.reset_index()
        long["Ticker"] = clean[0]

    records: List[MarketDataRecord] = []
    for row in long.itertuples(index=False):
        ts = Timestamp(utc=_to_utc(row.Date))
        records.append(
            MarketDataRecord(
                asset=Asset(symbol=row.Ticker, asset_class="equity", source="yfinance"),
                ohlcv=OHLCV(
                    open=float(row.Open),
                    high=float(row.High),
                    low=float(row.Low),
                    close=float(row.Close),
                    volume=float(row.Volume),
                    timestamp=ts,
                ),
            )
        )

    logger.info(f"yfinance_adapter: fetched {len(records)} records.")
    return records


def _to_utc(dt) -> datetime:
    if hasattr(dt, "to_pydatetime"):
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt