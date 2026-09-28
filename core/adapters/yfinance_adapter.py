import logging
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
import yfinance as yf

from core.models import Asset, MarketDataRecord, OHLCV, Timestamp

logger = logging.getLogger(__name__)


def fetch(
    symbols: List[str],
    start_date: str,
    end_date: str,
    interval: str = "1d",
    raise_errors: bool = False,
) -> List[MarketDataRecord]:
    """
    Fetch OHLCV equity data from Yahoo Finance.

    Args:
        symbols:    Ticker symbols, e.g. ['AAPL', 'MSFT'].
        start_date: Start date in YYYY-MM-DD format.
        end_date:   End date in YYYY-MM-DD format.
        interval:   yfinance interval string (default '1d').
        raise_errors: Re-raise a download failure instead of returning []. An
                    empty list otherwise means both "Yahoo has nothing" and
                    "the request failed", and a backfill must not mistake a
                    rate limit for a symbol Yahoo no longer serves.

    Returns:
        List of MarketDataRecord, one per (symbol, date) row.
    """
    clean = [s.replace(".", "-") for s in symbols]
    fetched_at = datetime.now(timezone.utc)

    # ONE unadjusted download serves both bases. Scaling open/high/low/close by
    # Adj Close / Close is exactly what auto_adjust=True does — checked
    # 2026-09-27 on MO, NVDA, HWM and BTC-USD: identical to the bit, volume
    # untouched. So `ohlcv` is unchanged for every existing reader, and
    # `served` is what read-time adjustment needs. See core/price_adjustment.py.
    try:
        raw = yf.download(
            tickers=clean,
            start=start_date,
            end=end_date,
            interval=interval,
            progress=False,
            auto_adjust=False,
            actions=True,
        )
    except Exception as e:
        if raise_errors:
            raise
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
    for row in long.to_dict("records"):
        ts = Timestamp(utc=_to_utc(row["Date"]))
        served = OHLCV(
            open=float(row["Open"]),
            high=float(row["High"]),
            low=float(row["Low"]),
            close=float(row["Close"]),
            volume=float(row["Volume"]),
            timestamp=ts,
        )
        # Absent only from hand-built frames; the real download always has it.
        ratio = (
            float(row["Adj Close"]) / served.close
            if "Adj Close" in row and served.close
            else 1.0
        )
        records.append(
            MarketDataRecord(
                asset=Asset(symbol=row["Ticker"], asset_class="equity", source="yfinance"),
                ohlcv=OHLCV(
                    open=served.open * ratio,
                    high=served.high * ratio,
                    low=served.low * ratio,
                    close=served.close * ratio,
                    volume=served.volume,
                    timestamp=ts,
                ),
                served=served,
                fetched_at=fetched_at,
                dividend=_positive(row.get("Dividends")),
                split_ratio=_positive(row.get("Stock Splits")),
            )
        )

    logger.info(f"yfinance_adapter: fetched {len(records)} records.")
    return records


def _positive(value) -> Optional[float]:
    """yfinance reports 'no action' as 0.0 and a missing cell as NaN."""
    if value is None:
        return None
    value = float(value)
    return value if value > 0 else None


def _to_utc(dt) -> datetime:
    if hasattr(dt, "to_pydatetime"):
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt