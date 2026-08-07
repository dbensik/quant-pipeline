"""
api/frames.py

Converts repository records into the pandas frames the strategy and backtest
layers expect.

Kept separate from the routers so that /backtest and /signals reuse the exact
same conversion the /ohlcv router serves — and so no router ever calls another
router over HTTP to get its data.

Phase 3 — FastAPI routers for the React UI
"""

from typing import List, Sequence

import pandas as pd

from core.models import MarketDataRecord

# Backtester and every strategy read capitalised OHLCV columns
# (backtester.py uses price_data[["Close"]]; atr_breakout uses High/Low).
# The API speaks lowercase JSON, so the boundary is here.
OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


def records_to_frame(records: Sequence[MarketDataRecord]) -> pd.DataFrame:
    """
    One symbol's records -> a DatetimeIndex'd OHLCV frame.

    Returns an empty frame with the right columns when there are no records, so
    callers can check `.empty` rather than handle None.
    """
    if not records:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    frame = pd.DataFrame(
        {
            "Open": [r.ohlcv.open for r in records],
            "High": [r.ohlcv.high for r in records],
            "Low": [r.ohlcv.low for r in records],
            "Close": [r.ohlcv.close for r in records],
            "Volume": [r.ohlcv.volume for r in records],
        },
        index=pd.DatetimeIndex(
            [r.ohlcv.timestamp.utc for r in records], name="Timestamp"
        ),
    )
    return frame.sort_index()


def frames_to_wide_close(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    {symbol: ohlcv_frame} -> a wide Close-price frame, one column per symbol.

    This is the input contract for multi-asset strategies (pairs, basket,
    cointegrated, index rebalancing), which is why the registry tracks
    input_contract — handing one of those a single-symbol frame silently
    produces nonsense rather than an error.
    """
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(
        {symbol: frame["Close"] for symbol, frame in frames.items() if not frame.empty}
    ).sort_index()


def drop_incomplete_bars(frame: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    Drop rows missing any required price column.

    The migration already excluded all-NULL padding bars, but individual gaps
    survive (e.g. a Close with no Open). Strategies that difference or roll over
    these produce NaN cascades, so callers doing maths should drop them first.
    """
    required = columns or ["Close"]
    present = [c for c in required if c in frame.columns]
    return frame.dropna(subset=present) if present else frame
