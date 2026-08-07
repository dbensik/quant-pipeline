"""
api/routers/ohlcv.py
OHLCV price-history endpoints.

The Phase 3 vertical slice: proves repository -> router -> Swagger -> Streamlit
end to end before the remaining five routers are built on the same pattern.

Phase 3 — FastAPI routers for the React UI
"""

from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field

from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo

router = APIRouter(prefix="/api/v1/ohlcv", tags=["ohlcv"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class OHLCVBar(BaseModel):
    """A single OHLCV bar. Price fields are nullable — the legacy data has gaps."""

    time: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None


class OHLCVResponse(BaseModel):
    symbol: str
    asset_class: str = Field(description="'equity' | 'crypto' | 'option' | 'future'")
    source: str = Field(description="Data provider, e.g. 'yfinance'")
    start: datetime
    end: datetime
    count: int = Field(description="Number of bars returned")
    bars: List[OHLCVBar]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get(
    "/{symbol}",
    response_model=OHLCVResponse,
    summary="Daily OHLCV bars for one symbol over a date range",
    responses={404: {"description": "Symbol is not in the asset registry"}},
)
async def get_ohlcv(
    symbol: str = Path(description="Ticker as stored, e.g. 'AAPL' or 'BTC-USD'"),
    start: datetime = Query(description="Inclusive range start (ISO 8601)"),
    end: datetime = Query(description="Inclusive range end (ISO 8601)"),
    asset_class: Optional[str] = Query(
        default=None,
        description=(
            "Optional disambiguator. Omit it — symbols are unique in the "
            "current registry, and the API deliberately does not infer asset "
            "class from the ticker."
        ),
    ),
    source: Optional[str] = Query(default=None, description="Filter by data provider"),
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> OHLCVResponse:
    """
    Return daily bars for `symbol` between `start` and `end`, ascending by time.

    404 means the symbol is not registered. A known symbol with no bars in the
    requested window returns 200 with an empty `bars` list — an empty result is
    not an error.
    """
    if start > end:
        raise HTTPException(
            status_code=422, detail="`start` must not be after `end`."
        )

    asset = await repo.find_asset(symbol, asset_class)
    if asset is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown symbol: {symbol!r}"
        )

    # market_data stores timezone-aware UTC. Naive query params are interpreted
    # as UTC rather than rejected, so ?start=2023-01-01 works as expected.
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    records = await repo.fetch_range(
        symbol=symbol,
        asset_class=asset_class,
        start=start,
        end=end,
        source=source,
    )

    return OHLCVResponse(
        symbol=asset.symbol,
        asset_class=asset.asset_class,
        source=asset.source,
        start=start,
        end=end,
        count=len(records),
        bars=[
            OHLCVBar(
                time=r.ohlcv.timestamp.utc,
                open=r.ohlcv.open,
                high=r.ohlcv.high,
                low=r.ohlcv.low,
                close=r.ohlcv.close,
                volume=r.ohlcv.volume,
            )
            for r in records
        ],
    )
