"""
api/routers/assets.py
Asset registry endpoints — what symbols exist and what is known about them.

Phase 3 — FastAPI routers for the React UI
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import AssetORM, MarketDataORM

from api.dependencies import get_db

router = APIRouter(prefix="/api/v1/assets", tags=["assets"])


class AssetSummary(BaseModel):
    symbol: str
    asset_class: str
    source: str
    metadata: dict = Field(default_factory=dict)


class AssetDetail(AssetSummary):
    """Adds coverage, which requires an aggregate over market_data."""

    bar_count: int = Field(description="Number of stored bars")
    first_bar: Optional[str] = Field(default=None, description="Earliest bar timestamp")
    last_bar: Optional[str] = Field(default=None, description="Latest bar timestamp")


class AssetListResponse(BaseModel):
    count: int
    assets: List[AssetSummary]


@router.get(
    "",
    response_model=AssetListResponse,
    summary="List registered assets",
)
async def list_assets(
    asset_class: Optional[str] = Query(
        default=None, description="Filter by 'equity' or 'crypto'"
    ),
    search: Optional[str] = Query(
        default=None, description="Case-insensitive symbol substring match"
    ),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_db),
) -> AssetListResponse:
    """
    Registered assets, alphabetically by symbol.

    This is what a UI populates a symbol picker from. `count` is the number
    returned in this page, not the total matching the filter.
    """
    stmt = select(AssetORM).order_by(AssetORM.symbol)
    if asset_class:
        stmt = stmt.where(AssetORM.asset_class == asset_class)
    if search:
        stmt = stmt.where(AssetORM.symbol.ilike(f"%{search}%"))

    rows = (await session.execute(stmt.offset(offset).limit(limit))).scalars().all()
    assets = [
        AssetSummary(
            symbol=r.symbol,
            asset_class=r.asset_class,
            source=r.source,
            metadata=r.metadata_ or {},
        )
        for r in rows
    ]
    return AssetListResponse(count=len(assets), assets=assets)


@router.get(
    "/{symbol}",
    response_model=AssetDetail,
    summary="One asset, with its stored data coverage",
    responses={404: {"description": "Symbol is not in the asset registry"}},
)
async def get_asset(
    symbol: str = Path(description="Ticker as stored, e.g. 'AAPL' or 'BTC-USD'"),
    session: AsyncSession = Depends(get_db),
) -> AssetDetail:
    """
    Asset metadata plus how much history is stored for it.

    Coverage matters to a consumer before it requests a date range: five
    registered crypto tickers have zero bars because every legacy row for them
    was an empty padding bar.
    """
    asset = (
        await session.execute(select(AssetORM).where(AssetORM.symbol == symbol).limit(1))
    ).scalar_one_or_none()
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol!r}")

    coverage = (
        await session.execute(
            select(
                func.count(MarketDataORM.time),
                func.min(MarketDataORM.time),
                func.max(MarketDataORM.time),
            ).where(MarketDataORM.asset_id == asset.id)
        )
    ).one()
    bar_count, first_bar, last_bar = coverage

    return AssetDetail(
        symbol=asset.symbol,
        asset_class=asset.asset_class,
        source=asset.source,
        metadata=asset.metadata_ or {},
        bar_count=bar_count or 0,
        first_bar=first_bar.isoformat() if first_bar else None,
        last_bar=last_bar.isoformat() if last_bar else None,
    )
