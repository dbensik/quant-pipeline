"""
api/routers/signals.py
Strategy signals for chart overlays.

SCOPE — read this before extending:
    These are UNSIGNED strategy signals: the raw generate_signals() output of a
    registered alpha model, for drawing a signal overlay on a price chart.

    They are NOT the signed signals served by services/ (gRPC -> GraphQL ->
    Ed25519 audit log). That stack remains the signed serving layer per the
    2026-07-31 decision "FastAPI for React UI alongside gRPC-GraphQL serving
    layer". Do not add signing, hashing or audit-log writes here — that would
    re-open the architectural conflict that decision resolved. A consumer that
    needs a verifiable signal should call the GraphQL gateway.

Phase 3 — FastAPI routers for the React UI
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo
from api.frames import records_to_frame

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


class SignalPoint(BaseModel):
    time: datetime
    signal: Optional[float] = Field(
        description="+1 long, -1 short/exit, 0 flat. Null where the strategy is still warming up."
    )
    close: Optional[float] = Field(default=None, description="Close price at that bar")


class SignalsResponse(BaseModel):
    symbol: str
    strategy_id: str
    strategy_name: str
    start: datetime
    end: datetime
    count: int
    params: Dict[str, Any]
    signed: bool = Field(
        default=False,
        description=(
            "Always false. These are unsigned strategy signals; signed signals "
            "are served by the gRPC/GraphQL layer, not this API."
        ),
    )
    caveat: Optional[str] = None
    signals: List[SignalPoint]


def _generate_sync(frame: pd.DataFrame, spec: registry.StrategySpec, params: Dict[str, Any]):
    """Strategy construction + signal generation — CPU-bound, kept off the event loop."""
    model = spec.build(params)
    return model.generate_signals(price_data=frame)


@router.get(
    "/{symbol}",
    response_model=SignalsResponse,
    summary="Unsigned strategy signals for a symbol",
    responses={
        404: {"description": "Unknown symbol or strategy"},
        422: {"description": "Invalid parameters, bad date range, or no data"},
    },
)
async def get_signals(
    symbol: str = Path(description="Ticker as stored, e.g. 'AAPL'"),
    strategy_id: str = Query(description="Registry id, e.g. 'ma_crossover'"),
    start: datetime = Query(description="Inclusive range start (ISO 8601)"),
    end: datetime = Query(description="Inclusive range end (ISO 8601)"),
    include_close: bool = Query(
        default=True, description="Include the Close price alongside each signal"
    ),
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> SignalsResponse:
    """
    Run a strategy over the range and return its per-bar signal.

    Strategy parameters use registry defaults. For non-default parameters, POST
    to /api/v1/backtest — its response carries the same signal in the equity
    curve, alongside the resulting KPIs.
    """
    if start > end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    try:
        spec = registry.get(strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    if spec.input_contract == "multi":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Strategy '{spec.id}' takes a multi-symbol frame and cannot "
                "generate signals for a single symbol."
            ),
        )

    asset = await repo.find_asset(symbol)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol!r}")

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    records = await repo.fetch_range(
        symbol=symbol, asset_class=None, start=start, end=end
    )
    frame = records_to_frame(records)
    if frame.empty:
        raise HTTPException(
            status_code=422,
            detail=f"No bars stored for {symbol!r} between {start.date()} and {end.date()}.",
        )

    try:
        signals = await run_in_threadpool(_generate_sync, frame, spec, {})
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    closes = frame["Close"]
    points: List[SignalPoint] = []
    for idx, value in signals["signal"].items():
        close = closes.get(idx)
        points.append(
            SignalPoint(
                time=idx.to_pydatetime(),
                # NaN is not valid JSON; the Backtester ffill/fillna(0)s these,
                # but the raw series legitimately has them during warm-up.
                signal=None if pd.isna(value) else float(value),
                close=(
                    None
                    if (not include_close or close is None or pd.isna(close))
                    else float(close)
                ),
            )
        )

    return SignalsResponse(
        symbol=asset.symbol,
        strategy_id=spec.id,
        strategy_name=spec.display_name,
        start=start,
        end=end,
        count=len(points),
        params={p.name: p.default for p in spec.params},
        caveat=spec.caveat,
        signals=points,
    )
