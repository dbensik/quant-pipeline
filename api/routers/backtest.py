"""
api/routers/backtest.py
Run a registered strategy over stored history and return the results.

Phase 3 — FastAPI routers for the React UI
"""

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from backtesting.backtester import Backtester
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo
from api.frames import records_to_frame

router = APIRouter(prefix="/api/v1/backtest", tags=["backtest"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class BacktestRequest(BaseModel):
    symbol: str = Field(description="Ticker as stored, e.g. 'AAPL'")
    strategy_id: str = Field(description="Registry id, e.g. 'ma_crossover'")
    start: datetime
    end: datetime
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Strategy parameters. Omitted parameters use registry defaults.",
    )
    initial_capital: float = Field(default=100_000.0, gt=0)
    transaction_cost: float = Field(
        default=0.001, ge=0, description="Per-trade cost as a fraction, e.g. 0.001 = 10bps"
    )
    seed: Optional[int] = Field(
        default=42,
        description=(
            "Seed for simulated slippage. Defaults to 42 so the same request "
            "always returns the same result — without it, identical backtests "
            "differ run to run and saved results cannot be reproduced. Pass a "
            "different int to sample another draw, or null for unseeded "
            "(non-reproducible) behaviour."
        ),
    )
    include_equity_curve: bool = Field(
        default=True, description="Set false for a metrics-only response"
    )
    include_trades: bool = Field(default=True)


class EquityPoint(BaseModel):
    time: datetime
    total: float
    cash: float
    holdings: float
    position: float
    signal: float


class BacktestResponse(BaseModel):
    symbol: str
    strategy_id: str
    strategy_name: str
    start: datetime
    end: datetime
    bars: int = Field(description="Bars fed to the strategy")
    params: Dict[str, Any] = Field(description="Parameters actually used, defaults filled in")
    seed: Optional[int] = Field(default=None, description="Slippage seed used for this run")
    metrics: Dict[str, Any] = Field(description="KPIs from PerformanceAnalyzer")
    caveat: Optional[str] = Field(
        default=None, description="Non-null when the strategy is known to be unsound"
    )
    equity_curve: List[EquityPoint] = Field(default_factory=list)
    trades: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_safe(value: Any) -> Any:
    """
    Make numpy/pandas values JSON-encodable.

    NaN and ±Inf are not valid JSON. PerformanceAnalyzer legitimately produces
    them (e.g. Sharpe on a zero-variance curve), so they become null rather than
    emitting invalid JSON that a strict client rejects.
    """
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):  # numpy scalar -> native Python
        try:
            value = value.item()
        except (ValueError, AttributeError):
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _run_backtest_sync(
    frame: pd.DataFrame,
    spec: registry.StrategySpec,
    params: Dict[str, Any],
    initial_capital: float,
    transaction_cost: float,
    seed: Optional[int] = None,
) -> tuple:
    """
    The CPU-bound half, run off the event loop via run_in_threadpool.

    Strategy construction happens here too, because several strategies validate
    their parameters in __init__ and raise ValueError — we want that surfaced
    from the same call site as the run itself.
    """
    model = spec.build(params)
    backtester = Backtester(
        initial_capital=initial_capital,
        transaction_cost=transaction_cost,
        seed=seed,
    )
    results = backtester.run(price_data=frame, model=model)
    metrics = backtester.get_performance_metrics()
    trades = backtester.get_trade_log()
    return results, metrics, trades


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=BacktestResponse,
    summary="Run a strategy backtest over stored history",
    responses={
        404: {"description": "Unknown symbol or strategy"},
        422: {"description": "Invalid parameters, bad date range, or no data"},
    },
)
async def run_backtest(
    request: BacktestRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> BacktestResponse:
    """
    Fetch history, run the strategy, return equity curve + KPIs + trades.

    The backtest itself is CPU-bound and runs in a threadpool so a long run does
    not block the event loop and stall every other request.
    """
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    try:
        spec = registry.get(request.strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    if spec.input_contract == "multi":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Strategy '{spec.id}' takes a multi-symbol frame and cannot be "
                "backtested against a single symbol. Filter the strategy list "
                "with ?input_contract=single."
            ),
        )

    asset = await repo.find_asset(request.symbol)
    if asset is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown symbol: {request.symbol!r}"
        )

    start = request.start.replace(tzinfo=timezone.utc) if request.start.tzinfo is None else request.start
    end = request.end.replace(tzinfo=timezone.utc) if request.end.tzinfo is None else request.end

    records = await repo.fetch_range(
        symbol=request.symbol, asset_class=None, start=start, end=end
    )
    frame = records_to_frame(records)
    if frame.empty:
        raise HTTPException(
            status_code=422,
            detail=(
                f"No bars stored for {request.symbol!r} between "
                f"{start.date()} and {end.date()}."
            ),
        )

    try:
        results, metrics, trades = await run_in_threadpool(
            _run_backtest_sync,
            frame,
            spec,
            request.params,
            request.initial_capital,
            request.transaction_cost,
            request.seed,
        )
    except ValueError as exc:
        # Strategies validate their own parameters and raise ValueError —
        # that is a client error, not a server fault.
        raise HTTPException(status_code=422, detail=str(exc)) from None

    equity: List[EquityPoint] = []
    if request.include_equity_curve and results is not None and not results.empty:
        equity = [
            EquityPoint(
                time=idx.to_pydatetime(),
                total=float(row["total"]),
                cash=float(row["cash"]),
                holdings=float(row["holdings"]),
                position=float(row["position"]),
                signal=float(row["signal"]),
            )
            for idx, row in results.iterrows()
        ]

    trade_records: List[Dict[str, Any]] = []
    if request.include_trades and trades is not None and not trades.empty:
        trade_records = [
            {k: _json_safe(v) for k, v in record.items()}
            for record in trades.to_dict(orient="records")
        ]

    effective_params = {
        p.name: request.params.get(p.name, p.default) for p in spec.params
    }

    return BacktestResponse(
        symbol=asset.symbol,
        strategy_id=spec.id,
        strategy_name=spec.display_name,
        start=start,
        end=end,
        bars=len(frame),
        params=effective_params,
        seed=request.seed,
        metrics={k: _json_safe(v) for k, v in (metrics or {}).items()},
        caveat=spec.caveat,
        equity_curve=equity,
        trades=trade_records,
    )
