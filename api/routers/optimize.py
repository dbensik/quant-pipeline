"""
api/routers/optimize.py
Parameter grid search and Monte Carlo portfolio weighting.

TWO ENDPOINTS, NOT ONE. They answer different questions and share no inputs:
`/strategy` searches one strategy's parameter space on a single symbol and
returns a ranking; `/portfolio` samples weight vectors across several symbols
and returns an allocation. The only thing they have in common is that both run
for long enough to want progress — which is why both are also reachable over
the websocket.

Phase 5 — decommissioning Streamlit
"""

import itertools
import logging
import math
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from backtesting.parameter_optimizer import OPTIMIZABLE_METRICS, ParameterOptimizer
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo
from api.frames import frames_to_wide_close, records_to_frame
from api.routers.backtest import _json_safe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/optimize", tags=["optimize"])

# Cap on grid size. Set from the case being ported rather than a round number:
# the Streamlit MA-crossover defaults were a 21x21 grid (441 combinations)
# before invalid ones were dropped, so a tighter cap would regress it.
MAX_COMBINATIONS = 1000
MAX_PORTFOLIOS = 50_000
MAX_SYMBOLS = 50


# ---------------------------------------------------------------------------
# Grid specification
# ---------------------------------------------------------------------------

class RangeSpec(BaseModel):
    """An inclusive numeric sweep. `{"min": 10, "max": 30, "step": 5}`."""

    min: float
    max: float
    step: float = Field(default=1.0, gt=0)


# A list covers categorical and non-uniform axes (a `str` parameter like
# rebalance_frequency, or explicit values); RangeSpec keeps a fine numeric
# sweep from having to be enumerated in the request body.
GridAxis = Union[List[Any], RangeSpec]


def _expand_axis(axis: GridAxis, param: registry.ParamSpec) -> List[Any]:
    """One axis to a list of values, cast to the parameter's declared type."""
    if isinstance(axis, RangeSpec):
        if axis.max < axis.min:
            raise ValueError(
                f"Range for {param.name!r} has max ({axis.max}) below min ({axis.min})."
            )
        count = int(math.floor((axis.max - axis.min) / axis.step + 1e-9)) + 1
        values: List[Any] = [axis.min + i * axis.step for i in range(count)]
    else:
        values = list(axis)

    if not values:
        raise ValueError(f"No values given for {param.name!r}.")

    if param.type == "int":
        return [int(round(float(v))) for v in values]
    if param.type == "float":
        return [float(v) for v in values]
    return values


def expand_grid(
    grid: Dict[str, GridAxis], spec: registry.StrategySpec
) -> List[Dict[str, Any]]:
    """
    Cartesian product of the requested axes.

    Grid keys are validated against the registry HERE rather than being left
    to fail per-combination inside the optimizer. The optimizer treats a
    ValueError from spec.build() as "invalid combination, skip it" — so a
    misspelled parameter name would otherwise skip every combination and
    return an empty ranking with a 200, while the same typo sent to
    /api/v1/backtest is a 422.
    """
    known = {p.name: p for p in spec.params}
    unknown = sorted(set(grid) - set(known))
    if unknown:
        raise ValueError(
            f"Unknown parameter(s) for '{spec.id}': {unknown}. "
            f"Known: {sorted(known)}."
        )

    names = list(grid)
    axes = [_expand_axis(grid[name], known[name]) for name in names]

    size = math.prod(len(a) for a in axes)
    if size > MAX_COMBINATIONS:
        raise ValueError(
            f"Grid has {size} combinations; the limit is {MAX_COMBINATIONS}. "
            "Widen the step or narrow the range."
        )

    return [dict(zip(names, combo)) for combo in itertools.product(*axes)]


# ---------------------------------------------------------------------------
# Strategy optimization
# ---------------------------------------------------------------------------

class StrategyOptimizeRequest(BaseModel):
    symbol: str
    strategy_id: str = Field(description="Registry id — must be single-asset")
    start: datetime
    end: datetime
    grid: Dict[str, GridAxis] = Field(
        description=(
            "Parameter axes to sweep. Each value is either a list of values or "
            'an inclusive range: {"short_window": {"min": 10, "max": 30, '
            '"step": 5}, "long_window": [40, 50, 60]}'
        )
    )
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Fixed values for parameters not being swept.",
    )
    metric: str = Field(
        default="Sharpe Ratio",
        description=f"Metric to rank by. One of: {', '.join(OPTIMIZABLE_METRICS)}",
    )
    initial_capital: float = Field(default=100_000.0, gt=0)
    transaction_cost: float = Field(default=0.001, ge=0)
    seed: Optional[int] = Field(
        default=42,
        description=(
            "Slippage seed applied identically to every combination, so the "
            "ranking reflects the parameters rather than the draw. Pass null "
            "for unseeded, non-reproducible behaviour."
        ),
    )
    top_n: int = Field(default=25, gt=0, le=MAX_COMBINATIONS)


class SkippedCombination(BaseModel):
    params: Dict[str, Any]
    reason: str


class StrategyOptimizeResponse(BaseModel):
    symbol: str
    strategy_id: str
    strategy_name: str
    start: datetime
    end: datetime
    bars: int
    metric: str
    seed: Optional[int]
    initial_capital: float
    combinations_requested: int
    combinations_evaluated: int
    best_params: Dict[str, Any]
    best_metrics: Dict[str, Any]
    results: List[Dict[str, Any]] = Field(
        description="Ranked best-first, truncated to top_n"
    )
    skipped: List[SkippedCombination] = Field(
        default_factory=list,
        description=(
            "Combinations the strategy itself rejected, e.g. short_window >= "
            "long_window. Reported rather than dropped."
        ),
    )
    caveat: Optional[str] = None


def _optimize_sync(
    frame: pd.DataFrame,
    strategy_id: str,
    combos: List[Dict[str, Any]],
    request: StrategyOptimizeRequest,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> ParameterOptimizer:
    optimizer = ParameterOptimizer(
        price_data=frame,
        strategy_id=strategy_id,
        param_grid=combos,
        metric=request.metric,
        initial_capital=request.initial_capital,
        transaction_cost=request.transaction_cost,
        seed=request.seed,
    )
    optimizer.run_optimization(progress_callback=progress_callback)
    return optimizer


def build_optimize_payload(
    optimizer: ParameterOptimizer,
    spec: registry.StrategySpec,
    frame: pd.DataFrame,
    combos: List[Dict[str, Any]],
    request: StrategyOptimizeRequest,
    start: datetime,
    end: datetime,
) -> Dict[str, Any]:
    """Shared by the REST route and the websocket, so both report identically."""
    results = [
        {k: _json_safe(v) for k, v in record.items()}
        for record in optimizer.get_ranked_records(top_n=request.top_n)
    ]

    return {
        "symbol": request.symbol,
        "strategy_id": spec.id,
        "strategy_name": spec.display_name,
        "start": start,
        "end": end,
        "bars": len(frame),
        "metric": request.metric,
        "seed": request.seed,
        "initial_capital": request.initial_capital,
        "combinations_requested": len(combos),
        "combinations_evaluated": (
            0 if optimizer.results_df is None else len(optimizer.results_df)
        ),
        "best_params": {
            k: _json_safe(v) for k, v in optimizer.get_best_parameters().items()
        },
        "best_metrics": {
            k: _json_safe(v) for k, v in optimizer.get_best_metrics().items()
        },
        "results": results,
        "skipped": optimizer.skipped,
        "caveat": spec.caveat,
    }


def validate_strategy_request(
    request: StrategyOptimizeRequest,
) -> tuple[registry.StrategySpec, List[Dict[str, Any]]]:
    """
    Everything checkable before touching the database. Raises HTTPException.
    Shared with the websocket so both transports reject the same inputs.
    """
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    if request.metric not in OPTIMIZABLE_METRICS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Cannot optimize for {request.metric!r}. "
                f"Choose one of: {', '.join(OPTIMIZABLE_METRICS)}. "
                "('Trade Count' is reported but has no better direction.)"
            ),
        )

    try:
        spec = registry.get(request.strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    if spec.input_contract == "multi":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Strategy '{spec.id}' is multi-asset and cannot be optimized "
                "against a single symbol."
            ),
        )

    if not request.grid:
        raise HTTPException(
            status_code=422,
            detail="`grid` is empty — there is nothing to search.",
        )

    try:
        combos = expand_grid(request.grid, spec)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    # Fixed params are merged in beneath the swept ones, which win.
    if request.params:
        unknown = sorted(set(request.params) - {p.name for p in spec.params})
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown parameter(s) for '{spec.id}': {unknown}.",
            )
        combos = [{**request.params, **combo} for combo in combos]

    return spec, combos


@router.post(
    "/strategy",
    response_model=StrategyOptimizeResponse,
    summary="Grid-search a strategy's parameters on one symbol",
    responses={
        404: {"description": "Unknown symbol or strategy"},
        422: {"description": "Bad grid, metric, date range, or no valid combination"},
    },
)
async def optimize_strategy(
    request: StrategyOptimizeRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> StrategyOptimizeResponse:
    spec, combos = validate_strategy_request(request)

    start = (
        request.start.replace(tzinfo=timezone.utc)
        if request.start.tzinfo is None else request.start
    )
    end = (
        request.end.replace(tzinfo=timezone.utc)
        if request.end.tzinfo is None else request.end
    )

    asset = await repo.find_asset(request.symbol)
    if asset is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown symbol: {request.symbol!r}"
        )
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

    optimizer = await run_in_threadpool(
        _optimize_sync, frame, spec.id, combos, request, None
    )

    # Every combination rejected is a client error, not an empty result set.
    # Returning 200 with best_params={} would read as "searched and found
    # nothing" when in fact nothing was searched.
    if optimizer.results_df is None or optimizer.results_df.empty:
        reasons = {entry["reason"] for entry in optimizer.skipped}
        raise HTTPException(
            status_code=422,
            detail=(
                f"All {len(combos)} combinations were rejected by "
                f"'{spec.id}': {'; '.join(sorted(reasons)) or 'no results'}"
            ),
        )

    return StrategyOptimizeResponse(
        **build_optimize_payload(optimizer, spec, frame, combos, request, start, end)
    )


# ---------------------------------------------------------------------------
# Portfolio weight optimization
# ---------------------------------------------------------------------------

class PortfolioOptimizeRequest(BaseModel):
    symbols: List[str] = Field(description="Two or more assets to allocate across")
    start: datetime
    end: datetime
    num_portfolios: int = Field(default=5_000, gt=0, le=MAX_PORTFOLIOS)
    risk_free_rate: float = Field(default=0.02)
    seed: Optional[int] = Field(
        default=42,
        description=(
            "Seed for weight sampling. Monte Carlo weights were drawn from the "
            "global numpy RNG, so the same request returned different optimal "
            "allocations each time."
        ),
    )
    include_frontier: bool = Field(
        default=False,
        description=(
            "Return every sampled portfolio for a frontier scatter. Off by "
            "default: 5,000 rows each carrying a weights mapping is a large "
            "response, and the allocation alone is what most callers want."
        ),
    )
    frontier_points: int = Field(
        default=1_000,
        gt=0,
        description="Cap on returned frontier points; sampled evenly.",
    )


class AllocationPoint(BaseModel):
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: float
    weights: Dict[str, float]


class PortfolioOptimizeResponse(BaseModel):
    symbols: List[str]
    start: datetime
    end: datetime
    bars: int
    num_portfolios: int
    risk_free_rate: float
    seed: Optional[int]
    max_sharpe: AllocationPoint
    min_volatility: AllocationPoint
    frontier: List[AllocationPoint] = Field(default_factory=list)


def _row_to_allocation(row: pd.Series) -> AllocationPoint:
    return AllocationPoint(
        annualized_return=float(row["Annualized Return"]),
        annualized_volatility=float(row["Annualized Volatility"]),
        sharpe_ratio=float(row["Sharpe Ratio"]),
        weights={k: float(v) for k, v in row["weights"].items()},
    )


def _simulate_sync(
    prices: pd.DataFrame,
    request: PortfolioOptimizeRequest,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> pd.DataFrame:
    # portfolio.portfolio_optimizer — the analytic mean/covariance one. There
    # is a second, unused class of the same name in optimization/ that runs
    # full backtests per trial; it is not what Streamlit used and not this.
    from portfolio.portfolio_optimizer import PortfolioOptimizer

    optimizer = PortfolioOptimizer(
        price_data=prices,
        risk_free_rate=request.risk_free_rate,
        seed=request.seed,
    )
    results_df, _max_sharpe_weights, _min_vol_weights = (
        optimizer.simulate_random_portfolios(
            num_portfolios=request.num_portfolios, callback=progress_callback
        )
    )
    return results_df


def build_portfolio_payload(
    results_df: pd.DataFrame,
    prices: pd.DataFrame,
    request: PortfolioOptimizeRequest,
    start: datetime,
    end: datetime,
) -> Dict[str, Any]:
    max_sharpe = results_df.loc[results_df["Sharpe Ratio"].idxmax()]
    min_vol = results_df.loc[results_df["Annualized Volatility"].idxmin()]

    frontier: List[AllocationPoint] = []
    if request.include_frontier:
        frame = results_df
        if len(frame) > request.frontier_points:
            # Even stride rather than head(): the frontier's shape comes from
            # the whole cloud, and the first N draws are not a sample of it.
            stride = math.ceil(len(frame) / request.frontier_points)
            frame = frame.iloc[::stride]
        frontier = [_row_to_allocation(row) for _, row in frame.iterrows()]

    return {
        "symbols": list(request.symbols),
        "start": start,
        "end": end,
        "bars": len(prices),
        "num_portfolios": request.num_portfolios,
        "risk_free_rate": request.risk_free_rate,
        "seed": request.seed,
        "max_sharpe": _row_to_allocation(max_sharpe),
        "min_volatility": _row_to_allocation(min_vol),
        "frontier": frontier,
    }


@router.post(
    "/portfolio",
    response_model=PortfolioOptimizeResponse,
    summary="Monte Carlo search for optimal portfolio weights",
    responses={
        404: {"description": "Unknown symbol"},
        422: {"description": "Fewer than two symbols, or insufficient overlap"},
    },
)
async def optimize_portfolio(
    request: PortfolioOptimizeRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> PortfolioOptimizeResponse:
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    if len(request.symbols) < 2:
        raise HTTPException(
            status_code=422,
            detail=(
                "Weight optimization needs at least 2 symbols; "
                f"{len(request.symbols)} given."
            ),
        )
    if len(request.symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"{len(request.symbols)} symbols requested; the limit is {MAX_SYMBOLS}.",
        )

    start = (
        request.start.replace(tzinfo=timezone.utc)
        if request.start.tzinfo is None else request.start
    )
    end = (
        request.end.replace(tzinfo=timezone.utc)
        if request.end.tzinfo is None else request.end
    )

    price_data: Dict[str, pd.DataFrame] = {}
    for symbol in request.symbols:
        asset = await repo.find_asset(symbol)
        if asset is None:
            raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol!r}")
        records = await repo.fetch_range(
            symbol=symbol, asset_class=None, start=start, end=end
        )
        frame = records_to_frame(records)
        if frame.empty:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"No bars stored for {symbol!r} between "
                    f"{start.date()} and {end.date()}."
                ),
            )
        price_data[symbol] = frame

    prices = frames_to_wide_close(price_data).dropna()
    # Covariance of a single row is degenerate; two rows give one return.
    if len(prices) < 3:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Only {len(prices)} dates are common to all symbols; at least "
                "3 are needed to estimate a covariance matrix."
            ),
        )

    results_df = await run_in_threadpool(_simulate_sync, prices, request, None)

    return PortfolioOptimizeResponse(
        **build_portfolio_payload(results_df, prices, request, start, end)
    )
