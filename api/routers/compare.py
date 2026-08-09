"""
api/routers/compare.py
Run several strategies on one symbol over one window and rank them.

Ported from the Streamlit comparison tab. Three things it did that are kept:

  * ONE symbol, several strategies — not several symbols. That is what makes
    the equity curves comparable on a single axis.
  * An optional BENCHMARK, which is buy-and-hold on a symbol rather than a
    registry strategy, normalised to the same starting capital so its "Total
    Return" is computed on the same basis as the strategies'.
  * Optional AUTO-TUNING before comparing, so the ranking is not merely
    measuring whose DEFAULTS happened to suit the window.

The tuning reuses ParameterOptimizer and the registry's `default_grid`. The
controller had its own recursive grid walk, its own `_OPTIMIZATION_GRIDS`, and
a restatement of the short<long constraint — a fifth copy of grid-search
knowledge. None of that is reproduced.

One thing NOT kept: the controller fell back to live yfinance when the
benchmark had no rows in the database. Prices come from the migrated database
everywhere else, so a missing benchmark is a 422 naming the symbol.

Phase 5 — decommissioning Streamlit
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from backtesting.parameter_optimizer import (
    MINIMIZE,
    OPTIMIZABLE_METRICS,
    ParameterOptimizer,
)
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo
from api.frames import records_to_frame
from api.routers.backtest import _json_safe, _run_backtest_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backtest/compare", tags=["backtest"])

MAX_STRATEGIES = 10
MAX_GRID_COMBINATIONS = 500


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class StrategyEntry(BaseModel):
    strategy_id: str
    params: Dict[str, Any] = Field(default_factory=dict)
    grid: Optional[Dict[str, List[Any]]] = Field(
        default=None,
        description=(
            "Sweep for this strategy when `optimize` is on. Omit to use the "
            "registry's default_grid; a strategy with neither runs on `params`."
        ),
    )


class CompareRequest(BaseModel):
    symbol: str
    start: datetime
    end: datetime
    strategies: List[StrategyEntry] = Field(min_length=1)
    benchmark_symbol: Optional[str] = Field(
        default=None,
        description="Buy-and-hold on this symbol, normalised to initial_capital.",
    )
    optimize: bool = Field(
        default=False,
        description=(
            "Tune each strategy over its grid before comparing, so the ranking "
            "does not just measure whose defaults suited the window."
        ),
    )
    metric: str = Field(default="Sharpe Ratio", description="Tuning and ranking metric")
    initial_capital: float = Field(default=100_000.0, gt=0)
    transaction_cost: float = Field(default=0.001, ge=0)
    seed: Optional[int] = Field(default=42)
    include_equity_curves: bool = Field(default=True)


class EquityPoint(BaseModel):
    time: datetime
    total: float


class ComparisonRow(BaseModel):
    strategy_id: str
    strategy_name: str
    params: Dict[str, Any] = Field(description="Parameters actually used")
    tuned: bool = Field(description="Whether these came from a grid search")
    combinations_evaluated: int = 0
    metrics: Dict[str, Any]
    caveat: Optional[str] = None
    equity_curve: List[EquityPoint] = Field(default_factory=list)


class BenchmarkRow(BaseModel):
    symbol: str
    metrics: Dict[str, Any]
    equity_curve: List[EquityPoint] = Field(default_factory=list)


class CompareResponse(BaseModel):
    symbol: str
    start: datetime
    end: datetime
    bars: int
    metric: str
    initial_capital: float
    seed: Optional[int]
    optimized: bool
    results: List[ComparisonRow] = Field(description="Ranked best-first by `metric`")
    benchmark: Optional[BenchmarkRow] = None
    skipped: List[Dict[str, str]] = Field(
        default_factory=list,
        description=(
            "Strategies that could not be run, with the reason. Reported rather "
            "than dropped, so a comparison missing an entry says why."
        ),
    )


# ---------------------------------------------------------------------------
# Work
# ---------------------------------------------------------------------------

def _curve(frame: Optional[pd.DataFrame]) -> List[EquityPoint]:
    if frame is None or frame.empty or "total" not in frame.columns:
        return []
    return [
        EquityPoint(time=index.to_pydatetime(), total=float(row["total"]))
        for index, row in frame.iterrows()
    ]


def _benchmark_metrics(
    frame: pd.DataFrame, initial_capital: float
) -> tuple[Dict[str, Any], pd.DataFrame]:
    """
    Buy-and-hold on the benchmark, scaled to the same starting capital.

    PerformanceAnalyzer expects `total` to be portfolio value, not raw price.
    Feeding it prices would make "Total Return" correct by luck and every
    capital-relative figure wrong.
    """
    from analysis.performance_analyzer import PerformanceAnalyzer

    series = frame["Close"].dropna()
    if series.empty or series.iloc[0] <= 0:
        raise ValueError("Benchmark has no usable closing prices.")

    values = (series / series.iloc[0]) * initial_capital
    portfolio = pd.DataFrame({"total": values})
    portfolio["returns"] = portfolio["total"].pct_change().fillna(0)

    analyzer = PerformanceAnalyzer(portfolio, initial_capital)
    return analyzer.calculate_all_metrics(), portfolio


def _compare_sync(
    frame: pd.DataFrame, request: CompareRequest
) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """CPU-bound half: tune (optionally), backtest, collect."""
    rows: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []

    for entry in request.strategies:
        try:
            spec = registry.get(entry.strategy_id)
        except KeyError as exc:
            skipped.append({"strategy_id": entry.strategy_id, "reason": str(exc)})
            continue

        if spec.input_contract != "single":
            skipped.append(
                {
                    "strategy_id": spec.id,
                    "reason": (
                        f"'{spec.id}' is multi-asset and cannot be compared on a "
                        "single symbol."
                    ),
                }
            )
            continue

        params = dict(entry.params)
        tuned = False
        evaluated = 0

        grid = entry.grid if entry.grid is not None else spec.default_grid
        if request.optimize and grid:
            combos = _expand(grid)
            if len(combos) > MAX_GRID_COMBINATIONS:
                skipped.append(
                    {
                        "strategy_id": spec.id,
                        "reason": (
                            f"Grid has {len(combos)} combinations; the per-strategy "
                            f"limit is {MAX_GRID_COMBINATIONS}."
                        ),
                    }
                )
                continue

            optimizer = ParameterOptimizer(
                price_data=frame,
                strategy_id=spec.id,
                param_grid=combos,
                metric=request.metric,
                initial_capital=request.initial_capital,
                transaction_cost=request.transaction_cost,
                seed=request.seed,
            )
            optimizer.run_optimization()
            best = optimizer.get_best_parameters()
            if best:
                params = best
                tuned = True
                evaluated = len(optimizer.results_df)

        try:
            results, metrics, _trades = _run_backtest_sync(
                frame,
                spec,
                params,
                request.initial_capital,
                request.transaction_cost,
                request.seed,
            )
        except (ValueError, KeyError) as exc:
            skipped.append({"strategy_id": spec.id, "reason": str(exc)})
            continue

        # EFFECTIVE params, not what the caller happened to send. An untuned
        # entry supplies {}, and echoing that back would leave the row unable
        # to say what produced it — the same reason /backtest and
        # /backtest/portfolio fill defaults before responding.
        effective = {
            p.name: params.get(p.name, p.default) for p in spec.params
        }

        rows.append(
            {
                "strategy_id": spec.id,
                "strategy_name": spec.display_name,
                "params": effective,
                "tuned": tuned,
                "combinations_evaluated": evaluated,
                "metrics": {k: _json_safe(v) for k, v in (metrics or {}).items()},
                "caveat": spec.caveat,
                "frame": results,
            }
        )

    return rows, skipped


def _expand(grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    import itertools

    names = list(grid)
    return [
        dict(zip(names, combo))
        for combo in itertools.product(*(grid[name] for name in names))
    ]


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=CompareResponse,
    summary="Compare several strategies on one symbol",
    responses={
        404: {"description": "Unknown symbol"},
        422: {"description": "Bad window, metric, or no strategy could run"},
    },
)
async def compare_strategies(
    request: CompareRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> CompareResponse:
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")
    if len(request.strategies) > MAX_STRATEGIES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{len(request.strategies)} strategies requested; the limit is "
                f"{MAX_STRATEGIES}."
            ),
        )
    if request.metric not in OPTIMIZABLE_METRICS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Cannot rank by {request.metric!r}. "
                f"Choose one of: {', '.join(OPTIMIZABLE_METRICS)}."
            ),
        )

    duplicates = [
        entry.strategy_id
        for entry in request.strategies
        if [e.strategy_id for e in request.strategies].count(entry.strategy_id) > 1
    ]
    if duplicates:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Duplicate strategies: {sorted(set(duplicates))}. Each may appear "
                "once — two rows with the same name cannot be told apart."
            ),
        )

    start = (
        request.start.replace(tzinfo=timezone.utc)
        if request.start.tzinfo is None else request.start
    )
    end = (
        request.end.replace(tzinfo=timezone.utc)
        if request.end.tzinfo is None else request.end
    )

    async def load(symbol: str) -> pd.DataFrame:
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
        return frame

    frame = await load(request.symbol)

    benchmark: Optional[BenchmarkRow] = None
    if request.benchmark_symbol:
        # No live-yfinance fallback: prices come from the migrated database
        # everywhere else, and a benchmark quietly sourced elsewhere would be
        # measured against a different price series than the strategies.
        benchmark_frame = await load(request.benchmark_symbol)
        try:
            metrics, portfolio = await run_in_threadpool(
                _benchmark_metrics, benchmark_frame, request.initial_capital
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        benchmark = BenchmarkRow(
            symbol=request.benchmark_symbol.upper(),
            metrics={k: _json_safe(v) for k, v in metrics.items()},
            equity_curve=_curve(portfolio) if request.include_equity_curves else [],
        )

    rows, skipped = await run_in_threadpool(_compare_sync, frame, request)

    if not rows:
        reasons = "; ".join(f"{s['strategy_id']}: {s['reason']}" for s in skipped)
        raise HTTPException(
            status_code=422,
            detail=f"No strategy could be compared. {reasons}",
        )

    # Direction matters. Ranking descending for every metric would report the
    # MOST volatile strategy as best when ranking by Annualized Volatility —
    # the same defect fixed in ParameterOptimizer and the Streamlit
    # optimization tab. MINIMIZE is imported from the optimizer so the three
    # cannot drift apart.
    minimize = request.metric in MINIMIZE
    unrankable = float("inf") if minimize else float("-inf")

    def rank_key(row: Dict[str, Any]) -> float:
        value = row["metrics"].get(request.metric)
        # A strategy that never traded yields NaN, which sorts unpredictably;
        # it goes last either way rather than winning by accident.
        if not isinstance(value, (int, float)) or value != value:
            return unrankable
        return value

    ranked = sorted(rows, key=rank_key, reverse=not minimize)

    return CompareResponse(
        symbol=request.symbol,
        start=start,
        end=end,
        bars=len(frame),
        metric=request.metric,
        initial_capital=request.initial_capital,
        seed=request.seed,
        optimized=request.optimize,
        results=[
            ComparisonRow(
                strategy_id=row["strategy_id"],
                strategy_name=row["strategy_name"],
                params=row["params"],
                tuned=row["tuned"],
                combinations_evaluated=row["combinations_evaluated"],
                metrics=row["metrics"],
                caveat=row["caveat"],
                equity_curve=(
                    _curve(row["frame"]) if request.include_equity_curves else []
                ),
            )
            for row in ranked
        ],
        benchmark=benchmark,
        skipped=skipped,
    )
