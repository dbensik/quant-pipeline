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

TUNING IS WALK-FORWARD (decision 41, 2026-10-19). It used to pick each
strategy's parameters on the window and then rank them on that same window:
a hindsight figure, which on 428 names had tuned strategies "beat" buy and
hold on 37-98% of names against 7-30% out of sample. A tuned row now reports
the walk-forward account (each calendar year traded with the parameters
picked on the years before it), and so that the rows stay comparable EVERY
row and the benchmark are measured over the same span: from the first
validate year to the end, each rebased to initial_capital there. A window
too short for one fold is refused with tuning on, never tuned in hindsight
instead.

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

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from api.dependencies import get_market_data_repo
from api.frames import records_to_frame
from api.lots import lot_size_or_422
from api.risk_free import (
    RISK_FREE_RATE_DESCRIPTION,
    RISK_FREE_SOURCE_DESCRIPTION,
    RiskFreeCoverageError,
    get_rate_repo,
    resolve_risk_free,
    risk_free_refusal,
)
from api.routers.backtest import _json_safe, _run_backtest_sync
from backtesting.parameter_optimizer import (
    MINIMIZE,
    OPTIMIZABLE_METRICS,
    WALK_FORWARD_MIN_TRAIN_YEARS,
    ParameterOptimizer,
    walk_forward_folds,
    window_metrics,
)
from core.rates import RateRepository
from db.repositories.market_data import TimescaleMarketDataRepo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backtest/compare", tags=["backtest"])

MAX_STRATEGIES = 10
MAX_GRID_COMBINATIONS = 500


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ComparisonEquityPoint(BaseModel):
    """
    A point on a comparison curve: time and total only.

    NOT named `EquityPoint`. api/routers/backtest.py already has a class by
    that name carrying six fields, and two Pydantic models sharing a class
    name collide in the OpenAPI document — openapi-typescript disambiguates
    them to `api__routers__backtest__EquityPoint` and
    `api__routers__compare__EquityPoint`, which silently renamed the type the
    frontend imported and broke `npm run build`. Same convention as
    PortfolioEquityPoint in portfolio_backtest.py.
    """

    time: datetime
    total: float


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
    risk_free_rate: Optional[float] = Field(default=None, description=RISK_FREE_RATE_DESCRIPTION)
    min_train_years: int = Field(
        default=WALK_FORWARD_MIN_TRAIN_YEARS,
        ge=1,
        le=20,
        description=(
            "With `optimize`: whole calendar years of history before the first "
            "walk-forward validate year."
        ),
    )


class CompareWalkForwardPick(BaseModel):
    """One calendar year of a tuned row: the parameters it traded with."""

    validate_start: datetime
    validate_end: datetime
    params: Optional[Dict[str, Any]] = Field(
        description="Picked on the years before; null when nothing traded in them"
    )


class ComparisonRow(BaseModel):
    strategy_id: str
    strategy_name: str
    params: Dict[str, Any] = Field(
        description="Parameters actually used; for a tuned row, the latest year's pick"
    )
    tuned: bool = Field(description="Whether these came from a walk-forward grid search")
    combinations_evaluated: int = 0
    picks: List[CompareWalkForwardPick] = Field(
        default_factory=list, description="A tuned row's pick for each validate year"
    )
    metrics: Dict[str, Any]
    caveat: Optional[str] = None
    equity_curve: List[ComparisonEquityPoint] = Field(default_factory=list)


class BenchmarkRow(BaseModel):
    symbol: str
    metrics: Dict[str, Any]
    equity_curve: List[ComparisonEquityPoint] = Field(default_factory=list)


class CompareResponse(BaseModel):
    symbol: str
    start: datetime
    end: datetime
    bars: int
    metric: str
    initial_capital: float
    seed: Optional[int]
    risk_free_rate: float = Field(
        description="Annual rate every Sharpe here is in excess of, the benchmark's included"
    )
    risk_free_source: str = Field(description=RISK_FREE_SOURCE_DESCRIPTION)
    optimized: bool
    evaluated_from: Optional[datetime] = Field(
        default=None,
        description=(
            "With `optimize`: every row and the benchmark are measured from here "
            "(the first walk-forward validate year) to `end`, rebased to "
            "initial_capital. Null when measured over the whole window."
        ),
    )
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


def _curve(frame: Optional[pd.DataFrame]) -> List[ComparisonEquityPoint]:
    if frame is None or frame.empty or "total" not in frame.columns:
        return []
    return [
        ComparisonEquityPoint(time=index.to_pydatetime(), total=float(row["total"]))
        for index, row in frame.iterrows()
    ]


def _benchmark_metrics(
    frame: pd.DataFrame,
    initial_capital: float,
    cash_accrual: np.ndarray,
    evaluated_from: Optional[pd.Timestamp] = None,
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

    if evaluated_from is not None:
        # The span the tuned rows are measured over: from the benchmark's last
        # bar before the first validate bar (its own calendar may differ).
        accrual = pd.Series(np.asarray(cash_accrual, dtype=float), index=frame.index)
        accrual = accrual.loc[series.index].to_numpy()
        positions = np.flatnonzero(series.index < evaluated_from)
        base = int(positions[-1]) if len(positions) else 0
        last = len(series) - 1
        metrics = window_metrics(
            values.to_numpy(), None, series.index, accrual, base, last, initial_capital
        )
        return metrics, _rebased(portfolio, base, initial_capital)

    analyzer = PerformanceAnalyzer(
        portfolio, initial_capital=initial_capital, risk_free_accrual=cash_accrual
    )
    return analyzer.calculate_all_metrics(), portfolio


def _rebased(portfolio: pd.DataFrame, base: int, capital: float) -> pd.DataFrame:
    """The curve from bar `base` on, worth `capital` there."""
    total = portfolio["total"].iloc[base:]
    return pd.DataFrame({"total": total * (capital / float(total.iloc[0]))})


def _compare_sync(
    frame: pd.DataFrame,
    request: CompareRequest,
    cash_accrual: np.ndarray,
    lot_size: float,
    span_base: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    CPU-bound half: tune (optionally), backtest, collect.

    With `optimize`, `span_base` is the boundary bar before the first
    walk-forward validate year, and every row is measured from it.
    """
    rows: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []
    last = len(frame) - 1

    def on_span(results: pd.DataFrame) -> tuple[Dict[str, Any], pd.DataFrame]:
        metrics = window_metrics(
            results["total"].to_numpy(),
            results["position"].to_numpy() if "position" in results.columns else None,
            frame.index,
            cash_accrual,
            span_base,
            last,
            request.initial_capital,
        )
        return metrics, _rebased(results, span_base, request.initial_capital)

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
                        f"'{spec.id}' is multi-asset and cannot be compared on a single symbol."
                    ),
                }
            )
            continue

        params = dict(entry.params)
        tuned = False
        evaluated = 0
        picks: List[Dict[str, Any]] = []
        walked: Optional[tuple[Dict[str, Any], pd.DataFrame]] = None

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
                cash_accrual=cash_accrual,
                lot_size=lot_size,
            )
            optimizer.run_optimization()
            report = (
                optimizer.run_walk_forward(request.min_train_years)
                if optimizer.get_best_parameters()
                else None
            )
            if report is not None:
                picks = [
                    {
                        "validate_start": fold["validate_start"],
                        "validate_end": fold["validate_end"],
                        "params": fold["params"],
                    }
                    for fold in report["folds"]
                ]
                latest = next((p["params"] for p in reversed(picks) if p["params"]), None)
                params = dict(latest) if latest else params
                tuned = True
                evaluated = len(optimizer.results_df)
                walked = (
                    report["oos_metrics"],
                    _rebased(optimizer.walk_forward_portfolio, span_base, request.initial_capital),
                )

        if walked is not None:
            metrics, results = walked
        else:
            try:
                results, metrics, _trades = _run_backtest_sync(
                    frame,
                    spec,
                    params,
                    request.initial_capital,
                    request.transaction_cost,
                    request.seed,
                    cash_accrual=cash_accrual,
                    lot_size=lot_size,
                )
            except (ValueError, KeyError) as exc:
                skipped.append({"strategy_id": spec.id, "reason": str(exc)})
                continue
            if span_base is not None:
                metrics, results = on_span(results)

        # EFFECTIVE params, not what the caller happened to send. An untuned
        # entry supplies {}, and echoing that back would leave the row unable
        # to say what produced it — the same reason /backtest and
        # /backtest/portfolio fill defaults before responding.
        effective = {p.name: params.get(p.name, p.default) for p in spec.params}

        rows.append(
            {
                "strategy_id": spec.id,
                "strategy_name": spec.display_name,
                "params": effective,
                "tuned": tuned,
                "combinations_evaluated": evaluated,
                "picks": picks,
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
        dict(zip(names, combo, strict=False))
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
    rates: RateRepository = Depends(get_rate_repo),
) -> CompareResponse:
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")
    if len(request.strategies) > MAX_STRATEGIES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{len(request.strategies)} strategies requested; the limit is {MAX_STRATEGIES}."
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
        if request.start.tzinfo is None
        else request.start
    )
    end = request.end.replace(tzinfo=timezone.utc) if request.end.tzinfo is None else request.end

    async def load(symbol: str) -> tuple[Any, pd.DataFrame]:
        asset = await repo.find_asset(symbol)
        if asset is None:
            raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol!r}")
        records = await repo.fetch_range(symbol=symbol, asset_class=None, start=start, end=end)
        frame = records_to_frame(records)
        if frame.empty:
            raise HTTPException(
                status_code=422,
                detail=(f"No bars stored for {symbol!r} between {start.date()} and {end.date()}."),
            )
        return asset, frame

    asset, frame = await load(request.symbol)
    lot_size = lot_size_or_422(asset)

    # Tuning is walk-forward, so it needs a validate year; refused otherwise
    # rather than tuned in hindsight, which is the figure decision 41 removed.
    span_base: Optional[int] = None
    evaluated_from: Optional[pd.Timestamp] = None
    if request.optimize:
        folds = walk_forward_folds(frame.index, request.min_train_years)
        if not folds:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Tuning is walk-forward: it needs {request.min_train_years} whole "
                    f"calendar year(s) of history before a validate year, and this window "
                    f"runs {frame.index[0].date()} to {frame.index[-1].date()}. Widen the "
                    "window, lower min_train_years, or compare without tuning."
                ),
            )
        span_base = folds[0][0]
        evaluated_from = frame.index[span_base + 1]

    # One rate for the whole comparison, the benchmark's Sharpe included: the
    # rows are ranked against each other, so they must share a hurdle. It is
    # taken over the strategy symbol's sessions, the window the rows trade.
    try:
        risk_free = await resolve_risk_free(request.risk_free_rate, frame.index, rates)
    except RiskFreeCoverageError as exc:
        raise HTTPException(status_code=422, detail=risk_free_refusal(exc)) from exc

    benchmark: Optional[BenchmarkRow] = None
    if request.benchmark_symbol:
        # No live-yfinance fallback: prices come from the migrated database
        # everywhere else, and a benchmark quietly sourced elsewhere would be
        # measured against a different price series than the strategies.
        _benchmark_asset, benchmark_frame = await load(request.benchmark_symbol)
        # Same rate source, accrued on the benchmark's own bars: a crypto
        # benchmark trades at weekends the strategy symbol does not.
        try:
            benchmark_rf = await resolve_risk_free(
                request.risk_free_rate, benchmark_frame.index, rates
            )
        except RiskFreeCoverageError as exc:
            raise HTTPException(status_code=422, detail=risk_free_refusal(exc)) from exc
        try:
            metrics, portfolio = await run_in_threadpool(
                _benchmark_metrics,
                benchmark_frame,
                request.initial_capital,
                benchmark_rf.accrual,
                evaluated_from,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        benchmark = BenchmarkRow(
            symbol=request.benchmark_symbol.upper(),
            metrics={k: _json_safe(v) for k, v in metrics.items()},
            equity_curve=_curve(portfolio) if request.include_equity_curves else [],
        )

    rows, skipped = await run_in_threadpool(
        _compare_sync, frame, request, risk_free.accrual, lot_size, span_base
    )

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
        risk_free_rate=risk_free[0],
        risk_free_source=risk_free[1],
        optimized=request.optimize,
        evaluated_from=evaluated_from,
        results=[
            ComparisonRow(
                strategy_id=row["strategy_id"],
                strategy_name=row["strategy_name"],
                params=row["params"],
                tuned=row["tuned"],
                combinations_evaluated=row["combinations_evaluated"],
                picks=row["picks"],
                metrics=row["metrics"],
                caveat=row["caveat"],
                equity_curve=(_curve(row["frame"]) if request.include_equity_curves else []),
            )
            for row in ranked
        ],
        benchmark=benchmark,
        skipped=skipped,
    )
