"""
api/routers/simulate.py
Monte Carlo over one strategy on one symbol: a distribution around the single
equity curve `/backtest` returns.

Two modes, chosen per request:

- `returns` (default) resamples the backtest's own realised daily returns —
  net of costs and seeded slippage — from the first invested bar. Instant,
  but it assumes the strategy's return distribution is stationary and ignores
  that signals depend on the path, so the response carries a caveat whenever
  the strategy actually traded.
- `prices` resamples the PRICE returns, rebuilds a synthetic OHLCV series per
  path and re-runs the strategy on each through the real Backtester. Honest
  for path-dependent signals; one backtest per path.

Three resampling methods (`simulation/resample.py`): the stationary block
bootstrap keeps volatility clustering and is the default; iid and GBM are
there for contrast and understate the tails.

Only assets with `price_basis = 'served'` are simulated by default — a
bootstrap over a series with an impossible move resamples that move into
every path. `allow_unverified` overrides and sets the caveat.

Plan: research/monte-carlo-plan-2026-10-07.md (approved 2026-10-07).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from api.dependencies import get_market_data_repo
from api.frames import records_to_frame
from api.routers.backtest import _json_safe, _run_backtest_sync
from backtesting.backtester import Backtester
from config import settings
from db.repositories.market_data import TimescaleMarketDataRepo
from simulation import (
    bands,
    cvar,
    draw,
    equity_from_returns,
    horizon_returns,
    max_drawdown,
    max_drawdown_duration,
    prob_ruin,
    prob_worse_drawdown,
    terminal_wealth,
    var,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/simulate", tags=["simulate"])

PERCENTILES = (5, 25, 50, 75, 95)
PROGRESS_EVERY = 25

Mode = Literal["returns", "prices"]
Method = Literal["stationary", "iid", "gbm"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SimulationRequest(BaseModel):
    symbol: str = Field(description="Ticker as stored, e.g. 'AAPL'")
    strategy_id: str = Field(description="Registry id, e.g. 'ma_crossover'")
    start: datetime
    end: datetime
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Strategy parameters. Omitted parameters use registry defaults.",
    )
    initial_capital: float = Field(default=100_000.0, gt=0)
    transaction_cost: float = Field(default=0.001, ge=0)
    seed: Optional[int] = Field(
        default=42,
        description=(
            "Seeds the historical run's slippage AND every resampling draw. "
            "The same request returns the same bands; null is unseeded."
        ),
    )
    mode: Mode = Field(
        default="returns",
        description=(
            "'returns' resamples the strategy's realised daily returns; "
            "'prices' resamples price returns and re-runs the strategy per path."
        ),
    )
    method: Method = Field(
        default="stationary",
        description="Stationary block bootstrap (default), iid bootstrap, or GBM.",
    )
    n_paths: Optional[int] = Field(
        default=None,
        gt=0,
        description=(
            "Defaults per mode (settings.SIM_DEFAULT_PATHS / "
            "SIM_DEFAULT_RERUN_PATHS); capped per mode."
        ),
    )
    horizon_days: Optional[int] = Field(
        default=None,
        gt=0,
        description=(
            "Bars to simulate. Default: the length of the resampled history, "
            "so the fan is comparable to the historical curve."
        ),
    )
    block_length_days: float = Field(
        default=settings.SIM_BLOCK_LENGTH_DAYS, ge=1.0,
        description="Stationary bootstrap mean block; ignored by iid and gbm.",
    )
    ruin_threshold: float = Field(
        default=settings.SIM_RUIN_THRESHOLD, gt=0, lt=1,
        description="P(ruin) counts paths that ever fall below this fraction of start equity.",
    )
    include_paths: int = Field(
        default=0, ge=0, le=settings.SIM_MAX_RETURNED_PATHS,
        description="How many individual sample paths to return beside the bands.",
    )
    allow_unverified: bool = Field(
        default=False,
        description=(
            "Simulate an asset whose price_basis is not 'served' (legacy names, "
            "crypto). The response caveat says so."
        ),
    )


class FanBand(BaseModel):
    step: int
    p05: float
    p25: float
    p50: float
    p75: float
    p95: float


class PercentileSummary(BaseModel):
    p05: float
    p25: float
    p50: float
    p75: float
    p95: float
    mean: float


class TerminalSummary(BaseModel):
    wealth: PercentileSummary
    total_return: PercentileSummary
    prob_loss: float = Field(description="Share of paths that end below start equity")


class DrawdownSummary(BaseModel):
    depth: PercentileSummary = Field(description="Max drawdown per path; <= 0")
    duration_bars: PercentileSummary
    historical_depth: Optional[float]
    historical_duration_bars: Optional[int]
    prob_worse_than_historical: Optional[float]


class RiskRow(BaseModel):
    horizon_days: int
    var_95: float = Field(description="Loss as a positive fraction")
    cvar_95: float
    var_99: float
    cvar_99: float


class RiskSummary(BaseModel):
    rows: List[RiskRow]
    prob_ruin: float
    ruin_threshold: float


class SimulationResponse(BaseModel):
    symbol: str
    strategy_id: str
    strategy_name: str
    start: datetime
    end: datetime
    bars: int = Field(description="Bars fed to the historical run")
    params: Dict[str, Any]
    initial_capital: float
    seed: Optional[int]
    mode: Mode
    method: Method
    n_paths: int
    horizon_days: int
    block_length_days: float
    resampled_from: int = Field(description="Daily returns the draws were taken from")
    historical: Dict[str, Any] = Field(description="The real run's KPIs, as /backtest returns them")
    bands: List[FanBand]
    terminal: TerminalSummary
    drawdown: DrawdownSummary
    risk: RiskSummary
    sample_paths: List[List[float]] = Field(default_factory=list)
    caveat: Optional[str] = None


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def _summary(values: np.ndarray) -> PercentileSummary:
    p = np.percentile(values, PERCENTILES)
    return PercentileSummary(
        p05=float(p[0]), p25=float(p[1]), p50=float(p[2]), p75=float(p[3]),
        p95=float(p[4]), mean=float(values.mean()),
    )


def _synthetic_frame(
    template: pd.DataFrame, path_returns: np.ndarray, horizon: int
) -> pd.DataFrame:
    """
    An OHLCV frame the strategy can run on, built from one path of close
    returns. Open is the previous close (no gaps); High and Low sit above and
    below the open/close range by the template's median intrabar extension,
    so range-based strategies (ATR breakout) see a plausible bar; Volume is
    the template's median.
    """
    closes = float(template["Close"].iloc[0]) * np.cumprod(1.0 + path_returns)
    opens = np.concatenate([[float(template["Close"].iloc[0])], closes[:-1]])
    hi_ext = float(
        (template["High"] / np.maximum(template["Open"], template["Close"]) - 1.0)
        .clip(lower=0).median()
    )
    lo_ext = float(
        (1.0 - template["Low"] / np.minimum(template["Open"], template["Close"]))
        .clip(lower=0).median()
    )
    index = pd.bdate_range(start=template.index[0], periods=horizon, name=template.index.name)
    return pd.DataFrame(
        {
            "Open": opens,
            "High": np.maximum(opens, closes) * (1.0 + hi_ext),
            "Low": np.minimum(opens, closes) * (1.0 - lo_ext),
            "Close": closes,
            "Volume": float(template["Volume"].median()),
        },
        index=index,
    )


def _simulate_sync(
    frame: pd.DataFrame,
    spec: registry.StrategySpec,
    request: SimulationRequest,
    n_paths: int,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    The CPU-bound half. Returns (historical metrics, simulation payload
    fields). Raises ValueError for anything that is the caller's fault.
    """
    results, metrics, trades = _run_backtest_sync(
        frame, spec, request.params, request.initial_capital,
        request.transaction_cost, request.seed,
    )
    trade_count = 0 if trades is None or trades.empty else len(trades)
    rng = np.random.default_rng(request.seed)

    if request.mode == "returns":
        invested = np.flatnonzero(results["position"].to_numpy() > 0)
        if invested.size == 0:
            raise ValueError(
                f"{spec.display_name} never invested over this window; there "
                "are no strategy returns to resample. Try 'prices' mode or a "
                "longer window."
            )
        source = results["returns"].to_numpy()[invested[0]:]
        horizon = _resolve_horizon(request.horizon_days, source.size)
        paths = draw(request.method, source, n_paths, horizon, rng, request.block_length_days)
        equity = equity_from_returns(paths, request.initial_capital)
    else:
        source = frame["Close"].pct_change().dropna().to_numpy()
        if source.size < 2:
            raise ValueError("Fewer than two price returns in this window; nothing to resample.")
        horizon = _resolve_horizon(request.horizon_days, source.size)
        price_paths = draw(request.method, source, n_paths, horizon, rng, request.block_length_days)
        equity = np.empty((n_paths, horizon + 1))
        equity[:, 0] = request.initial_capital
        for i in range(n_paths):
            synthetic = _synthetic_frame(frame, price_paths[i], horizon)
            backtester = Backtester(
                initial_capital=request.initial_capital,
                transaction_cost=request.transaction_cost,
                # Per-path seed: the result never depends on execution order.
                seed=None if request.seed is None else request.seed + i,
            )
            run = backtester.run(price_data=synthetic, model=spec.build(request.params))
            equity[i, 1:] = run["total"].to_numpy()
            if on_progress is not None and ((i + 1) % PROGRESS_EVERY == 0 or i + 1 == n_paths):
                on_progress(i + 1, n_paths)
        paths = equity[:, 1:] / equity[:, :-1] - 1.0

    # -- summaries ----------------------------------------------------------
    band_matrix = bands(equity, PERCENTILES)
    depth = max_drawdown(equity)
    duration = max_drawdown_duration(equity)
    wealth = terminal_wealth(equity)
    total_return = wealth / request.initial_capital - 1.0

    hist_depth = metrics.get("Max Drawdown")
    hist_duration = metrics.get("Max Drawdown Duration (Days)")
    hist_depth = None if hist_depth is None or not np.isfinite(hist_depth) else float(hist_depth)

    rows = [
        RiskRow(
            horizon_days=h,
            var_95=var(horizon_returns(paths, h), 0.95),
            cvar_95=cvar(horizon_returns(paths, h), 0.95),
            var_99=var(horizon_returns(paths, h), 0.99),
            cvar_99=cvar(horizon_returns(paths, h), 0.99),
        )
        for h in settings.SIM_VAR_HORIZONS_DAYS
        if h <= horizon
    ]

    payload: Dict[str, Any] = {
        "horizon_days": horizon,
        "resampled_from": int(source.size),
        "trade_count": trade_count,
        "bands": [
            FanBand(step=i, p05=float(r[0]), p25=float(r[1]), p50=float(r[2]),
                    p75=float(r[3]), p95=float(r[4]))
            for i, r in enumerate(band_matrix)
        ],
        "terminal": TerminalSummary(
            wealth=_summary(wealth),
            total_return=_summary(total_return),
            prob_loss=float(np.mean(wealth < request.initial_capital)),
        ),
        "drawdown": DrawdownSummary(
            depth=_summary(depth),
            duration_bars=_summary(duration.astype(float)),
            historical_depth=hist_depth,
            historical_duration_bars=None if hist_duration is None else int(hist_duration),
            prob_worse_than_historical=(
                None if hist_depth is None else prob_worse_drawdown(depth, hist_depth)
            ),
        ),
        "risk": RiskSummary(
            rows=rows,
            prob_ruin=prob_ruin(equity, request.ruin_threshold),
            ruin_threshold=request.ruin_threshold,
        ),
        "sample_paths": [
            [round(float(v), 2) for v in equity[i]] for i in range(min(request.include_paths, n_paths))
        ],
    }
    return metrics, payload


def _resolve_horizon(requested: Optional[int], history: int) -> int:
    if requested is None:
        return history
    cap = max(settings.SIM_MAX_HORIZON_DAYS, history)
    if requested > cap:
        raise ValueError(
            f"horizon_days={requested} exceeds the cap of {cap} "
            f"(max(SIM_MAX_HORIZON_DAYS, history length))."
        )
    return requested


def resolve_n_paths(request: SimulationRequest) -> int:
    """Default and cap depend on the mode; a request over the cap is a 422."""
    if request.mode == "returns":
        default, cap = settings.SIM_DEFAULT_PATHS, settings.SIM_MAX_PATHS
    else:
        default, cap = settings.SIM_DEFAULT_RERUN_PATHS, settings.SIM_MAX_RERUN_PATHS
    n = default if request.n_paths is None else request.n_paths
    if n > cap:
        raise ValueError(f"n_paths={n} exceeds the cap of {cap} for mode '{request.mode}'.")
    return n


def compose_caveat(
    spec: registry.StrategySpec, request: SimulationRequest, price_basis: Optional[str], trade_count: int
) -> Optional[str]:
    parts: List[str] = []
    if spec.caveat:
        parts.append(spec.caveat)
    if request.mode == "returns" and trade_count >= 2:
        parts.append(
            f"'returns' mode resamples {spec.display_name}'s realised returns as if "
            "they were independent of the price path, but its signals depend on "
            "the path. Treat the bands as a lower bound on dispersion, or use "
            "'prices' mode."
        )
    if price_basis != "served":
        parts.append(
            f"{request.symbol} is not read-time adjusted (price_basis={price_basis!r}); "
            "a bootstrap reproduces any artefact in its stored history."
        )
    return " ".join(parts) or None


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=SimulationResponse,
    summary="Monte Carlo a strategy on stored history: fan bands, drawdown and tail risk",
    responses={
        404: {"description": "Unknown symbol or strategy"},
        422: {"description": "Invalid parameters, bad date range, no data, over a cap, or an unverified asset"},
    },
)
async def run_simulation(
    request: SimulationRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> SimulationResponse:
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    try:
        spec = registry.get(request.strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    if spec.input_contract == "multi":
        raise HTTPException(
            status_code=422,
            detail=f"Strategy '{spec.id}' takes a multi-symbol frame and cannot be simulated on one symbol.",
        )

    try:
        n_paths = resolve_n_paths(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    asset = await repo.find_asset(request.symbol)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {request.symbol!r}")

    if asset.price_basis != "served" and not request.allow_unverified:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{asset.symbol} is not read-time adjusted (price_basis="
                f"{asset.price_basis!r}). A bootstrap would reproduce any artefact "
                "in its stored history. Send allow_unverified=true to simulate it anyway."
            ),
        )

    start = request.start.replace(tzinfo=timezone.utc) if request.start.tzinfo is None else request.start
    end = request.end.replace(tzinfo=timezone.utc) if request.end.tzinfo is None else request.end

    records = await repo.fetch_range(symbol=request.symbol, asset_class=None, start=start, end=end)
    frame = records_to_frame(records)
    if frame.empty:
        raise HTTPException(
            status_code=422,
            detail=f"No bars stored for {request.symbol!r} between {start.date()} and {end.date()}.",
        )

    try:
        metrics, sim = await run_in_threadpool(_simulate_sync, frame, spec, request, n_paths)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    return SimulationResponse(
        symbol=asset.symbol,
        strategy_id=spec.id,
        strategy_name=spec.display_name,
        start=start,
        end=end,
        bars=len(frame),
        params={p.name: request.params.get(p.name, p.default) for p in spec.params},
        initial_capital=request.initial_capital,
        seed=request.seed,
        mode=request.mode,
        method=request.method,
        n_paths=n_paths,
        horizon_days=sim["horizon_days"],
        block_length_days=request.block_length_days,
        resampled_from=sim["resampled_from"],
        historical={k: _json_safe(v) for k, v in (metrics or {}).items()},
        bands=sim["bands"],
        terminal=sim["terminal"],
        drawdown=sim["drawdown"],
        risk=sim["risk"],
        sample_paths=sim["sample_paths"],
        caveat=compose_caveat(spec, request, asset.price_basis, sim["trade_count"]),
    )
