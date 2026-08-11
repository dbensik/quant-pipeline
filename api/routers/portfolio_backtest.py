"""
api/routers/portfolio_backtest.py
Multi-asset portfolio backtests, with post-hoc risk metrics.

Separate from /api/v1/backtest rather than a mode of it. The two differ in
almost everything a caller supplies and receives: this one takes many symbols
plus target weights and volatility targeting, returns one portfolio history
rather than one per symbol, and accepts exactly the multi-asset strategies the
single-symbol endpoint rejects with a 422. One endpoint with two mutually
exclusive shapes would be harder to use and to document than two.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from alpha_models import registry
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo
from api.frames import frames_to_wide_close, records_to_frame
from api.routers.backtest import _json_safe

router = APIRouter(prefix="/api/v1/backtest/portfolio", tags=["backtest"])

MAX_SYMBOLS = 50


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class PortfolioBacktestRequest(BaseModel):
    symbols: List[str] = Field(description="Constituents, two or more")
    strategy_id: str = Field(description="Registry id — must be a multi-asset strategy")
    start: datetime
    end: datetime
    params: Dict[str, Any] = Field(default_factory=dict)
    weights: Optional[Dict[str, float]] = Field(
        default=None,
        description=(
            "Target weight per symbol. Omitted means equal-weight. Must cover "
            "every requested symbol when supplied — a partial mapping would "
            "silently zero-weight the rest."
        ),
    )
    initial_capital: float = Field(default=100_000.0, gt=0)
    seed: Optional[int] = Field(
        default=42,
        description=(
            "Seed for simulated slippage, so the same request returns the same "
            "result. Pass null for unseeded, non-reproducible behaviour."
        ),
    )
    enable_vol_targeting: bool = Field(default=False)
    target_volatility: float = Field(default=0.15, gt=0)

    # Both default to PortfolioRiskManager's own defaults, so an existing
    # request returns exactly what it returned before. They are exposed because
    # those defaults dominate the result and were previously unreachable — a
    # backtest was reporting the risk manager's behaviour, not the strategy's.
    max_trade_risk_pct: float = Field(
        default=0.02,
        gt=0,
        le=1.0,
        description=(
            "Cap on a single BUY as a fraction of total equity. The default "
            "0.02 scales every purchase to 2%, which makes `weights` inert "
            "above 2% and leaves a portfolio ~98% in cash: measured over "
            "2015-2026, paired switching ran 49 trades at 1.9% average "
            "exposure for +1.03%, versus the same 49 trades at 95.7% exposure "
            "for +48.49% uncapped. Pass 1.0 to size purely from `weights`."
        ),
    )
    max_portfolio_drawdown_pct: float = Field(
        default=0.20,
        gt=0,
        le=1.0,
        description=(
            "Drawdown from the high-water mark at which new BUYs stop. NOT "
            "recoverable: the high-water mark only rises, and a portfolio that "
            "cannot buy cannot recover, so once tripped it halts buying for the "
            "rest of the run. Measured over 2015-2026, paired switching sat 0% "
            "invested from 2023 onward with a perfectly flat equity curve "
            "(standard deviation 0.0000, zero new highs). Pass 1.0 to disable."
        ),
    )

    include_risk: bool = Field(
        default=True, description="Compute VaR/CVaR on the resulting returns"
    )
    include_equity_curve: bool = Field(default=True)


class PortfolioEquityPoint(BaseModel):
    time: datetime
    total: float


class PortfolioBacktestResponse(BaseModel):
    symbols: List[str]
    strategy_id: str
    strategy_name: str
    start: datetime
    end: datetime
    bars: int
    params: Dict[str, Any]
    weights: Dict[str, float] = Field(description="Weights actually used")
    initial_capital: float
    seed: Optional[int]
    # Echoed for the same reason as `weights`: these dominate the metrics, so a
    # saved result is not interpretable without knowing what they were.
    max_trade_risk_pct: float
    max_portfolio_drawdown_pct: float
    metrics: Dict[str, Any]
    risk_metrics: Dict[str, Any] = Field(
        default_factory=dict, description="VaR/CVaR etc., empty when not requested"
    )
    caveat: Optional[str] = None
    equity_curve: List[PortfolioEquityPoint] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Signal construction
# ---------------------------------------------------------------------------

def _build_signals(
    spec: registry.StrategySpec,
    model: Any,
    price_data: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """
    Multi-asset strategies do NOT share one output shape, so signals are
    assembled per shape — ported from the Streamlit AnalysisController, which
    dispatched on isinstance.

    Dispatch is on `spec.signal_shape`, declared in the registry. It used to be
    hardcoded sets of strategy ids right here, which meant a new multi-asset
    strategy could not work without editing this router — and worse, failed
    UNSAFELY: an unlisted id fell through to the per-symbol branch and was
    handed each symbol's own frame in isolation, so a cross-asset strategy
    would silently compare nothing and still return plausible numbers.
    """
    shape = spec.signal_shape

    if shape == "wide_per_asset":
        # Wide frame in, one position column per asset out. Split into the
        # per-ticker dict the PortfolioBacktester consumes.
        wide = frames_to_wide_close(price_data).dropna()
        signals = model.generate_signals(wide)
        return {
            column: signals[[column]].rename(columns={column: "signal"})
            for column in signals.columns
        }

    if shape == "wide_portfolio":
        # Wide frame in, one 'signal' column for the basket as a single unit.
        wide = frames_to_wide_close(price_data).dropna()
        return {"Portfolio": model.generate_signals(wide)}

    if shape == "calendar_shared":
        # Rebalance schedules depend only on the calendar, so any constituent's
        # frame gives the dates; every symbol then shares them.
        any_frame = next(iter(price_data.values()))
        rebalance = model.generate_signals(any_frame)
        return {symbol: rebalance for symbol in price_data}

    # per_symbol: each symbol's own frame produces its own signal.
    #
    # UNREACHABLE from the route as it stands — this function has one caller and
    # the route 422s anything whose input_contract is not "multi" before getting
    # here, and test_every_registered_strategy_declares_a_wired_shape forbids a
    # multi-asset spec from declaring per_symbol. It is kept as the safety net
    # for a future misdeclaration, which is precisely the case that used to be
    # silent: under id-dispatch an unlisted strategy landed here and was handed
    # each symbol's frame in isolation.
    return {
        symbol: model.generate_signals(frame) for symbol, frame in price_data.items()
    }


def _run_sync(
    spec: registry.StrategySpec,
    model: Any,
    price_data: Dict[str, pd.DataFrame],
    weights: Dict[str, float],
    request: PortfolioBacktestRequest,
) -> tuple:
    """CPU-bound half: signals, backtest, metrics, risk."""
    from backtesting.portfolio_backtester import PortfolioBacktester
    from backtesting.risk_manager import PortfolioRiskManager

    signals_data = _build_signals(spec, model, price_data)

    backtester = PortfolioBacktester(
        initial_capital=request.initial_capital,
        enable_vol_targeting=request.enable_vol_targeting,
        target_volatility=request.target_volatility,
        seed=request.seed,
        # Constructed explicitly rather than letting PortfolioBacktester build a
        # default one, so the limits that shaped the result are the caller's.
        risk_manager=PortfolioRiskManager(
            max_trade_risk_pct=request.max_trade_risk_pct,
            max_portfolio_drawdown_pct=request.max_portfolio_drawdown_pct,
        ),
    )
    portfolio, _holdings = backtester.run(price_data, signals_data, weights)
    metrics = backtester.get_performance_metrics()

    risk: Dict[str, Any] = {}
    if request.include_risk and portfolio is not None and not portfolio.empty:
        returns = portfolio.get("returns")
        if returns is not None and not returns.empty:
            from portfolio.risk_manager import RiskManager

            risk = RiskManager(portfolio_returns=returns).get_all_risk_metrics()

    return portfolio, metrics, risk


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=PortfolioBacktestResponse,
    summary="Backtest a multi-asset strategy across a portfolio",
    responses={
        404: {"description": "Unknown symbol or strategy"},
        422: {"description": "Single-asset strategy, bad weights, or no data"},
    },
)
async def run_portfolio_backtest(
    request: PortfolioBacktestRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> PortfolioBacktestResponse:
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    if len(request.symbols) < 2:
        raise HTTPException(
            status_code=422,
            detail=f"A portfolio needs at least 2 symbols; {len(request.symbols)} given.",
        )
    if len(request.symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"{len(request.symbols)} symbols requested; the limit is {MAX_SYMBOLS}.",
        )

    try:
        spec = registry.get(request.strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    if spec.input_contract != "multi":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Strategy '{spec.id}' is single-asset — use POST /api/v1/backtest. "
                "Filter the strategy list with ?input_contract=multi."
            ),
        )

    if spec.id == "pairs_trading" and len(request.symbols) != 2:
        raise HTTPException(
            status_code=422,
            detail=f"'pairs_trading' takes exactly 2 symbols; {len(request.symbols)} given.",
        )

    # Weights must cover every symbol. A partial mapping would silently
    # zero-weight the remainder, producing a smaller portfolio than requested
    # while still reporting the full symbol list.
    if request.weights is not None:
        missing = set(request.symbols) - set(request.weights)
        extra = set(request.weights) - set(request.symbols)
        if missing or extra:
            raise HTTPException(
                status_code=422,
                detail=(
                    "`weights` must name exactly the requested symbols. "
                    f"Missing: {sorted(missing)}. Unexpected: {sorted(extra)}."
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

    weights = request.weights or {
        symbol: 1.0 / len(price_data) for symbol in price_data
    }

    try:
        model = spec.build(request.params)
    except (ValueError, TypeError) as exc:
        # Cointegrated Mean Reversion needs a `weights` mapping the registry
        # cannot default, so it raises TypeError on a missing argument.
        raise HTTPException(status_code=422, detail=str(exc)) from None

    try:
        portfolio, metrics, risk = await run_in_threadpool(
            _run_sync, spec, model, price_data, weights, request
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    curve: List[PortfolioEquityPoint] = []
    if (
        request.include_equity_curve
        and portfolio is not None
        and not portfolio.empty
        and "total" in portfolio.columns
    ):
        curve = [
            PortfolioEquityPoint(time=idx.to_pydatetime(), total=float(row["total"]))
            for idx, row in portfolio.iterrows()
        ]

    effective_params = {
        p.name: request.params.get(p.name, p.default) for p in spec.params
    }

    return PortfolioBacktestResponse(
        symbols=list(request.symbols),
        strategy_id=spec.id,
        strategy_name=spec.display_name,
        start=start,
        end=end,
        bars=0 if portfolio is None or portfolio.empty else len(portfolio),
        params=effective_params,
        weights=weights,
        initial_capital=request.initial_capital,
        seed=request.seed,
        max_trade_risk_pct=request.max_trade_risk_pct,
        max_portfolio_drawdown_pct=request.max_portfolio_drawdown_pct,
        metrics={k: _json_safe(v) for k, v in (metrics or {}).items()},
        risk_metrics={k: _json_safe(v) for k, v in (risk or {}).items()},
        caveat=spec.caveat,
        equity_curve=curve,
    )
