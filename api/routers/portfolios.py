"""
api/routers/portfolios.py
Paper and manually-recorded portfolios: CRUD, trade log, derived state,
rebalancing previews.

SCOPE. This mirrors the split already chosen for signals: the REST API owns
portfolio management and the trade log, while `services/` keeps the signed
gRPC execution path and its Ed25519 audit log. This router does not execute
signed trades and does not talk to the gRPC service.

THE TRADE LOG IS THE ONLY STORED STATE. Cash, positions, average cost and P&L
are computed by core/portfolio.py on read. `portfolios.json` stored a trade
log AND a cash/positions ledger under one key; they disagreed, and
ExecutionService.GetPortfolio raised KeyError('cash') against the real file.

Phase 5 — decommissioning Streamlit
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from alpha_models import registry
from api.dependencies import get_market_data_repo, get_portfolio_repo
from api.paper_tracking import track
from api.risk_free import get_rate_repo, risk_free_refusal
from config import settings
from core.ledger import ShortTerms
from core.paper_history import latest_prices, load_history, new_york_today
from core.paper_runner import StaleSession, run_strategy
from core.paper_trader import META_STRATEGY, StrategyBinding, binding_of
from core.portfolio import (
    ACTIONS,
    META_CASH_FROM,
    META_SHORT_TERMS,
    MarketHistory,
    Portfolio,
    PortfolioState,
    Trade,
    cash_from,
    derive_state,
    rebalancing_orders,
    short_terms_of,
    trade_day,
    trade_refusal,
)
from core.rates import RateRepository, RiskFreeCoverageError
from db.repositories.market_data import TimescaleMarketDataRepo
from db.repositories.portfolios import TimescalePortfolioRepo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])

MAX_NAME_LENGTH = 100


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TradeIn(BaseModel):
    ticker: str
    action: str = Field(description="BUY or SELL")
    quantity: float = Field(gt=0, description="Always positive; `action` signs it")
    price: float = Field(gt=0)
    time: Optional[datetime] = Field(
        default=None, description="Execution time; defaults to now (UTC)"
    )
    costs: float = Field(default=0.0, ge=0, description="Commission; reduces cash")
    broker: Optional[str] = None
    notes: Optional[str] = None


class TradeOut(TradeIn):
    id: str


class PositionOut(BaseModel):
    ticker: str
    quantity: float = Field(description="Negative for a short position")
    average_price: float
    realised_pnl: float
    last_price: Optional[float] = None
    market_value: Optional[float] = None
    unrealised_pnl: Optional[float] = None


class PortfolioSummary(BaseModel):
    name: str
    initial_cash: float
    created_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class PortfolioShortTerms(BaseModel):
    """A portfolio's short terms (decision 49): without them it cannot short."""

    borrow_rate: float = Field(ge=0, le=1, description="Annual fee on the shorts' value")
    max_gross: float = Field(gt=0, le=10, description="Gross exposure cap, x equity")
    maintenance: float = Field(
        gt=0, lt=1, description="Equity / gross exposure below which it is flagged"
    )


class FlowOut(BaseModel):
    """A dated corporate action that changed cash or a holding without a trade."""

    day: str
    kind: str = Field(description="dividend, split, or manual_factor (listed, not modelled)")
    ticker: str
    amount: float = Field(description="Cash change for a dividend; the ratio or factor otherwise")
    quantity: float = Field(description="Quantity held when it applied")


class PaperStrategyOut(BaseModel):
    """A strategy attached to a paper portfolio (decision 50)."""

    strategy: str
    params: Dict[str, Any]
    symbols: List[str]
    weights: Dict[str, float] = Field(
        description="Weight per symbol an entry buys; empty for a strategy that sizes its legs"
    )
    transaction_cost: float = Field(description="Per-fill cost as a fraction of its value")
    attached_on: str = Field(description="New York date; the first session is the next bar")
    signal_from: str = Field(description="First bar every signal frame starts at")
    last_session: str = Field(description="Newest session traded (the attach date before any)")


class PortfolioStateOut(BaseModel):
    name: str
    initial_cash: float
    cash: float
    positions: List[PositionOut]
    realised_pnl: float
    unrealised_pnl: float
    market_value: float
    total_equity: float
    trade_count: int
    unpriced: List[str] = Field(
        default_factory=list,
        description=(
            "Open positions with no price available. Excluded from market "
            "value rather than valued at cost, which would overstate equity "
            "while looking like a complete answer."
        ),
    )
    priced_at: Optional[datetime] = None
    # Decision 49: derived from the trade log, corporate actions and ^IRX.
    free_cash: float = Field(description="Cash less the shorts' collateral: what a buy may spend")
    long_value: float
    short_value: float = Field(description="Priced shorts, as a positive value")
    gross_exposure: float
    dividends_received: float
    dividends_paid: float = Field(description="Owed on shorts over their ex-dates")
    interest: float = Field(description="^IRX earned on free cash, by calendar day")
    borrow_paid: float
    flows: List[FlowOut] = Field(default_factory=list)
    unhandled_actions: List[FlowOut] = Field(
        default_factory=list,
        description="Corporate actions on a holding that are not modelled (a spinoff factor)",
    )
    short_terms: Optional[PortfolioShortTerms] = None
    maintenance_required: Optional[float] = None
    margin_excess: Optional[float] = None
    below_maintenance: bool = False
    strategy: Optional[PaperStrategyOut] = Field(
        default=None, description="The attached strategy that trades it (decision 50)"
    )


class AttachStrategyRequest(BaseModel):
    strategy_id: str = Field(description="Registry id (GET /strategies)")
    symbols: List[str] = Field(
        description="One for a single-asset strategy; two or more for a multi-asset one"
    )
    params: Dict[str, Any] = Field(default_factory=dict, description="Fixed at attach")
    transaction_cost: float = Field(
        default=0.001,
        ge=0,
        le=0.05,
        description="Per-fill cost as a fraction of its value, beside $1 a fill (the backtests')",
    )


class PaperFillOut(BaseModel):
    ticker: str
    action: str
    quantity: float
    price: float = Field(description="The session's raw open")
    costs: float
    time: datetime
    notes: Optional[str] = None


class RefusedFillOut(BaseModel):
    ticker: str
    action: str
    quantity: float
    price: float
    reason: str


class PaperSessionOut(BaseModel):
    session: str = Field(description="The bar whose open filled")
    signal_day: str = Field(description="The bar whose close decided it")
    fills: List[PaperFillOut]
    refused: List[RefusedFillOut] = Field(
        default_factory=list, description="Fills the trade check would not record"
    )


class HoldingOut(BaseModel):
    ticker: str
    wanted: Optional[str] = Field(
        description="long, short or flat: what the signals since attach ask; null before any"
    )
    held: float
    drift: bool = Field(description="What is held is not what is wanted")


class PaperRunOut(BaseModel):
    name: str
    strategy: str
    written: bool = Field(description="False for a dry run: nothing was stored")
    newest_bar: Optional[str]
    last_session: str
    sessions: List[PaperSessionOut]
    held_sessions: List[str] = Field(
        default_factory=list, description="Sessions waiting for a basket symbol's bar"
    )
    missing_bars: Dict[str, List[str]] = Field(default_factory=dict)
    holdings: List[HoldingOut] = Field(default_factory=list)
    next_signals: Dict[str, float] = Field(
        default_factory=dict,
        description="The newest bar's signal or target where it asks for a trade at the next open",
    )
    ruined_on: Optional[str] = None
    flags: List[str] = Field(default_factory=list)


class CreatePortfolioRequest(BaseModel):
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    initial_cash: float = Field(default=100_000.0, gt=0)
    metadata: Optional[Dict[str, Any]] = None
    short_terms: Optional[PortfolioShortTerms] = Field(
        default=None, description="Set to let the portfolio short; none means it cannot"
    )


class RebalanceRequest(BaseModel):
    target_weights: Dict[str, float] = Field(description="Ticker to target weight of total equity")
    minimum_order_value: float = Field(
        default=10.0, ge=0, description="Skip orders smaller than this"
    )


class RebalanceOrder(BaseModel):
    ticker: str
    action: str
    quantity: int
    price: float
    value: float
    current_weight: float
    target_weight: float


class RebalancePreview(BaseModel):
    name: str
    total_equity: float
    orders: List[RebalanceOrder]
    unpriced: List[str] = Field(
        default_factory=list,
        description="Tickers skipped because no price was available to size them",
    )


# ---------------------------------------------------------------------------
# Pricing helper
# ---------------------------------------------------------------------------


async def _latest_prices(
    tickers: List[str], market: TimescaleMarketDataRepo
) -> tuple[Dict[str, float], Optional[datetime]]:
    """Most recent stored close per ticker (core.paper_history.latest_prices)."""
    return await latest_prices(tickers, market)


async def _history(
    portfolio: Portfolio,
    extra: List[str],
    market: TimescaleMarketDataRepo,
    rates: RateRepository,
) -> Optional[MarketHistory]:
    """
    Corporate actions, raw closes and ^IRX from the first trade (or the
    portfolio's `cash_from`, when earlier) to today.
    """
    tickers = {t.ticker for t in portfolio.trades} | set(extra)
    start = cash_from(portfolio.metadata)
    days = [trade_day(t) for t in portfolio.trades] + ([start] if start else [])
    if not days:
        return None
    return await load_history(tickers, min(days), new_york_today(), market, rates, cash_from=start)


def _terms(portfolio: Portfolio) -> Optional[ShortTerms]:
    return short_terms_of(portfolio.metadata)


def _flow(f) -> FlowOut:
    return FlowOut(
        day=f.day.isoformat(), kind=f.kind, ticker=f.ticker, amount=f.amount, quantity=f.quantity
    )


def _state_response(
    portfolio: Portfolio,
    state: PortfolioState,
    priced_at: Optional[datetime],
) -> PortfolioStateOut:
    terms = _terms(portfolio)
    return PortfolioStateOut(
        name=portfolio.name,
        initial_cash=state.initial_cash,
        cash=state.cash,
        positions=[
            PositionOut(
                ticker=p.ticker,
                quantity=p.quantity,
                average_price=p.average_price,
                realised_pnl=p.realised_pnl,
                last_price=p.last_price,
                market_value=p.market_value,
                unrealised_pnl=p.unrealised_pnl,
            )
            for p in state.positions
        ],
        realised_pnl=state.realised_pnl,
        unrealised_pnl=state.unrealised_pnl,
        market_value=state.market_value,
        total_equity=state.total_equity,
        trade_count=state.trade_count,
        unpriced=state.unpriced,
        priced_at=priced_at,
        free_cash=state.free_cash,
        long_value=state.long_value,
        short_value=state.short_value,
        gross_exposure=state.gross_exposure,
        dividends_received=state.dividends_received,
        dividends_paid=state.dividends_paid,
        interest=state.interest,
        borrow_paid=state.borrow_paid,
        flows=[_flow(f) for f in state.flows],
        unhandled_actions=[_flow(f) for f in state.unhandled_actions],
        short_terms=None if terms is None else PortfolioShortTerms(**vars(terms)),
        maintenance_required=state.maintenance_required,
        margin_excess=state.margin_excess,
        below_maintenance=state.below_maintenance,
        strategy=_strategy_out(binding_of(portfolio.metadata)),
    )


def _strategy_out(binding: Optional[StrategyBinding]) -> Optional[PaperStrategyOut]:
    return None if binding is None else PaperStrategyOut(**binding.to_metadata())


def _refuse_if_attached(portfolio: Portfolio, what: str) -> None:
    """An attached portfolio's record is its strategy's alone (decision 50)."""
    binding = binding_of(portfolio.metadata)
    if binding is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{portfolio.name!r} is traded by '{binding.strategy}'; {what} would mix "
                "a manual decision into its record. Detach the strategy first."
            ),
        )


async def _require(name: str, repo: TimescalePortfolioRepo) -> Portfolio:
    """
    Load a portfolio or 404.

    PortfolioManager.execute_trade did something different and dangerous: when
    the named portfolio was missing and exactly one existed, it silently traded
    in THAT one. A typo executed against the wrong portfolio. There is no
    fallback here.
    """
    portfolio = await repo.get_portfolio(name)
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"No portfolio named {name!r}.")
    return portfolio


# ---------------------------------------------------------------------------
# Portfolio CRUD
# ---------------------------------------------------------------------------


@router.get("", response_model=List[PortfolioSummary], summary="List portfolios")
async def list_portfolios(
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> List[PortfolioSummary]:
    return [
        PortfolioSummary(
            name=p.name,
            initial_cash=p.initial_cash,
            created_at=p.created_at,
            metadata=p.metadata,
        )
        for p in await repo.list_portfolios()
    ]


@router.post(
    "",
    response_model=PortfolioSummary,
    status_code=201,
    summary="Create a portfolio",
    responses={409: {"description": "A portfolio with that name already exists"}},
)
async def create_portfolio(
    request: CreatePortfolioRequest,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> PortfolioSummary:
    metadata = dict(request.metadata or {})
    if request.short_terms is not None:
        try:
            ShortTerms(**request.short_terms.model_dump())
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        metadata[META_SHORT_TERMS] = request.short_terms.model_dump()
    try:
        created = await repo.create_portfolio(
            name=request.name,
            initial_cash=request.initial_cash,
            metadata=metadata or None,
        )
    except ValueError as exc:
        # 409, not 422: the request is well-formed, it conflicts with state.
        raise HTTPException(status_code=409, detail=str(exc)) from None

    return PortfolioSummary(
        name=created.name,
        initial_cash=created.initial_cash,
        created_at=created.created_at,
        metadata=created.metadata,
    )


@router.delete(
    "/{name}",
    status_code=204,
    summary="Delete a portfolio and its trades",
    responses={404: {"description": "No such portfolio"}},
)
async def delete_portfolio(
    name: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> None:
    if not await repo.delete_portfolio(name):
        raise HTTPException(status_code=404, detail=f"No portfolio named {name!r}.")


# ---------------------------------------------------------------------------
# Derived state
# ---------------------------------------------------------------------------


@router.get(
    "/{name}",
    response_model=PortfolioStateOut,
    summary="Portfolio state derived from its trade log",
    responses={404: {"description": "No such portfolio"}},
)
async def get_portfolio_state(
    name: str,
    include_prices: bool = Query(
        default=True,
        description=(
            "Value open positions at the latest stored close. Set false for "
            "cost-basis-only state, which needs no market-data lookup."
        ),
    ),
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    market: TimescaleMarketDataRepo = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_rate_repo),
) -> PortfolioStateOut:
    portfolio = await _require(name, repo)
    history = await _history(portfolio, [], market, rates)
    terms = _terms(portfolio)

    prices: Dict[str, float] = {}
    priced_at: Optional[datetime] = None
    if include_prices:
        # Derive once unpriced to learn which tickers are actually open —
        # cheaper than pricing every ticker that ever appeared in the log.
        provisional = derive_state(
            portfolio.trades, portfolio.initial_cash, history=history, terms=terms
        )
        tickers = [p.ticker for p in provisional.positions]
        if tickers:
            prices, priced_at = await _latest_prices(tickers, market)

    state = derive_state(
        portfolio.trades, portfolio.initial_cash, prices, history=history, terms=terms
    )
    return _state_response(portfolio, state, priced_at)


# ---------------------------------------------------------------------------
# Trade log
# ---------------------------------------------------------------------------


@router.get(
    "/{name}/trades",
    response_model=List[TradeOut],
    summary="The portfolio's trade log, oldest first",
    responses={404: {"description": "No such portfolio"}},
)
async def list_trades(
    name: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> List[TradeOut]:
    portfolio = await _require(name, repo)
    return [
        TradeOut(
            id=t.id or "",
            ticker=t.ticker,
            action=t.action,
            quantity=t.quantity,
            price=t.price,
            time=t.ts,
            costs=t.costs,
            broker=t.broker,
            notes=t.notes,
        )
        for t in sorted(portfolio.trades, key=lambda t: (t.ts, t.id or ""))
    ]


@router.post(
    "/{name}/trades",
    response_model=TradeOut,
    status_code=201,
    summary="Record a trade",
    responses={
        404: {"description": "No such portfolio"},
        422: {"description": "Bad action, insufficient free cash, or a short not allowed"},
    },
)
async def add_trade(
    name: str,
    trade: TradeIn,
    allow_overdraft: bool = Query(
        default=False,
        description=(
            "Permit a trade that takes free cash negative or gross exposure past "
            "the portfolio's cap. Off by default so paper trading cannot run on "
            "unlimited leverage — PortfolioManager never checked cash at all. Turn "
            "it on when RECORDING trades that already happened elsewhere, where "
            "rejecting them would be wrong. A short still needs short terms."
        ),
    ),
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    market: TimescaleMarketDataRepo = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_rate_repo),
) -> TradeOut:
    action = trade.action.upper()
    if action not in ACTIONS:
        raise HTTPException(
            status_code=422,
            detail=f"`action` must be one of {list(ACTIONS)}; got {trade.action!r}.",
        )

    portfolio = await _require(name, repo)
    _refuse_if_attached(portfolio, "a manual trade")

    timestamp = trade.time or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    domain = Trade(
        ticker=trade.ticker.upper(),
        action=action,
        quantity=trade.quantity,
        price=trade.price,
        ts=timestamp,
        costs=trade.costs,
        broker=trade.broker,
        notes=trade.notes,
    )

    history = await _history(portfolio, [domain.ticker], market, rates)
    if history is None:
        history = await load_history(
            [domain.ticker], trade_day(domain), new_york_today(), market, rates
        )
    terms = _terms(portfolio)
    open_now = derive_state(portfolio.trades, portfolio.initial_cash, history=history)
    prices, _ = await _latest_prices(
        sorted({p.ticker for p in open_now.positions} | {domain.ticker}), market
    )
    asset = await market.find_asset(domain.ticker)
    refusal = trade_refusal(
        portfolio.trades,
        domain,
        portfolio.initial_cash,
        prices,
        history=history,
        terms=terms,
        asset_class=None if asset is None else asset.asset_class,
        shortable=settings.SHORTABLE_ASSET_CLASSES,
        allow_overdraft=allow_overdraft,
    )
    if refusal:
        raise HTTPException(status_code=422, detail=refusal)

    stored = await repo.add_trade(name, domain)
    if stored is None:
        raise HTTPException(status_code=404, detail=f"No portfolio named {name!r}.")

    return TradeOut(
        id=stored.id or "",
        ticker=stored.ticker,
        action=stored.action,
        quantity=stored.quantity,
        price=stored.price,
        time=stored.ts,
        costs=stored.costs,
        broker=stored.broker,
        notes=stored.notes,
    )


@router.delete(
    "/{name}/trades/{trade_id}",
    status_code=204,
    summary="Remove a trade from the log",
    responses={404: {"description": "No such portfolio or trade"}},
)
async def delete_trade(
    name: str,
    trade_id: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> None:
    portfolio = await repo.get_portfolio(name)
    if portfolio is not None:
        _refuse_if_attached(portfolio, "removing a trade")
    if not await repo.delete_trade(name, trade_id):
        raise HTTPException(
            status_code=404,
            detail=f"No trade {trade_id!r} in portfolio {name!r}.",
        )


# ---------------------------------------------------------------------------
# Rebalancing
# ---------------------------------------------------------------------------


@router.post(
    "/{name}/rebalance",
    response_model=RebalancePreview,
    summary="Preview orders to reach target weights",
    responses={
        404: {"description": "No such portfolio"},
        422: {"description": "Weights out of range"},
    },
)
async def preview_rebalance(
    name: str,
    request: RebalanceRequest,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    market: TimescaleMarketDataRepo = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_rate_repo),
) -> RebalancePreview:
    """
    A PREVIEW. It returns orders; it does not record them. Post them to
    /trades to act on them — the Streamlit tool executed straight from the
    preview button, so there was no point at which the orders could be
    inspected and declined.
    """
    if any(weight < 0 for weight in request.target_weights.values()):
        raise HTTPException(status_code=422, detail="Target weights must not be negative.")
    total = sum(request.target_weights.values())
    if total > 1.0 + 1e-9:
        raise HTTPException(
            status_code=422,
            detail=f"Target weights sum to {total:.4f}; they must not exceed 1.0.",
        )

    portfolio = await _require(name, repo)
    history = await _history(portfolio, [], market, rates)
    terms = _terms(portfolio)
    state = derive_state(portfolio.trades, portfolio.initial_cash, history=history, terms=terms)

    tickers = sorted({p.ticker for p in state.positions} | set(request.target_weights))
    prices, _ = await _latest_prices(tickers, market)

    priced_state = derive_state(
        portfolio.trades, portfolio.initial_cash, prices, history=history, terms=terms
    )
    orders = rebalancing_orders(
        priced_state,
        request.target_weights,
        prices,
        minimum_order_value=request.minimum_order_value,
    )

    return RebalancePreview(
        name=portfolio.name,
        total_equity=priced_state.total_equity,
        orders=[RebalanceOrder(**order) for order in orders],
        unpriced=[t for t in tickers if t not in prices],
    )


# ---------------------------------------------------------------------------
# Short terms (decision 49)
# ---------------------------------------------------------------------------


@router.put(
    "/{name}/short-terms",
    response_model=PortfolioShortTerms,
    summary="Set the borrow rate and margin terms that let a portfolio short",
    responses={
        404: {"description": "No such portfolio"},
        422: {"description": "Terms that cannot hold"},
    },
)
async def set_short_terms(
    name: str,
    terms: PortfolioShortTerms,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> PortfolioShortTerms:
    """
    Facts about the account this portfolio mirrors; never defaulted. Changing
    them re-derives past borrow at the new rate: the terms are the account's,
    not a dated history.
    """
    try:
        ShortTerms(**terms.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    portfolio = await _require(name, repo)
    metadata = {**(portfolio.metadata or {}), META_SHORT_TERMS: terms.model_dump()}
    if not await repo.set_metadata(name, metadata):
        raise HTTPException(status_code=404, detail=f"No portfolio named {name!r}.")
    return terms


@router.delete(
    "/{name}/short-terms",
    status_code=204,
    summary="Remove a portfolio's short terms",
    responses={
        404: {"description": "No such portfolio"},
        409: {"description": "A short is open, or a strategy that shorts is attached"},
    },
)
async def clear_short_terms(
    name: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> None:
    portfolio = await _require(name, repo)
    binding = binding_of(portfolio.metadata)
    if binding is not None and registry.get(binding.strategy).signal_shape == "target":
        raise HTTPException(
            status_code=409,
            detail=f"'{binding.strategy}' shorts and is attached; detach it first.",
        )
    shorts = [
        p.ticker
        for p in derive_state(portfolio.trades, portfolio.initial_cash).positions
        if p.quantity < 0
    ]
    if shorts:
        raise HTTPException(
            status_code=409,
            detail=f"Short positions are open ({', '.join(shorts)}); cover them first.",
        )
    metadata = {k: v for k, v in (portfolio.metadata or {}).items() if k != META_SHORT_TERMS}
    await repo.set_metadata(name, metadata or None)


# ---------------------------------------------------------------------------
# An attached strategy (decision 50, research/paper-trader-2026-10-20.md)
# ---------------------------------------------------------------------------

#: As the portfolio backtest's limit.
MAX_STRATEGY_SYMBOLS = 50


@router.put(
    "/{name}/strategy",
    response_model=PaperStrategyOut,
    summary="Attach a strategy that trades this paper portfolio forward",
    responses={
        404: {"description": "No such portfolio, strategy or symbol"},
        409: {"description": "The portfolio has trades, or a strategy is already attached"},
        422: {"description": "A basket or parameters the strategy cannot trade"},
    },
)
async def attach_strategy(
    name: str,
    request: AttachStrategyRequest,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    market: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> PaperStrategyOut:
    """
    Forward only: the first session traded is the bar after today (New York),
    decided on today's close. Only on a portfolio with no trades, so its
    record is the strategy's alone; manual trades are then 409 until it is
    detached. Parameters are fixed here.
    """
    portfolio = await _require(name, repo)
    if binding_of(portfolio.metadata) is not None:
        raise HTTPException(status_code=409, detail=f"{name!r} already has a strategy; detach it.")
    if portfolio.trades:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{name!r} has {len(portfolio.trades)} trade(s). A strategy is attached to an "
                "empty portfolio, so its record is the strategy's alone."
            ),
        )
    try:
        spec = registry.get(request.strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    symbols = list(dict.fromkeys(s.upper() for s in request.symbols))
    if spec.input_contract == "single" and len(symbols) != 1:
        raise HTTPException(
            status_code=422,
            detail=f"'{spec.id}' is single-asset: it trades exactly 1 symbol; {len(symbols)} given.",
        )
    if spec.input_contract == "multi" and not 2 <= len(symbols) <= MAX_STRATEGY_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"'{spec.id}' needs 2 to {MAX_STRATEGY_SYMBOLS} symbols; {len(symbols)} given."
            ),
        )
    if spec.id == "pairs_trading" and len(symbols) != 2:
        raise HTTPException(
            status_code=422,
            detail=f"'pairs_trading' takes exactly 2 symbols; {len(symbols)} given.",
        )
    shorts = spec.signal_shape == "target"
    if shorts and _terms(portfolio) is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"'{spec.id}' shorts, and {name!r} has no short terms. Set them first "
                "(PUT /portfolios/{name}/short-terms)."
            ),
        )

    classes, firsts = {}, []
    for symbol in symbols:
        asset = await market.find_asset(symbol)
        if asset is None:
            raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol!r}")
        classes[symbol] = asset.asset_class
        try:
            settings.lot_size_for(asset.asset_class)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        if shorts and asset.asset_class not in settings.SHORTABLE_ASSET_CLASSES:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"'{spec.id}' shorts, and {symbol} is {asset.asset_class}: only "
                    f"{', '.join(sorted(settings.SHORTABLE_ASSET_CLASSES))} can be shorted."
                ),
            )
        records = await market.fetch_range(
            symbol=symbol,
            asset_class=None,
            start=datetime(1970, 1, 1, tzinfo=timezone.utc),
            end=datetime.now(timezone.utc),
            adjust="none",
        )
        if not records:
            raise HTTPException(status_code=422, detail=f"No bars stored for {symbol!r}.")
        firsts.append(min(r.ohlcv.timestamp.utc for r in records).date())
    if len({c == "crypto" for c in classes.values()}) > 1:
        raise HTTPException(
            status_code=422,
            detail=(
                "The basket mixes crypto with exchange-traded assets: they share no session "
                "calendar (crypto trades on weekends), so no session has a bar for every symbol."
            ),
        )
    try:
        spec.build(request.params)
        weights = {} if shorts else spec.default_weights(symbols, request.params)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    today = new_york_today()
    binding = StrategyBinding(
        strategy=spec.id,
        params={p.name: request.params.get(p.name, p.default) for p in spec.params},
        symbols=tuple(symbols),
        weights=weights,
        transaction_cost=request.transaction_cost,
        attached_on=today,
        signal_from=max(firsts),
        last_session=today,
    )
    metadata = {
        META_CASH_FROM: today.isoformat(),
        **(portfolio.metadata or {}),
        META_STRATEGY: binding.to_metadata(),
    }
    if not await repo.set_metadata(name, metadata):
        raise HTTPException(status_code=404, detail=f"No portfolio named {name!r}.")
    return _strategy_out(binding)


@router.delete(
    "/{name}/strategy",
    status_code=204,
    summary="Detach the strategy: its positions stay, and the portfolio becomes manual",
    responses={404: {"description": "No such portfolio, or none attached"}},
)
async def detach_strategy(
    name: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> None:
    portfolio = await _require(name, repo)
    if binding_of(portfolio.metadata) is None:
        raise HTTPException(status_code=404, detail=f"{name!r} has no strategy attached.")
    metadata = {k: v for k, v in (portfolio.metadata or {}).items() if k != META_STRATEGY}
    await repo.set_metadata(name, metadata or None)


@router.post(
    "/{name}/run",
    response_model=PaperRunOut,
    summary="Trade the attached strategy over the sessions not yet traded",
    responses={
        404: {"description": "No such portfolio"},
        409: {"description": "No strategy attached, or another run traded it first"},
    },
)
async def run_attached_strategy(
    name: str,
    dry_run: bool = Query(
        default=True,
        description=(
            "Show what the run would write, and write nothing. The 06:00 job writes; "
            "pass false to trade now."
        ),
    ),
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    market: TimescaleMarketDataRepo = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_rate_repo),
) -> PaperRunOut:
    try:
        report = await run_strategy(name, repo, market, rates, write=not dry_run)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    except (ValueError, StaleSession) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return run_out(report)


def run_out(report) -> PaperRunOut:
    return PaperRunOut(
        name=report.name,
        strategy=report.strategy,
        written=report.written,
        newest_bar=None if report.newest_bar is None else report.newest_bar.isoformat(),
        last_session=report.last_session.isoformat(),
        sessions=[
            PaperSessionOut(
                session=plan.session.isoformat(),
                signal_day=plan.signal_day.isoformat(),
                fills=[
                    PaperFillOut(
                        ticker=t.ticker,
                        action=t.action,
                        quantity=t.quantity,
                        price=t.price,
                        costs=t.costs,
                        time=t.ts,
                        notes=t.notes,
                    )
                    for t in plan.fills
                ],
                refused=[
                    RefusedFillOut(
                        ticker=t.ticker,
                        action=t.action,
                        quantity=t.quantity,
                        price=t.price,
                        reason=reason,
                    )
                    for t, reason in plan.refused
                ],
            )
            for plan in report.sessions
        ],
        held_sessions=[d.isoformat() for d in report.held],
        missing_bars={d.isoformat(): s for d, s in report.missing.items()},
        holdings=[
            HoldingOut(ticker=h.ticker, wanted=h.wanted, held=h.held, drift=h.drift)
            for h in report.holdings
        ],
        next_signals=report.next_signals,
        ruined_on=None if report.ruined_on is None else report.ruined_on.isoformat(),
        flags=report.flags,
    )


# ---------------------------------------------------------------------------
# Against its own backtest (decision 51, research/paper-tracking-2026-10-20.md)
# ---------------------------------------------------------------------------


class TrackingPoint(BaseModel):
    time: str = Field(description="New York date of the close")
    total: float


class TrackedFillOut(BaseModel):
    session: str
    ticker: str
    action: str
    quantity: float
    price: float
    costs: float


class RestatedOut(BaseModel):
    day: str = Field(description="A signal day already traded")
    then: Dict[str, float] = Field(description="What the signals asked for when it was traded")
    now: Dict[str, float] = Field(description="What today's history gives for that day")


class TrackingOut(BaseModel):
    name: str
    strategy: str
    attached_on: str
    paper: List[TrackingPoint] = Field(description="Paper equity at each raw close")
    backtest: List[TrackingPoint] = Field(
        description="The same strategy backtested flat from the attach date, at the defaults"
    )
    gap: Optional[float] = Field(
        default=None, description="Paper less backtest at the last close both have"
    )
    backtest_slippage: float = Field(description="Adverse slippage the backtest paid")
    costs_difference: float = Field(description="Commissions the backtest paid less paper costs")
    other: Optional[float] = Field(
        default=None,
        description=(
            "The gap slippage and costs leave: sizing on raw opens, dividends as cash "
            "against reinvested, the interest method"
        ),
    )
    dividends_received: float = Field(description="Net dividends the paper portfolio took as cash")
    paper_fills: List[TrackedFillOut]
    backtest_fills: List[TrackedFillOut]
    restated: List[RestatedOut] = Field(
        default_factory=list,
        description="Traded decisions today's history no longer gives: the record has moved",
    )
    flags: List[str] = Field(default_factory=list)


def _fill_out(f) -> TrackedFillOut:
    return TrackedFillOut(
        session=f.session.isoformat(),
        ticker=f.ticker,
        action=f.action,
        quantity=f.quantity,
        price=f.price,
        costs=f.costs,
    )


@router.get(
    "/{name}/tracking",
    response_model=TrackingOut,
    summary="The paper portfolio beside a backtest of its strategy from the attach date",
    responses={
        404: {"description": "No such portfolio"},
        409: {"description": "No strategy attached"},
        422: {"description": "The risk-free rate does not cover the window"},
    },
)
async def get_tracking(
    name: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    market: TimescaleMarketDataRepo = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_rate_repo),
) -> TrackingOut:
    try:
        report = await track(name, repo, market, rates)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    except RiskFreeCoverageError as exc:
        # Before ValueError, which it subclasses.
        raise HTTPException(status_code=422, detail=risk_free_refusal(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return TrackingOut(
        name=report.name,
        strategy=report.strategy,
        attached_on=report.attached_on.isoformat(),
        paper=[TrackingPoint(time=d.isoformat(), total=v) for d, v in report.paper],
        backtest=[TrackingPoint(time=d.isoformat(), total=v) for d, v in report.backtest],
        gap=report.gap,
        backtest_slippage=report.backtest_slippage,
        costs_difference=report.costs_difference,
        other=report.other,
        dividends_received=report.dividends_received,
        paper_fills=[_fill_out(f) for f in report.paper_fills],
        backtest_fills=[_fill_out(f) for f in report.backtest_fills],
        restated=[
            RestatedOut(day=r.day.isoformat(), then=r.then, now=r.now) for r in report.restated
        ],
        flags=report.flags,
    )
