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

from api.dependencies import get_market_data_repo, get_portfolio_repo
from api.risk_free import get_rate_repo
from config import settings
from core.ledger import ShortTerms
from core.paper_history import latest_prices, load_history, new_york_today
from core.portfolio import (
    ACTIONS,
    META_SHORT_TERMS,
    MarketHistory,
    Portfolio,
    PortfolioState,
    Trade,
    derive_state,
    rebalancing_orders,
    short_terms_of,
    trade_day,
    trade_refusal,
)
from core.rates import RateRepository
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
    """Corporate actions, raw closes and ^IRX from the first trade to today."""
    tickers = {t.ticker for t in portfolio.trades} | set(extra)
    if not portfolio.trades:
        return None
    first = min(trade_day(t) for t in portfolio.trades)
    return await load_history(tickers, first, new_york_today(), market, rates)


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
        409: {"description": "A short is open: it needs terms to be charged borrow"},
    },
)
async def clear_short_terms(
    name: str,
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
) -> None:
    portfolio = await _require(name, repo)
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
