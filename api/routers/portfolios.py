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

from core.portfolio import (
    ACTIONS,
    BUY,
    Portfolio,
    PortfolioState,
    Trade,
    derive_state,
    rebalancing_orders,
)
from db.repositories.market_data import TimescaleMarketDataRepo
from db.repositories.portfolios import TimescalePortfolioRepo

from api.dependencies import get_market_data_repo, get_portfolio_repo

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


class CreatePortfolioRequest(BaseModel):
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    initial_cash: float = Field(default=100_000.0, gt=0)
    metadata: Optional[Dict[str, Any]] = None


class RebalanceRequest(BaseModel):
    target_weights: Dict[str, float] = Field(
        description="Ticker to target weight of total equity"
    )
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
    """
    Most recent stored close per ticker.

    Prices come from the migrated database, NOT from yfinance: this endpoint
    must not acquire a network dependency, and every other router values
    positions the same way.
    """
    prices: Dict[str, float] = {}
    latest: Optional[datetime] = None

    for ticker in tickers:
        asset = await market.find_asset(ticker)
        if asset is None:
            continue
        records = await market.fetch_range(
            symbol=ticker,
            asset_class=None,
            start=datetime(1970, 1, 1, tzinfo=timezone.utc),
            end=datetime.now(timezone.utc),
        )
        if not records:
            continue
        last = max(records, key=lambda r: r.ohlcv.timestamp.utc)
        if last.ohlcv.close is None:
            continue
        prices[ticker] = float(last.ohlcv.close)
        if latest is None or last.ohlcv.timestamp.utc > latest:
            latest = last.ohlcv.timestamp.utc

    return prices, latest


def _state_response(
    portfolio: Portfolio,
    state: PortfolioState,
    priced_at: Optional[datetime],
) -> PortfolioStateOut:
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
    )


async def _require(
    name: str, repo: TimescalePortfolioRepo
) -> Portfolio:
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
    try:
        created = await repo.create_portfolio(
            name=request.name,
            initial_cash=request.initial_cash,
            metadata=request.metadata,
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
) -> PortfolioStateOut:
    portfolio = await _require(name, repo)

    prices: Dict[str, float] = {}
    priced_at: Optional[datetime] = None
    if include_prices:
        # Derive once unpriced to learn which tickers are actually open —
        # cheaper than pricing every ticker that ever appeared in the log.
        provisional = derive_state(portfolio.trades, portfolio.initial_cash)
        tickers = [p.ticker for p in provisional.positions]
        if tickers:
            prices, priced_at = await _latest_prices(tickers, market)

    state = derive_state(portfolio.trades, portfolio.initial_cash, prices)
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
        422: {"description": "Bad action, or insufficient cash"},
    },
)
async def add_trade(
    name: str,
    trade: TradeIn,
    allow_overdraft: bool = Query(
        default=False,
        description=(
            "Permit a BUY that takes cash negative. Off by default so paper "
            "trading cannot run on unlimited leverage — PortfolioManager never "
            "checked cash at all. Turn it on when RECORDING trades that "
            "already happened elsewhere, where rejecting them would be wrong."
        ),
    ),
    repo: TimescalePortfolioRepo = Depends(get_portfolio_repo),
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

    if action == BUY and not allow_overdraft:
        # Checked against state INCLUDING this trade, not against a running
        # balance, so an out-of-order backdated trade is judged on the same
        # basis as the log it joins.
        projected = derive_state(
            portfolio.trades + [domain], portfolio.initial_cash
        )
        if projected.cash < 0:
            current = derive_state(portfolio.trades, portfolio.initial_cash)
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Insufficient cash: the trade costs "
                    f"{domain.quantity * domain.price + domain.costs:,.2f} but "
                    f"only {current.cash:,.2f} is available, leaving "
                    f"{projected.cash:,.2f}. Pass allow_overdraft=true to "
                    "record it anyway."
                ),
            )

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
) -> RebalancePreview:
    """
    A PREVIEW. It returns orders; it does not record them. Post them to
    /trades to act on them — the Streamlit tool executed straight from the
    preview button, so there was no point at which the orders could be
    inspected and declined.
    """
    if any(weight < 0 for weight in request.target_weights.values()):
        raise HTTPException(
            status_code=422, detail="Target weights must not be negative."
        )
    total = sum(request.target_weights.values())
    if total > 1.0 + 1e-9:
        raise HTTPException(
            status_code=422,
            detail=f"Target weights sum to {total:.4f}; they must not exceed 1.0.",
        )

    portfolio = await _require(name, repo)
    state = derive_state(portfolio.trades, portfolio.initial_cash)

    tickers = sorted({p.ticker for p in state.positions} | set(request.target_weights))
    prices, _ = await _latest_prices(tickers, market)

    priced_state = derive_state(portfolio.trades, portfolio.initial_cash, prices)
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
