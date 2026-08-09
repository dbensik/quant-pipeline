"""
core/portfolio.py
Deriving portfolio state from a trade log.

THE TRADE LOG IS THE ONLY STORED STATE. Cash, positions, average cost and P&L
are all computed from it. `portfolios.json` stored two incompatible shapes —
`{"trades": [...]}` written by the Streamlit portfolio tab, and
`{"cash": ..., "positions": {...}}` expected by the gRPC paper-trading service
— which is why `ExecutionService.GetPortfolio` raised KeyError('cash') against
the real file. Deriving one from the other removes the possibility.

WHY THERE IS NO `direction` FIELD
    The Streamlit add-trade form recorded `action` (Buy/Sell) AND `direction`
    (Long/Short) as independent selections, and nothing in the codebase ever
    read `direction` or defined what Sell+Short meant. Both cannot be
    authoritative: a short position is simply a negative net quantity, which
    falls out of selling more than is held. So `action` alone signs the trade
    and `direction` is dropped. (Nothing was lost: both portfolios in the live
    file had zero trades.)

These functions are pure — no database, no I/O — so the accounting can be
tested directly.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Mapping, Optional

BUY = "BUY"
SELL = "SELL"
ACTIONS = (BUY, SELL)


@dataclass(frozen=True)
class Trade:
    """One executed trade. `costs` is commission and always reduces cash."""

    ticker: str
    action: str
    quantity: float
    price: float
    ts: datetime
    costs: float = 0.0
    id: Optional[str] = None
    broker: Optional[str] = None
    notes: Optional[str] = None

    @property
    def signed_quantity(self) -> float:
        return self.quantity if self.action == BUY else -self.quantity

    @property
    def cash_delta(self) -> float:
        """Effect on cash. Buying spends, selling (incl. shorting) receives."""
        return -(self.signed_quantity * self.price) - self.costs


@dataclass
class Portfolio:
    """
    A named portfolio and its trade log.

    Carries no cash or positions: those are derived by derive_state(). The
    stored/derived split is the whole point — see the module docstring.
    """

    name: str
    initial_cash: float
    trades: List[Trade] = field(default_factory=list)
    created_at: Optional[datetime] = None
    metadata: Optional[dict] = None


@dataclass
class Position:
    ticker: str
    quantity: float
    average_price: float
    realised_pnl: float = 0.0
    #: Filled in only when a price is supplied to derive_state.
    last_price: Optional[float] = None
    market_value: Optional[float] = None
    unrealised_pnl: Optional[float] = None


@dataclass
class PortfolioState:
    initial_cash: float
    cash: float
    positions: List[Position] = field(default_factory=list)
    realised_pnl: float = 0.0
    trade_count: int = 0
    #: Present only for positions that were priced.
    market_value: float = 0.0
    unrealised_pnl: float = 0.0
    #: Cash + market value of priced positions.
    total_equity: float = 0.0
    #: Symbols with an open position but no price supplied. Reported rather
    #: than silently valued at cost, which would overstate a portfolio whose
    #: prices could not be loaded while looking like a complete answer.
    unpriced: List[str] = field(default_factory=list)


def _apply(
    trade: Trade, quantity: float, average_price: float
) -> tuple[float, float, float]:
    """
    Fold one trade into a running position.

    Returns (new_quantity, new_average_price, realised_pnl_from_this_trade).

    Standard average-cost accounting: adding to a position re-averages, and
    reducing one realises P&L against the average without changing it. A trade
    that crosses through zero closes the old position (realising against it)
    and opens the remainder at the trade price.
    """
    signed = trade.signed_quantity
    new_quantity = quantity + signed

    if quantity == 0.0:
        return new_quantity, trade.price, 0.0

    same_direction = (quantity > 0) == (signed > 0)
    if same_direction:
        total_cost = abs(quantity) * average_price + abs(signed) * trade.price
        return new_quantity, total_cost / abs(new_quantity), 0.0

    # Reducing, closing, or flipping.
    closed = min(abs(signed), abs(quantity))
    # Long closed above cost is a gain; short closed below cost is a gain.
    direction = 1.0 if quantity > 0 else -1.0
    realised = closed * (trade.price - average_price) * direction

    if abs(signed) < abs(quantity):
        return new_quantity, average_price, realised  # reduced, cost basis holds
    if abs(signed) == abs(quantity):
        return 0.0, 0.0, realised  # flat
    return new_quantity, trade.price, realised  # flipped, re-based


def derive_state(
    trades: List[Trade],
    initial_cash: float,
    prices: Optional[Mapping[str, float]] = None,
) -> PortfolioState:
    """
    Fold a trade log into cash, positions and P&L.

    Args:
        trades: Executed trades. Sorted by timestamp here rather than trusting
                the caller — average cost is order-dependent, so an unsorted
                log silently produces a different cost basis.
        prices: Latest price per ticker for market value and unrealised P&L.
                Omit for cost-basis-only state.
    """
    ordered = sorted(trades, key=lambda t: (t.ts, t.id or ""))

    cash = float(initial_cash)
    realised_total = 0.0
    running: Dict[str, Position] = {}

    for trade in ordered:
        cash += trade.cash_delta
        position = running.get(trade.ticker)
        quantity = position.quantity if position else 0.0
        average = position.average_price if position else 0.0

        new_quantity, new_average, realised = _apply(trade, quantity, average)
        realised_total += realised

        if position is None:
            position = Position(
                ticker=trade.ticker, quantity=new_quantity, average_price=new_average
            )
            running[trade.ticker] = position
        else:
            position.quantity = new_quantity
            position.average_price = new_average
        position.realised_pnl += realised

    # A closed position is dropped from the holdings list, but its realised
    # P&L has already been folded into realised_total above.
    open_positions = [p for p in running.values() if p.quantity != 0.0]
    open_positions.sort(key=lambda p: p.ticker)

    market_value = 0.0
    unrealised_total = 0.0
    unpriced: List[str] = []

    for position in open_positions:
        price = (prices or {}).get(position.ticker)
        if price is None:
            unpriced.append(position.ticker)
            continue
        position.last_price = float(price)
        position.market_value = position.quantity * float(price)
        position.unrealised_pnl = (
            float(price) - position.average_price
        ) * position.quantity
        market_value += position.market_value
        unrealised_total += position.unrealised_pnl

    return PortfolioState(
        initial_cash=float(initial_cash),
        cash=cash,
        positions=open_positions,
        realised_pnl=realised_total,
        trade_count=len(ordered),
        market_value=market_value,
        unrealised_pnl=unrealised_total,
        total_equity=cash + market_value,
        unpriced=unpriced,
    )


def rebalancing_orders(
    state: PortfolioState,
    target_weights: Mapping[str, float],
    prices: Mapping[str, float],
    minimum_order_value: float = 10.0,
) -> List[dict]:
    """
    Orders that move the portfolio toward `target_weights`.

    A PREVIEW — it returns orders, it does not execute them. Equity is cash
    plus the market value of priced positions, so weights are of total
    equity rather than of the invested portion.

    Tickers without a price are skipped and reported by the caller: sizing an
    order needs a price, and guessing one would produce a wrong quantity that
    looks authoritative.
    """
    held = {p.ticker: p for p in state.positions}
    equity = state.cash + sum(
        p.quantity * prices[p.ticker] for p in state.positions if p.ticker in prices
    )
    if equity <= 0:
        return []

    orders: List[dict] = []
    for ticker in sorted(set(target_weights) | set(held)):
        price = prices.get(ticker)
        if not price or price <= 0:
            continue

        current_quantity = held[ticker].quantity if ticker in held else 0.0
        current_value = current_quantity * price
        target_value = equity * target_weights.get(ticker, 0.0)
        difference = target_value - current_value

        if abs(difference) <= minimum_order_value:
            continue

        quantity = int(abs(difference) / price)
        if quantity <= 0:
            continue

        orders.append(
            {
                "ticker": ticker,
                "action": BUY if difference > 0 else SELL,
                "quantity": quantity,
                "price": float(price),
                "value": quantity * float(price),
                "current_weight": current_value / equity,
                "target_weight": target_weights.get(ticker, 0.0),
            }
        )
    return orders
