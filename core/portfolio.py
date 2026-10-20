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

DERIVED FLOWS (decision 49, research/paper-shorts-2026-10-19.md). Given a
MarketHistory, derive_state also folds in what happened to the holdings
between trades, all computed from stored events and never stored:
  - a split multiplies a held quantity on its ex-date and divides its cost;
  - a dividend pays a long and charges a short, per RAW share
    (core.price_adjustment.raw_dividends, the backtests' own conversion);
  - free cash (cash less the shorts' collateral) earns ^IRX by calendar day;
  - a short pays borrow on its value at the previous raw close, at the
    portfolio's ShortTerms (core/ledger.py).
Without a history it is the trade log alone, as before.

These functions are pure — no database, no I/O — so the accounting can be
tested directly.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from core import ledger
from core.ledger import ShortTerms
from core.price_adjustment import DIVIDEND, MANUAL, SPLIT, Action, raw_dividends

#: Trades and ex-dates are New York trading dates.
MARKET_TZ = ZoneInfo("America/New_York")

#: ^IRX is a bond-equivalent yield: simple, on a 365-day year (core/rates.py).
YEAR_DAYS = 365.0

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
    #: Priced longs and shorts (shorts as a positive value), and their sum.
    long_value: float = 0.0
    short_value: float = 0.0
    gross_exposure: float = 0.0
    #: Cash less the shorts' collateral: what a buy may spend.
    free_cash: float = 0.0
    #: Derived flows (decision 49); all 0 without a MarketHistory.
    dividends_received: float = 0.0
    dividends_paid: float = 0.0
    interest: float = 0.0
    borrow_paid: float = 0.0
    flows: List["Flow"] = field(default_factory=list)
    #: Corporate actions on a holding that are not modelled (a spinoff's
    #: manual price factor): listed, never valued.
    unhandled_actions: List["Flow"] = field(default_factory=list)
    #: With ShortTerms and every position priced: equity required by
    #: maintenance, equity above (below if negative) it, and the flag.
    maintenance_required: Optional[float] = None
    margin_excess: Optional[float] = None
    below_maintenance: bool = False


@dataclass(frozen=True)
class Flow:
    """A dated event that changed cash or a holding without a trade."""

    day: date
    kind: str  # dividend | split | manual_factor
    ticker: str
    amount: float  # cash change for a dividend; the ratio or factor otherwise
    quantity: float  # quantity held when it applied


@dataclass(frozen=True)
class MarketHistory:
    """
    What derive_state needs to fold in the flows between trades.

    actions   per ticker, corporate actions as stored (as_of filters nothing:
              an action after as_of is simply not reached)
    closes    per ticker, (date, RAW close) oldest first: the borrow base
    rates     (date, annual bond-equivalent ^IRX), oldest first
    as_of     the New York date the state is for: interest and borrow
              accrue for every day before it
    max_rate_age_days   a day whose newest rate is older earns nothing
    """

    actions: Mapping[str, Sequence[Action]]
    closes: Mapping[str, Sequence[Tuple[date, float]]]
    rates: Sequence[Tuple[date, float]]
    as_of: date
    max_rate_age_days: int = 7


def _apply(trade: Trade, quantity: float, average_price: float) -> tuple[float, float, float]:
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


def trade_day(trade: Trade) -> date:
    """The New York date a trade belongs to (a naive time is UTC, as the router stores it)."""
    ts = trade.ts if trade.ts.tzinfo else trade.ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(MARKET_TZ).date()


def _close_on_or_before(series: Sequence[Tuple[date, float]], day: date) -> Optional[float]:
    i = bisect.bisect_right([d for d, _ in series], day) - 1
    return series[i][1] if i >= 0 else None


def derive_state(
    trades: List[Trade],
    initial_cash: float,
    prices: Optional[Mapping[str, float]] = None,
    *,
    history: Optional[MarketHistory] = None,
    terms: Optional[ShortTerms] = None,
) -> PortfolioState:
    """
    Fold a trade log into cash, positions and P&L.

    Args:
        trades: Executed trades. Sorted by timestamp here rather than trusting
                the caller — average cost is order-dependent, so an unsorted
                log silently produces a different cost basis.
        prices: Latest price per ticker for market value and unrealised P&L.
                Omit for cost-basis-only state.
        history: Corporate actions, raw closes and rates: with it, splits,
                dividends, interest and borrow are folded in, day by day from
                the first trade to `history.as_of` (module docstring).
        terms:  The portfolio's short terms: borrow rate and margin. Without
                them a short pays no borrow and has no margin status.
    """
    ordered = sorted(trades, key=lambda t: (t.ts, t.id or ""))

    cash = float(initial_cash)
    realised_total = 0.0
    running: Dict[str, Position] = {}
    totals = {"dividends_received": 0.0, "dividends_paid": 0.0, "interest": 0.0, "borrow": 0.0}
    flows: List[Flow] = []
    unhandled: List[Flow] = []

    def book(trade: Trade) -> None:
        """Fold one trade into its position and realised P&L; cash is the caller's."""
        nonlocal realised_total
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

    if history is None or not ordered:
        for trade in ordered:
            cash += trade.cash_delta
            book(trade)
    else:
        cash = _fold_with_history(
            ordered, cash, running, book, history, terms, totals, flows, unhandled
        )

    # A closed position is dropped from the holdings list, but its realised
    # P&L has already been folded into realised_total above.
    open_positions = [p for p in running.values() if p.quantity != 0.0]
    open_positions.sort(key=lambda p: p.ticker)

    market_value = 0.0
    unrealised_total = 0.0
    unpriced: List[str] = []
    long_v = short_v = 0.0

    for position in open_positions:
        price = (prices or {}).get(position.ticker)
        if price is None:
            unpriced.append(position.ticker)
            continue
        position.last_price = float(price)
        position.market_value = position.quantity * float(price)
        position.unrealised_pnl = (float(price) - position.average_price) * position.quantity
        market_value += position.market_value
        unrealised_total += position.unrealised_pnl
        if position.quantity > 0:
            long_v += position.market_value
        else:
            short_v -= position.market_value

    state = PortfolioState(
        initial_cash=float(initial_cash),
        cash=cash,
        positions=open_positions,
        realised_pnl=realised_total,
        trade_count=len(ordered),
        market_value=market_value,
        unrealised_pnl=unrealised_total,
        total_equity=cash + market_value,
        unpriced=unpriced,
        long_value=long_v,
        short_value=short_v,
        gross_exposure=long_v + short_v,
        free_cash=cash - short_v,
        dividends_received=totals["dividends_received"],
        dividends_paid=totals["dividends_paid"],
        interest=totals["interest"],
        borrow_paid=totals["borrow"],
        flows=flows,
        unhandled_actions=unhandled,
    )
    if terms is not None and not unpriced:
        state.maintenance_required = terms.maintenance * state.gross_exposure
        state.margin_excess = state.total_equity - state.maintenance_required
        state.below_maintenance = state.gross_exposure > 0 and state.margin_excess < 0
    return state


def _fold_with_history(
    ordered: List[Trade],
    cash: float,
    running: Dict[str, Position],
    book,
    history: MarketHistory,
    terms: Optional[ShortTerms],
    totals: Dict[str, float],
    flows: List[Flow],
    unhandled: List[Flow],
) -> float:
    """
    Trades, corporate actions, interest and borrow, one New York day at a time.

    Each day: the day's corporate actions apply to what was held at the
    previous close (a dividend before a split on the same ex-date: raw
    dividends are per pre-split share, decision 27); then the day's trades;
    then, for days before as_of, the night's interest on free cash and borrow
    on the shorts, both by calendar day (one night = one day, so a Friday
    position pays for Saturday and Sunday on those days).
    """
    by_day: Dict[date, List[Trade]] = {}
    for trade in ordered:
        by_day.setdefault(trade_day(trade), []).append(trade)

    dividends = {t: dict(raw_dividends(a)) for t, a in history.actions.items()}
    splits = {
        t: {x.ex_date: x.value for x in a if x.kind == SPLIT} for t, a in history.actions.items()
    }
    manual = {
        t: {x.ex_date: x.value for x in a if x.kind == MANUAL} for t, a in history.actions.items()
    }
    rate_days = [d for d, _ in history.rates]

    day = min(by_day)
    last = max(history.as_of, max(by_day))
    while day <= last:
        for ticker, position in running.items():
            if position.quantity == 0:
                continue
            amount = dividends.get(ticker, {}).get(day)
            if amount is not None:
                paid = position.quantity * amount
                cash += paid
                key = "dividends_received" if paid > 0 else "dividends_paid"
                totals[key] += abs(paid)
                flows.append(Flow(day, DIVIDEND, ticker, paid, position.quantity))
            ratio = splits.get(ticker, {}).get(day)
            if ratio is not None:
                flows.append(Flow(day, SPLIT, ticker, ratio, position.quantity))
                position.quantity *= ratio
                position.average_price /= ratio
            factor = manual.get(ticker, {}).get(day)
            if factor is not None:
                unhandled.append(Flow(day, MANUAL, ticker, factor, position.quantity))

        for trade in by_day.get(day, []):
            cash += trade.cash_delta
            book(trade)

        if day < history.as_of:
            closes = {
                t: _close_on_or_before(history.closes.get(t, ()), day)
                for t, p in running.items()
                if p.quantity < 0
            }
            short_value = sum(-running[t].quantity * c for t, c in closes.items() if c is not None)
            i = bisect.bisect_right(rate_days, day) - 1
            if i >= 0 and (day - rate_days[i]).days <= history.max_rate_age_days:
                interest = (cash - short_value) * history.rates[i][1] / YEAR_DAYS
                cash += interest
                totals["interest"] += interest
            if short_value and terms is not None:
                borrow = short_value * terms.borrow_rate / ledger.BORROW_DAY_COUNT
                cash -= borrow
                totals["borrow"] += borrow
        day += timedelta(days=1)
    return cash


#: Portfolio metadata key holding its short terms (decision 49).
META_SHORT_TERMS = "short_terms"


def short_terms_of(metadata: Optional[Mapping]) -> Optional[ShortTerms]:
    """The portfolio's ShortTerms, or None when it has none (it cannot short)."""
    raw = (metadata or {}).get(META_SHORT_TERMS)
    return None if not raw else ShortTerms(**raw)


def trade_refusal(
    trades: List[Trade],
    trade: Trade,
    initial_cash: float,
    prices: Mapping[str, float],
    *,
    history: Optional[MarketHistory],
    terms: Optional[ShortTerms],
    asset_class: Optional[str],
    shortable: frozenset,
    allow_overdraft: bool = False,
) -> Optional[str]:
    """
    Why `trade` may not be recorded, or None. The one check for REST and gRPC.

    - A SELL that opens or adds to a short needs short terms and a shortable
      asset class, always: without terms there is no borrow rate to charge.
    - Unless `allow_overdraft` (recording a trade made elsewhere):
      - free cash (cash less the shorts' collateral) must not go negative
        through this trade: a buy cannot be paid with short proceeds;
      - with terms, gross exposure must stay within max_gross x equity.
    Valued at the latest prices, the traded ticker at the trade's price.
    """
    before = derive_state(trades, initial_cash, prices, history=history, terms=terms)
    held = next((p.quantity for p in before.positions if p.ticker == trade.ticker), 0.0)
    opens_short = trade.action == SELL and trade.quantity > max(held, 0.0)
    if opens_short:
        if asset_class not in shortable:
            return (
                f"{trade.ticker} is {asset_class or 'not a registered asset'}: only "
                f"{', '.join(sorted(shortable))} can be shorted."
            )
        if terms is None:
            return (
                f"Selling {trade.quantity:g} {trade.ticker} against {max(held, 0.0):g} held "
                "opens a short, and this portfolio has no short terms (borrow rate, max "
                "gross, maintenance). Set them first."
            )
    if allow_overdraft:
        return None

    marks = {**prices, trade.ticker: trade.price}
    after = derive_state(trades + [trade], initial_cash, marks, history=history, terms=terms)
    if after.unpriced:
        return f"No price for {', '.join(after.unpriced)}: the trade cannot be checked."
    marked = derive_state(trades, initial_cash, marks, history=history, terms=terms)
    if after.free_cash < 0 and after.free_cash < marked.free_cash:
        return (
            f"Insufficient cash: the trade leaves {after.free_cash:,.2f} of free cash "
            f"({marked.free_cash:,.2f} available; short proceeds are collateral, not cash). "
            "Pass allow_overdraft=true to record it anyway."
        )
    if (
        terms is not None
        and after.gross_exposure > marked.gross_exposure
        and after.gross_exposure > terms.max_gross * after.total_equity
    ):
        return (
            f"Gross exposure would be {after.gross_exposure:,.2f}, above "
            f"{terms.max_gross:g} x equity {after.total_equity:,.2f}."
        )
    return None


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
