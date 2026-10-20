"""
core/paper_trader.py

A strategy attached to a paper portfolio, traded forward one session at a
time under the rules its backtest used (decision 50,
research/paper-trader-2026-10-20.md).

A SESSION is a bar of the basket's calendar. For session s, the signal is
read on the bar before it (the signal day) from frames CUT at that bar, so no
strategy can see s, and acted on at s's RAW open:

  - events (1 enters at weight x equity, -1 sells the holding, 2 rebalances
    to the weights, 0 or NaN holds) by PortfolioBacktester.run's rules;
  - targets (a signed fraction of equity; NaN holds) by
    PortfolioBacktester._run_targets' rules, with its margin cut at the open
    after a close below maintenance.

Sized on the paper portfolio's own state at that open (core.portfolio
.derive_state: its cash, with dividends, interest and borrow), filled at the
raw open with no slippage draw, costed at the backtests' $1 a fill plus
`transaction_cost` of its value. Every fill must also pass
core.portfolio.trade_refusal; a refused one is reported, never written.

The functions here are pure: the caller (core/paper_runner.py) loads the
bars, the portfolio and its history, and writes what comes back.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from backtesting.events import OrderEvent
from backtesting.risk_manager import PortfolioRiskManager
from core import ledger
from core.ledger import ShortTerms
from core.portfolio import (
    BUY,
    MARKET_TZ,
    SELL,
    MarketHistory,
    Trade,
    derive_state,
    trade_refusal,
)
from execution.simulated_handler import SimulatedExecutionHandler, to_lot

#: Portfolio metadata key holding the attached strategy.
META_STRATEGY = "strategy"
#: `broker` on every trade the trader writes.
TRADER_BROKER = "paper-trader"
#: A fill's New York time: the open. Later fills of a session are a second
#: apart, so the log keeps the order they were made in (sells first).
OPEN_TIME = time(9, 30)
#: The backtests' fixed fee per fill (SimulatedExecutionHandler's default).
COMMISSION_PER_FILL = 1.0


@dataclass(frozen=True)
class StrategyBinding:
    """
    What is attached, stored in `metadata["strategy"]`.

    weights       per symbol, resolved at attach (the registry's
                  `held_weight`); empty for a target strategy, which sizes
                  its own legs
    signal_from   the first bar every signal frame starts at, fixed at
                  attach, so a refit schedule counted from the frame's start
                  does not move from day to day
    last_session  the newest session traded (at attach: the attach date, so
                  the first session is the next bar)
    decisions     per signal day traded, what the signals asked for that
                  day (decision_on), as the trader saw them: what the
                  RESTATED check compares today's history with (decision 51)
    """

    strategy: str
    params: Dict[str, object]
    symbols: Tuple[str, ...]
    weights: Dict[str, float]
    transaction_cost: float
    attached_on: date
    signal_from: date
    last_session: date
    decisions: Dict[str, Dict[str, float]] = field(default_factory=dict)

    def to_metadata(self) -> dict:
        return {
            "strategy": self.strategy,
            "params": dict(self.params),
            "symbols": list(self.symbols),
            "weights": dict(self.weights),
            "transaction_cost": self.transaction_cost,
            "attached_on": self.attached_on.isoformat(),
            "signal_from": self.signal_from.isoformat(),
            "last_session": self.last_session.isoformat(),
            "decisions": {d: dict(v) for d, v in self.decisions.items()},
        }

    @classmethod
    def from_metadata(cls, raw: Mapping) -> "StrategyBinding":
        return cls(
            strategy=raw["strategy"],
            params=dict(raw.get("params") or {}),
            symbols=tuple(raw["symbols"]),
            weights={k: float(v) for k, v in (raw.get("weights") or {}).items()},
            transaction_cost=float(raw["transaction_cost"]),
            attached_on=date.fromisoformat(raw["attached_on"]),
            signal_from=date.fromisoformat(raw["signal_from"]),
            last_session=date.fromisoformat(raw["last_session"]),
            decisions={
                d: {k: float(x) for k, x in v.items()}
                for d, v in (raw.get("decisions") or {}).items()
            },
        )


def binding_of(metadata: Optional[Mapping]) -> Optional[StrategyBinding]:
    """The portfolio's attached strategy, or None for a manual portfolio."""
    raw = (metadata or {}).get(META_STRATEGY)
    return None if not raw else StrategyBinding.from_metadata(raw)


@dataclass
class SessionPlan:
    """What one session does: the fills to write, and what was not done."""

    session: date
    signal_day: date
    fills: List[Trade] = field(default_factory=list)
    #: Fills trade_refusal would not record: (the trade, the reason).
    refused: List[Tuple[Trade, str]] = field(default_factory=list)
    #: Equity was at or below 0 with shorts held at the signal day's close.
    ruined: bool = False


@dataclass(frozen=True)
class Sessions:
    """The sessions after `last_session`: those ready to trade, and those held."""

    ready: List[Tuple[date, date]]  # (signal day, session), oldest first
    held: List[date]  # sessions after the first one a symbol has no bar for
    missing: Dict[date, List[str]]  # held session -> symbols without its bar


def pending_sessions(binding: StrategyBinding, bar_days: Mapping[str, Sequence[date]]) -> Sessions:
    """
    Sessions not yet traded, from each symbol's bar dates.

    The calendar is the union of the basket's bars, as in
    PortfolioBacktester. A session every symbol has a bar for is ready; the
    first one some symbol lacks holds it and every later one (they are
    traded in order), until the bar arrives.
    """
    days = {s: set(bar_days.get(s, ())) for s in binding.symbols}
    timeline = sorted(set().union(*days.values())) if days else []
    ready: List[Tuple[date, date]] = []
    held: List[date] = []
    missing: Dict[date, List[str]] = {}
    for i, day in enumerate(timeline):
        if day <= binding.last_session or i == 0:
            continue
        lacking = sorted(s for s in binding.symbols if day not in days[s])
        if held or lacking:
            held.append(day)
            if lacking:
                missing[day] = lacking
            continue
        ready.append((timeline[i - 1], day))
    return Sessions(ready, held, missing)


def cut_frames(
    frames: Mapping[str, pd.DataFrame], signal_day: date, session: Optional[date] = None
) -> Dict[str, pd.DataFrame]:
    """
    What the strategy may see when deciding on `signal_day`: each frame
    through that bar, plus, with `session`, ONE placeholder row dated at the
    session it trades, carrying the signal day's own values.

    The placeholder gives the strategy the session's DATE, which is the
    exchange calendar's, known at the signal day's close, and none of its
    prices. Without it a calendar rule cannot tell that the signal day closes
    a month: alpha_models.rebalancing.rebalance_dates drops the final period
    as possibly unfinished, so a monthly rebalance on the 31st is only
    emitted once a bar dated in the next month exists. The backtest, run on
    the whole history, always has that bar.
    """
    end = pd.Timestamp(signal_day + timedelta(days=1), tz="UTC")
    out = {}
    for symbol, frame in frames.items():
        seen = frame[frame.index < end]
        if session is not None and len(seen):
            label = _label_on(frame, session)
            row = seen.iloc[[-1]].copy()
            row.index = pd.DatetimeIndex([label], name=frame.index.name)
            seen = pd.concat([seen, row])
        out[symbol] = seen
    return out


def _label_on(frame: pd.DataFrame, day: date) -> pd.Timestamp:
    """The frame's index label for `day`, or that day at midnight UTC."""
    hits = frame.index[frame.index.date == day]
    return hits[0] if len(hits) else pd.Timestamp(day, tz="UTC")


def fill_time(session: date, k: int) -> datetime:
    """The k-th fill of a session: the open, then a second apart."""
    return datetime.combine(session, OPEN_TIME, tzinfo=MARKET_TZ) + timedelta(seconds=k)


def _value_at(frame: pd.DataFrame, day: date, column: str):
    """The column's value on the frame's bar dated `day` (UTC date), or NaN."""
    if frame is None or frame.empty:
        return np.nan
    hits = frame.index[frame.index.date == day]
    return frame.loc[hits[0], column] if len(hits) else np.nan


def plan_session(
    binding: StrategyBinding,
    signal_shape: str,
    signals: Mapping[str, pd.DataFrame],
    signal_day: date,
    session: date,
    *,
    trades: List[Trade],
    initial_cash: float,
    opens: Mapping[str, float],
    signal_closes: Mapping[str, float],
    history: MarketHistory,
    terms: Optional[ShortTerms],
    lot_sizes: Mapping[str, float],
    asset_classes: Mapping[str, Optional[str]],
    shortable: frozenset,
) -> SessionPlan:
    """
    The fills session `session` makes on the signals of `signal_day`.

    `opens` are the session's RAW opens, `signal_closes` the signal day's RAW
    closes, for every basket symbol. `history` must reach `session`.
    """
    plan = SessionPlan(session=session, signal_day=signal_day)
    handler = SimulatedExecutionHandler(
        slippage_pct=0.0,
        commission_per_trade=COMMISSION_PER_FILL,
        commission_rate=binding.transaction_cost,
    )
    # Off, as the Allocation page's defaults (decision 32). The off setting
    # still caps a single opening at 100% of equity, as in the backtests.
    risk = PortfolioRiskManager(max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=1.0)

    at_open = derive_state(
        trades, initial_cash, opens, history=replace(history, as_of=session), terms=terms
    )
    cash = at_open.cash
    pos = {s: 0.0 for s in binding.symbols}
    for p in at_open.positions:
        pos[p.ticker] = p.quantity

    written: List[Trade] = []

    def record(symbol: str, direction: str, fill) -> bool:
        """Check one fill with trade_refusal; True if it is written."""
        nonlocal cash
        trade = Trade(
            ticker=symbol,
            action=direction,
            quantity=float(fill.quantity),
            price=float(fill.fill_price),
            ts=fill_time(session, len(written) + len(plan.refused)),
            costs=float(fill.commission),
            broker=TRADER_BROKER,
            notes=f"{binding.strategy}: {reason_of[symbol]} on {signal_day.isoformat()}",
        )
        refusal = trade_refusal(
            trades + written,
            trade,
            initial_cash,
            opens,
            history=replace(history, as_of=session),
            terms=terms,
            asset_class=asset_classes.get(symbol),
            shortable=shortable,
        )
        if refusal:
            plan.refused.append((trade, refusal))
            return False
        written.append(trade)
        cash += trade.cash_delta
        pos[symbol] += trade.signed_quantity
        return True

    reason_of: Dict[str, str] = {}

    if signal_shape == "target":
        _target_session(
            binding,
            signals,
            signal_day,
            session,
            plan,
            trades=trades,
            initial_cash=initial_cash,
            signal_closes=signal_closes,
            history=history,
            terms=terms,
            opens=opens,
            lot_sizes=lot_sizes,
            handler=handler,
            risk=risk,
            pos=pos,
            cash_now=lambda: cash,
            record=record,
            reason_of=reason_of,
        )
    else:
        _event_session(
            binding,
            signals,
            signal_day,
            session,
            opens=opens,
            lot_sizes=lot_sizes,
            handler=handler,
            risk=risk,
            pos=pos,
            cash_now=lambda: cash,
            record=record,
            reason_of=reason_of,
        )
    plan.fills = written
    return plan


def _event_session(
    binding,
    signals,
    signal_day,
    session,
    *,
    opens,
    lot_sizes,
    handler,
    risk,
    pos,
    cash_now,
    record,
    reason_of,
) -> None:
    """PortfolioBacktester.run's event rules, for one bar."""
    ts = pd.Timestamp(datetime.combine(session, OPEN_TIME, tzinfo=MARKET_TZ))
    equity_open = cash_now() + sum(pos[s] * opens[s] for s in binding.symbols)
    open_state = pd.Series({"total": equity_open})

    portfolio_signal = _value_at(signals.get("Portfolio"), signal_day, "signal")
    portfolio_signal = 0 if pd.isna(portfolio_signal) else portfolio_signal

    orders = []
    for symbol in binding.symbols:
        ticker_signal = _value_at(signals.get(symbol), signal_day, "signal")
        if signals.get(symbol) is None:
            ticker_signal = 0
        signal = portfolio_signal if portfolio_signal != 0 else ticker_signal
        if signal == 0:
            continue
        current = pos[symbol]
        price = opens[symbol]
        lot = lot_sizes[symbol]
        weight = binding.weights.get(symbol, 0)
        order = None
        if signal == 1 and np.isclose(current, 0):
            quantity = to_lot(equity_open * weight / price, lot)
            if quantity > 0:
                order = OrderEvent(ts, symbol, "MKT", quantity, BUY)
        elif signal == -1 and current > 0:
            quantity = int(current) if lot == 1.0 else current
            order = OrderEvent(ts, symbol, "MKT", quantity, SELL)
        elif signal == 2:
            change = to_lot(equity_open * weight / price, lot) - current
            if change > 0:
                order = OrderEvent(ts, symbol, "MKT", to_lot(change, lot), BUY)
            elif change < 0:
                order = OrderEvent(ts, symbol, "MKT", to_lot(abs(change), lot), SELL)
        if order:
            reason_of[symbol] = f"signal {signal:g}"
            orders.append((symbol, order))

    # Sells fund buys, as in the engine; stable within each side.
    for symbol, order in sorted(orders, key=lambda item: item[1].direction != SELL):
        open_state[f"{symbol}_price"] = opens[symbol]
        approved = risk.assess_order(order, open_state, lot_size=lot_sizes[symbol])
        if not approved:
            continue
        fill = handler.execute_order(approved, opens[symbol])
        if fill.direction == BUY and fill.total_cost > cash_now():
            affordable = handler.affordable_quantity(cash_now(), fill.fill_price, lot_sizes[symbol])
            if affordable <= 0:
                continue
            fill = handler.resize(fill, affordable)
        record(symbol, fill.direction, fill)


def _target_session(
    binding,
    signals,
    signal_day,
    session,
    plan,
    *,
    trades,
    initial_cash,
    signal_closes,
    history,
    terms,
    opens,
    lot_sizes,
    handler,
    risk,
    pos,
    cash_now,
    record,
    reason_of,
) -> None:
    """PortfolioBacktester._run_targets' rules, for one bar."""
    ts = pd.Timestamp(datetime.combine(session, OPEN_TIME, tzinfo=MARKET_TZ))
    symbols = binding.symbols

    def execute(symbol, quantity, direction):
        fill = handler.execute_order(
            OrderEvent(ts, symbol, "MKT", quantity, direction), opens[symbol]
        )
        record(symbol, direction, fill)

    # The previous close: ruin, and a margin cut at this open.
    at_close = derive_state(
        trades, initial_cash, signal_closes, history=replace(history, as_of=signal_day), terms=terms
    )
    if at_close.short_value > 0 and at_close.total_equity <= 0:
        plan.ruined = True
        return
    cut = (
        0.0
        if terms is None
        else ledger.margin_cut(at_close.total_equity, at_close.gross_exposure, terms.maintenance)
    )
    if cut > 0:
        for symbol in symbols:
            if pos[symbol] == 0:
                continue
            lot = lot_sizes[symbol]
            quantity = min(abs(pos[symbol]), math.ceil(abs(pos[symbol]) * cut / lot) * lot)
            reason_of[symbol] = "margin"
            execute(symbol, quantity, SELL if pos[symbol] > 0 else BUY)

    def marks():
        return {s: opens[s] for s in symbols if pos[s] != 0}

    equity_open = ledger.equity(cash_now(), pos, marks())
    wanted = {}
    for symbol in symbols:
        target = _value_at(signals.get(symbol), signal_day, "target")
        if pd.isna(target):
            continue
        size = to_lot(abs(target) * max(equity_open, 0.0) / opens[symbol], lot_sizes[symbol])
        wanted[symbol] = math.copysign(size, target) if size else 0.0
        reason_of[symbol] = f"target {target:+.4g}"

    # Closing fills first: they free cash and gross for the openings.
    for symbol, desired in wanted.items():
        current = pos[symbol]
        if current > 0 and desired < current:
            execute(symbol, current - max(desired, 0.0), SELL)
        elif current < 0 and desired > current:
            execute(symbol, -current - max(-desired, 0.0), BUY)

    for symbol, desired in wanted.items():
        current = pos[symbol]
        lot = lot_sizes[symbol]
        if desired > max(current, 0.0):
            quantity, direction = desired - max(current, 0.0), BUY
        elif desired < min(current, 0.0):
            quantity, direction = -desired + min(current, 0.0), SELL
        else:
            continue
        price = opens[symbol]
        equity_now = ledger.equity(cash_now(), pos, marks())
        if terms is not None:
            room = ledger.short_room(
                equity_now, ledger.gross_exposure(pos, marks()), terms.max_gross, price, lot
            )
            quantity = min(quantity, room)
        probe = OrderEvent(ts, symbol, "MKT", quantity, BUY)
        state = pd.Series({"total": equity_now, f"{symbol}_price": price})
        approved = risk.assess_order(probe, state, lot_size=lot) if quantity > 0 else None
        if not approved:
            continue
        fill = handler.execute_order(
            OrderEvent(ts, symbol, "MKT", approved.quantity, direction), price
        )
        if direction == BUY:
            spendable = ledger.free_cash(cash_now(), pos, marks())
            if fill.total_cost > spendable:
                affordable = handler.affordable_quantity(spendable, fill.fill_price, lot)
                if affordable <= 0:
                    continue
                fill = handler.resize(fill, affordable)
        record(symbol, direction, fill)


@dataclass(frozen=True)
class Holding:
    """What the strategy last asked for, beside what the portfolio holds."""

    ticker: str
    wanted: Optional[str]  # long | short | flat; None before its first signal since attach
    held: float
    drift: bool


def wanted_vs_held(
    binding: StrategyBinding,
    signal_shape: str,
    signals: Mapping[str, pd.DataFrame],
    since: date,
    held: Mapping[str, float],
    through: Optional[date] = None,
) -> List[Holding]:
    """
    Per ticker, the side the strategy's signals since `since` (the first
    signal day traded) through `through` ask for, against the holding. A
    signal from before attach sets nothing: an event strategy already "in"
    when attached waits for its next entry, as a backtest started that day
    would. A mismatch is DRIFT: a fill refused or sized to nothing.
    """

    def window(frame: Optional[pd.DataFrame], column: str) -> pd.Series:
        if frame is None or frame.empty or column not in frame:
            return pd.Series(dtype=float)
        days = frame.index.date
        keep = days >= since
        if through is not None:
            keep &= days <= through
        return frame.loc[keep, column]

    out = []
    portfolio = window(signals.get("Portfolio"), "signal")
    for symbol in binding.symbols:
        side = None
        if signal_shape == "target":
            values = window(signals.get(symbol), "target").dropna()
            if len(values):
                last = values.iloc[-1]
                side = "long" if last > 0 else "short" if last < 0 else "flat"
        else:
            own = window(signals.get(symbol), "signal")
            events = own.reindex(own.index.union(portfolio.index))
            if len(portfolio):
                p = portfolio.reindex(events.index)
                events = p.where(p.notna() & (p != 0), events)
            for value in events.dropna():
                if value == 1:
                    side = "long"
                elif value == -1:
                    side = "flat"
                elif value == 2:
                    side = "long" if binding.weights.get(symbol, 0) > 0 else "flat"
        quantity = float(held.get(symbol, 0.0))
        drift = (
            (side == "long" and quantity <= 0)
            or (side == "flat" and quantity != 0)
            or (side == "short" and quantity >= 0)
        )
        out.append(Holding(symbol, side, quantity, drift))
    return out


def decision_on(
    signal_shape: str, signals: Mapping[str, pd.DataFrame], day: date
) -> Dict[str, float]:
    """
    What the signals ask for on `day`: each frame's event (non-zero) or
    target (non-NaN), by key ("Portfolio" for a basket-wide signal). Empty
    when the day asks for nothing.
    """
    column = "target" if signal_shape == "target" else "signal"
    out = {}
    for key, frame in signals.items():
        if frame is None or column not in frame:
            continue
        value = _value_at(frame, day, column)
        if pd.notna(value) and (column == "target" or value != 0):
            out[key] = float(value)
    return out


@dataclass(frozen=True)
class Restatement:
    """A traded signal day whose decision today's history no longer gives."""

    day: date
    then: Dict[str, float]
    now: Dict[str, float]


def restatements(
    binding: StrategyBinding,
    signal_shape: str,
    signals_today: Mapping[str, pd.DataFrame],
    traded_signal_days: Sequence[date],
) -> List[Restatement]:
    """
    The traded signal days whose decision, re-derived from today's bars,
    differs from the one recorded when the session was traded. A record that
    passes still follows the strategy it names; one that fails traded on a
    history that has since moved (a restated bar, a re-adjusting action).
    """
    out = []
    for day in traded_signal_days:
        then = binding.decisions.get(day.isoformat(), {})
        now = decision_on(signal_shape, signals_today, day)
        same = then.keys() == now.keys() and all(
            math.isclose(then[k], now[k], rel_tol=1e-9, abs_tol=1e-12) for k in then
        )
        if not same:
            out.append(Restatement(day, then, now))
    return out
