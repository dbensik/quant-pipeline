"""
core/paper_runner.py

Runs a paper portfolio's attached strategy over the sessions it has not
traded yet (decision 50, research/paper-trader-2026-10-20.md): loads the
bars, the portfolio and its MarketHistory, plans each session with
core.paper_trader, and writes each session's fills together with its
`last_session`, in one transaction. Shared by
`POST /portfolios/{name}/run` and scripts/run_paper_strategies.py (the 06:00
job).

Signals are read on bars as backtests read them (`adjust="total"`); fills
at the RAW open (`adjust="none"`), which is what the paper portfolio values
at and what the session actually printed.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timezone
from typing import Dict, List, Optional

import pandas as pd

from alpha_models import registry
from config import settings
from core.paper_history import load_history, new_york_today
from core.paper_trader import (
    META_STRATEGY,
    Holding,
    SessionPlan,
    StrategyBinding,
    binding_of,
    cut_frames,
    pending_sessions,
    plan_session,
    wanted_vs_held,
)
from core.portfolio import cash_from, derive_state, short_terms_of, trade_day
from core.strategy_signals import build_signals

#: Portfolio metadata key for a strategy the trader detached (ruin).
META_DETACHED = "strategy_detached"


class StaleSession(RuntimeError):
    """Another run traded this portfolio first: nothing was written."""


@dataclass
class RunReport:
    name: str
    strategy: str
    newest_bar: Optional[date]
    last_session: date
    sessions: List[SessionPlan] = field(default_factory=list)
    held: List[date] = field(default_factory=list)
    missing: Dict[date, List[str]] = field(default_factory=dict)
    holdings: List[Holding] = field(default_factory=list)
    #: The newest bar's signal (event) or target per symbol, where it asks
    #: for something: acted on at the next open.
    next_signals: Dict[str, float] = field(default_factory=dict)
    ruined_on: Optional[date] = None
    written: bool = False

    @property
    def flags(self) -> List[str]:
        out = []
        for plan in self.sessions:
            for trade, reason in plan.refused:
                out.append(
                    f"REFUSED {self.name} {plan.session}: {trade.action} {trade.quantity:g} "
                    f"{trade.ticker}: {reason}"
                )
        for h in self.holdings:
            if h.drift:
                out.append(f"DRIFT {self.name}: {h.ticker} wanted {h.wanted}, held {h.held:g}")
        if len(self.held) >= settings.PAPER_TRADER_HELD_SESSIONS_FLAGGED:
            first = self.held[0]
            out.append(
                f"HELD {self.name}: {len(self.held)} session(s) from {first}, no bar for "
                f"{', '.join(self.missing.get(first, []))}"
            )
        if self.ruined_on is not None:
            out.append(f"RUINED {self.name} at the close of {self.ruined_on}: strategy detached")
        return out


def _frame(records) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [r.ohlcv.open for r in records],
            "High": [r.ohlcv.high for r in records],
            "Low": [r.ohlcv.low for r in records],
            "Close": [r.ohlcv.close for r in records],
            "Volume": [r.ohlcv.volume for r in records],
        },
        index=pd.DatetimeIndex([r.ohlcv.timestamp.utc for r in records], name="Timestamp"),
        dtype=float,
    ).sort_index()


def _on(frame: pd.DataFrame, day: date, column: str) -> float:
    return float(frame.loc[frame.index.date == day, column].iloc[0])


def _next_business_day(day: date) -> date:
    return (pd.Timestamp(day) + pd.offsets.BDay(1)).date()


async def load_bars(binding: StrategyBinding, market, through: Optional[date] = None):
    """Adjusted and raw OHLCV frames per symbol, from `signal_from`."""
    start = datetime.combine(binding.signal_from, time(), tzinfo=timezone.utc)
    end = (
        datetime.combine(through, time(23, 59), tzinfo=timezone.utc)
        if through is not None
        else datetime.now(timezone.utc)
    )
    adjusted, raw = {}, {}
    for symbol in binding.symbols:
        common = dict(symbol=symbol, asset_class=None, start=start, end=end)
        adjusted[symbol] = _frame(await market.fetch_range(**common))
        raw[symbol] = _frame(await market.fetch_range(**common, adjust="none"))
    return adjusted, raw


async def run_strategy(
    name: str,
    portfolios,
    market,
    rates,
    *,
    write: bool,
    through: Optional[date] = None,
) -> RunReport:
    """
    Trade `name`'s attached strategy over every ready session (through
    `through`, default the newest bar), oldest first. With `write` False
    nothing is stored and the report shows what would be.

    Raises LookupError for no such portfolio, ValueError for one with no
    strategy, StaleSession when another run wrote first.
    """
    portfolio = await portfolios.get_portfolio(name)
    if portfolio is None:
        raise LookupError(f"No portfolio named {name!r}.")
    binding = binding_of(portfolio.metadata)
    if binding is None:
        raise ValueError(f"Portfolio {name!r} has no strategy attached.")
    spec = registry.get(binding.strategy)
    terms = short_terms_of(portfolio.metadata)

    adjusted, raw = await load_bars(binding, market, through)
    bar_days = {s: list(f.index.date) for s, f in raw.items()}
    newest = max((d[-1] for d in bar_days.values() if d), default=None)
    sessions = pending_sessions(binding, bar_days)

    classes, lots = {}, {}
    for symbol in binding.symbols:
        asset = await market.find_asset(symbol)
        classes[symbol] = None if asset is None else asset.asset_class
        lots[symbol] = settings.lot_size_for(classes[symbol])

    trades = list(portfolio.trades)
    tickers = set(binding.symbols) | {t.ticker for t in trades}
    start = cash_from(portfolio.metadata) or binding.attached_on
    first_day = min([start] + [trade_day(t) for t in trades])
    history = await load_history(
        tickers,
        first_day,
        max(newest or first_day, new_york_today()),
        market,
        rates,
        cash_from=start,
    )

    report = RunReport(
        name=name,
        strategy=binding.strategy,
        newest_bar=newest,
        last_session=binding.last_session,
        held=sessions.held,
        missing=sessions.missing,
    )
    metadata = dict(portfolio.metadata or {})
    current = binding
    for signal_day, session in sessions.ready:
        signals = build_signals(
            spec, spec.build(binding.params), cut_frames(adjusted, signal_day, session)
        )
        plan = plan_session(
            current,
            spec.signal_shape,
            signals,
            signal_day,
            session,
            trades=trades,
            initial_cash=portfolio.initial_cash,
            opens={s: _on(raw[s], session, "Open") for s in binding.symbols},
            signal_closes={s: _on(raw[s], signal_day, "Close") for s in binding.symbols},
            history=history,
            terms=terms,
            lot_sizes=lots,
            asset_classes=classes,
            shortable=settings.SHORTABLE_ASSET_CLASSES,
        )
        report.sessions.append(plan)
        if plan.ruined:
            report.ruined_on = signal_day
            # A new dict each write: the ORM sees a replaced value, never one
            # mutated in place after it was handed over.
            metadata = {k: v for k, v in metadata.items() if k != META_STRATEGY}
            metadata[META_DETACHED] = {
                **current.to_metadata(),
                "detached_on": session.isoformat(),
                "reason": f"equity at or below 0 with shorts held at the close of {signal_day}",
            }
            if write:
                await _record(portfolios, name, [], metadata, current.last_session)
            break
        previous = current.last_session
        current = replace(current, last_session=session)
        metadata = {**metadata, META_STRATEGY: current.to_metadata()}
        trades += plan.fills
        if write:
            await _record(portfolios, name, plan.fills, metadata, previous)
    report.last_session = current.last_session
    report.written = write

    # What the strategy asks for, beside what is held, and its newest signal.
    if newest is not None and any(len(f) for f in adjusted.values()):
        signals = build_signals(
            spec,
            spec.build(binding.params),
            cut_frames(adjusted, newest, _next_business_day(newest)),
        )
        timeline = sorted(set().union(*bar_days.values()))
        before = [d for d in timeline if d <= binding.attached_on]
        since = before[-1] if before else binding.attached_on
        traded_through = max(
            (d for d in timeline if d < current.last_session), default=binding.attached_on
        )
        held = {
            p.ticker: p.quantity
            for p in derive_state(trades, portfolio.initial_cash, history=history).positions
        }
        report.holdings = wanted_vs_held(
            current, spec.signal_shape, signals, since=since, held=held, through=traded_through
        )
        column = "target" if spec.signal_shape == "target" else "signal"
        for key, frame in signals.items():
            hits = frame.index[frame.index.date == newest]
            if not len(hits) or column not in frame:
                continue
            value = frame.loc[hits[0], column]
            if pd.notna(value) and (column == "target" or value != 0):
                report.next_signals[key] = float(value)
    return report


async def _record(portfolios, name, fills, metadata, expected_last_session) -> None:
    ok = await portfolios.record_session(
        name, fills, metadata, expected_last_session=expected_last_session.isoformat()
    )
    if not ok:
        raise StaleSession(
            f"{name!r} was traded past {expected_last_session} by another run; nothing written."
        )


def first_bar_day(frames: Dict[str, pd.DataFrame]) -> Optional[date]:
    """The first day every symbol has a bar: where signal frames start."""
    firsts = [f.index[0].date() for f in frames.values() if len(f)]
    return max(firsts) if len(firsts) == len(frames) else None
