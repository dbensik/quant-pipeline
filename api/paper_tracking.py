"""
api/paper_tracking.py

A paper portfolio against its own backtest (decision 51,
research/paper-tracking-2026-10-20.md). Shared by
`GET /portfolios/{name}/tracking` and scripts/run_paper_strategies.py.

Two daily equity curves from the attach date:
  - paper: core.portfolio.derive_state's day loop, at raw closes;
  - backtest: PortfolioBacktester on the same strategy, parameters and
    basket, its signals masked before the attach date so it starts flat that
    day, at the Allocation page's defaults (seed 42 slippage, the stored
    transaction cost, ^IRX accrual).

And the RESTATED check: each traded signal day's decision, re-derived from
today's bars, against the one the trader recorded. Signals are computed ONCE,
on today's frames, for both.

In api/ rather than core/ because it resolves the rate as every backtest
router does (api.risk_free).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from alpha_models import registry
from api.risk_free import resolve_risk_free
from config import settings
from core.paper_history import load_history, new_york_today
from core.paper_runner import load_bars, next_business_day
from core.paper_trader import Restatement, binding_of, cut_frames, restatements
from core.portfolio import Trade, cash_from, derive_state, short_terms_of, trade_day
from core.strategy_signals import build_signals
from execution.simulated_handler import SimulatedExecutionHandler

#: The Allocation page's (and the portfolio backtest's) default seed.
BACKTEST_SEED = 42
#: SimulatedExecutionHandler's default slippage bound, read from it.
BACKTEST_SLIPPAGE = SimulatedExecutionHandler().slippage_pct


@dataclass(frozen=True)
class TrackedFill:
    session: date
    ticker: str
    action: str
    quantity: float
    price: float
    costs: float


@dataclass
class TrackingReport:
    name: str
    strategy: str
    attached_on: date
    paper: List[Tuple[date, float]] = field(default_factory=list)
    backtest: List[Tuple[date, float]] = field(default_factory=list)
    paper_fills: List[TrackedFill] = field(default_factory=list)
    backtest_fills: List[TrackedFill] = field(default_factory=list)
    #: The backtest's adverse slippage: fill price against the open, x quantity.
    backtest_slippage: float = 0.0
    #: Commissions the backtest paid less the paper costs.
    costs_difference: float = 0.0
    dividends_received: float = 0.0
    restated: List[Restatement] = field(default_factory=list)

    @property
    def gap(self) -> Optional[float]:
        """Paper less backtest at the last close both have."""
        both = sorted(set(dict(self.paper)) & set(dict(self.backtest)))
        if not both:
            return None
        return dict(self.paper)[both[-1]] - dict(self.backtest)[both[-1]]

    @property
    def other(self) -> Optional[float]:
        """The gap the slippage and costs do not explain: sizing on raw
        opens, dividends as cash against reinvested, the interest method."""
        gap = self.gap
        return None if gap is None else gap - self.backtest_slippage - self.costs_difference

    @property
    def flags(self) -> List[str]:
        return [
            f"RESTATED {self.name}: the decision of {r.day} was {r.then or 'nothing'}; "
            f"today's history gives {r.now or 'nothing'}"
            for r in self.restated
        ]


def _mask_before(signals: Dict[str, pd.DataFrame], since: pd.Timestamp, last: pd.Timestamp):
    """No signal before `since` (the backtest starts flat), none after `last`."""
    out = {}
    for key, frame in signals.items():
        frame = frame[frame.index <= last].copy()
        early = frame.index < since
        for column in frame.columns:
            frame.loc[early, column] = np.nan if column == "target" else 0.0
        out[key] = frame[frame.index >= since]
    return out


async def track(
    name: str, portfolios, market, rates, *, slippage_pct: float = BACKTEST_SLIPPAGE
) -> TrackingReport:
    """
    `slippage_pct` is the backtest's slippage bound: the handler's default,
    as every backtest surface uses; tests pass 0 to compare like with like.

    Raises LookupError for no such portfolio, ValueError for one with no
    strategy, core.rates.RiskFreeCoverageError for a window ^IRX misses.
    """
    from backtesting.portfolio_backtester import PortfolioBacktester
    from backtesting.risk_manager import PortfolioRiskManager

    portfolio = await portfolios.get_portfolio(name)
    if portfolio is None:
        raise LookupError(f"No portfolio named {name!r}.")
    binding = binding_of(portfolio.metadata)
    if binding is None:
        raise ValueError(f"Portfolio {name!r} has no strategy attached.")
    spec = registry.get(binding.strategy)
    terms = short_terms_of(portfolio.metadata)
    report = TrackingReport(name=name, strategy=binding.strategy, attached_on=binding.attached_on)

    adjusted, _ = await load_bars(binding, market)
    if not all(len(f) for f in adjusted.values()):
        return report
    timeline = sorted(set().union(*(f.index for f in adjusted.values())))
    newest = timeline[-1]
    before = [t for t in timeline if t.date() <= binding.attached_on]
    since = before[-1] if before else timeline[0]
    signals = build_signals(
        spec,
        spec.build(binding.params),
        cut_frames(adjusted, newest.date(), next_business_day(newest.date())),
    )

    # RESTATED: every signal day traded so far.
    traded = [
        t.date()
        for t, after in zip(timeline, timeline[1:], strict=False)
        if since <= t and after.date() <= binding.last_session
    ]
    report.restated = restatements(binding, spec.signal_shape, signals, traded)

    # Paper: the state's own day loop, at raw closes.
    trades: List[Trade] = list(portfolio.trades)
    start = cash_from(portfolio.metadata) or binding.attached_on
    history = await load_history(
        set(binding.symbols) | {t.ticker for t in trades},
        min([start] + [trade_day(t) for t in trades]),
        new_york_today(),
        market,
        rates,
        cash_from=start,
    )
    curve: List[Tuple[date, float]] = []
    state = derive_state(trades, portfolio.initial_cash, history=history, terms=terms, curve=curve)
    report.paper = [(d, v) for d, v in curve if d >= since.date()]
    report.dividends_received = state.dividends_received - state.dividends_paid
    report.paper_fills = [
        TrackedFill(trade_day(t), t.ticker, t.action, t.quantity, t.price, t.costs)
        for t in sorted(trades, key=lambda t: t.ts)
    ]

    # Backtest: the same signals, flat until the attach date.
    prices = {s: f[f.index >= since] for s, f in adjusted.items()}
    index = pd.DatetimeIndex(sorted(set().union(*(f.index for f in prices.values()))))
    risk_free = await resolve_risk_free(None, index, rates)
    lots = {}
    for symbol in binding.symbols:
        asset = await market.find_asset(symbol)
        lots[symbol] = settings.lot_size_for(None if asset is None else asset.asset_class)
    engine = PortfolioBacktester(
        initial_capital=portfolio.initial_cash,
        execution_handler=SimulatedExecutionHandler(
            slippage_pct=slippage_pct, seed=BACKTEST_SEED, commission_rate=binding.transaction_cost
        ),
        risk_manager=PortfolioRiskManager(1.0, 1.0),
    )
    frame, log = engine.run(
        prices,
        _mask_before(signals, since, newest),
        binding.weights,
        cash_accrual=pd.Series(risk_free.accrual, index=index),
        lot_sizes=lots,
        short_terms=terms if spec.signal_shape == "target" else None,
    )
    # Equity at each close, from the engine's own cash and positions. Its
    # `total` column on the event path is computed BEFORE that bar's open
    # fills (a bar's fills first show at the next close), which would put a
    # one-bar step into the gap on every fill day.
    closes = pd.DataFrame({s: f["Close"] for s, f in prices.items()}).reindex(frame.index).ffill()
    total = frame["cash"] + sum(frame[f"{s}_pos"] * closes[s] for s in binding.symbols)
    report.backtest = [(t.date(), float(v)) for t, v in total.items()]
    slippage = commissions = 0.0
    for row in log.itertuples():
        ts = pd.Timestamp(row.timestamp)
        opened = float(prices[row.symbol].loc[ts, "Open"])
        slippage += abs(row.fill_price - opened) * row.quantity
        commissions += row.commission
        report.backtest_fills.append(
            TrackedFill(
                ts.date(),
                row.symbol,
                row.direction,
                float(row.quantity),
                float(row.fill_price),
                float(row.commission),
            )
        )
    report.backtest_slippage = slippage
    report.costs_difference = commissions - sum(t.costs for t in trades)
    return report
