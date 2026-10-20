"""
The paper trader (decision 50, research/paper-trader-2026-10-20.md): a
strategy attached to a paper portfolio, traded one session at a time.

THE HEADLINE IS PARITY. Walked forward session by session, with frames cut
at each signal day, the trader must write exactly the fills
PortfolioBacktester makes on the same bars in one run: dates, sides,
quantities, prices and costs. Fixtures have no corporate actions and no
rates, the engine's slippage is 0, so raw and adjusted prices agree and the
paper portfolio's cash is the engine's. If they differ, the trader is wrong,
not the test. Each test was checked failing against the code with the rule
it covers removed.
"""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from alpha_models import registry
from backtesting.portfolio_backtester import PortfolioBacktester
from backtesting.risk_manager import PortfolioRiskManager
from core.ledger import ShortTerms
from core.paper_trader import (
    StrategyBinding,
    binding_of,
    cut_frames,
    fill_time,
    pending_sessions,
    plan_session,
    wanted_vs_held,
)
from core.portfolio import MarketHistory, Trade, derive_state, trade_day
from core.strategy_signals import build_signals
from execution.simulated_handler import SimulatedExecutionHandler

CAPITAL = 100_000.0
COST = 0.001
TERMS = ShortTerms(borrow_rate=0.01, max_gross=2.0, maintenance=0.3)
SHORTABLE = frozenset({"equity", "etf"})


def ohlcv(close, rng):
    """Opens a little off the previous close, so open and close differ."""
    close = np.asarray(close, dtype=float)
    opens = np.concatenate([[close[0]], close[:-1]]) * (1 + rng.normal(0, 0.003, len(close)))
    return opens, close


def basket(n_bars=520, symbols=("SPY", "EFA", "IEF"), seed=5):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2022-01-03", periods=n_bars, tz="UTC")
    frames = {}
    for k, symbol in enumerate(symbols):
        drift = (0.0006, -0.0003, 0.0002, 0.0004)[k % 4]
        close = 100 * np.exp(np.cumsum(rng.normal(drift, 0.012, n_bars)))
        opens, close = ohlcv(close, rng)
        frames[symbol] = pd.DataFrame(
            {"Open": opens, "High": close, "Low": close, "Close": close, "Volume": 1e6},
            index=idx,
        )
    return frames


def cointegrated_pair(n_bars=420, seed=8):
    """A = 10 + 2B + an AR(1) spread: crosses +-2 sigma several times."""
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2022-01-03", periods=n_bars, tz="UTC")
    b = 50 + np.cumsum(rng.normal(0, 0.5, n_bars))
    spread = np.zeros(n_bars)
    for i in range(1, n_bars):
        spread[i] = 0.9 * spread[i - 1] + rng.normal(0, 1.0)
    a = 10 + 2 * b + spread
    frames = {}
    for symbol, close in (("XLE", a), ("XOP", b)):
        opens, close = ohlcv(close, rng)
        frames[symbol] = pd.DataFrame(
            {"Open": opens, "High": close, "Low": close, "Close": close, "Volume": 1e6},
            index=idx,
        )
    return frames


def bind(strategy, frames, params=None, terms=None):
    spec = registry.get(strategy)
    symbols = tuple(frames)
    first = next(iter(frames.values())).index[0].date()
    weights = (
        {} if spec.signal_shape == "target" else spec.default_weights(list(symbols), params or {})
    )
    return spec, StrategyBinding(
        strategy=strategy,
        params=dict(params or {}),
        symbols=symbols,
        weights=weights,
        transaction_cost=COST,
        attached_on=first,
        signal_from=first,
        last_session=first,
    )


def history_for(frames, as_of):
    closes = {s: [(t.date(), float(c)) for t, c in f["Close"].items()] for s, f in frames.items()}
    return MarketHistory(actions={}, closes=closes, rates=[], as_of=as_of)


def bar(frames, symbol, day, column):
    frame = frames[symbol]
    return float(frame.loc[frame.index.date == day, column].iloc[0])


def walk(spec, binding, frames, terms=None, until=None):
    """The trader, one session at a time from the attach date: its fills."""
    days = {s: list(f.index.date) for s, f in frames.items()}
    sessions = pending_sessions(binding, days)
    history = history_for(frames, sessions.ready[-1][1])
    trades = []
    for signal_day, session in sessions.ready:
        if until is not None and session > until:
            break
        signals = build_signals(
            spec, spec.build(binding.params), cut_frames(frames, signal_day, session)
        )
        plan = plan_session(
            binding,
            spec.signal_shape,
            signals,
            signal_day,
            session,
            trades=trades,
            initial_cash=CAPITAL,
            opens={s: bar(frames, s, session, "Open") for s in binding.symbols},
            signal_closes={s: bar(frames, s, signal_day, "Close") for s in binding.symbols},
            history=history,
            terms=terms,
            lot_sizes={s: 1.0 for s in binding.symbols},
            asset_classes={s: "etf" for s in binding.symbols},
            shortable=SHORTABLE,
        )
        assert not plan.refused, plan.refused
        trades += plan.fills
    return trades


def engine_fills(spec, binding, frames, terms=None):
    bt = PortfolioBacktester(
        initial_capital=CAPITAL,
        execution_handler=SimulatedExecutionHandler(slippage_pct=0.0, commission_rate=COST),
        risk_manager=PortfolioRiskManager(1.0, 1.0),
    )
    index = next(iter(frames.values())).index
    _, log = bt.run(
        frames,
        build_signals(spec, spec.build(binding.params), frames),
        binding.weights,
        cash_accrual=pd.Series(0.0, index=index),
        lot_sizes={s: 1.0 for s in frames},
        short_terms=terms,
    )
    return log


def as_rows(trades):
    return [
        (trade_day(t), t.ticker, t.action, t.quantity, round(t.price, 9), round(t.costs, 9))
        for t in trades
    ]


def engine_rows(log):
    return [
        (
            pd.Timestamp(r.timestamp).date(),
            r.symbol,
            r.direction,
            float(r.quantity),
            round(r.fill_price, 9),
            round(r.commission, 9),
        )
        for r in log.itertuples()
    ]


# ---------------------------------------------------------------------------
# Parity with PortfolioBacktester
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "strategy, params",
    [
        ("asset_class_trend", {"window": 60}),
        ("momentum_allocation", {"lookback": 40, "top_n": 2}),
        ("paired_switching", {"lookback": 40}),
        ("basket_trading", {}),
    ],
)
def test_walked_forward_it_makes_the_backtests_fills(strategy, params):
    frames = basket()
    if strategy == "paired_switching":
        frames = {s: frames[s] for s in ("SPY", "IEF")}
    spec, binding = bind(strategy, frames, params)
    engine = engine_rows(engine_fills(spec, binding, frames))
    assert len(engine) >= 6, "the fixture must trade"
    assert as_rows(walk(spec, binding, frames)) == engine


def test_a_single_asset_strategy_on_one_symbol_makes_the_backtests_fills():
    frames = {"SPY": basket()["SPY"]}
    spec, binding = bind("ma_crossover", frames, {"short_window": 10, "long_window": 40})
    engine = engine_rows(engine_fills(spec, binding, frames))
    assert len(engine) >= 6
    assert as_rows(walk(spec, binding, frames)) == engine


def test_pairs_walked_forward_make_the_backtests_long_and_short_fills():
    frames = cointegrated_pair()
    params = {"formation": 100, "refit": 21}
    spec, binding = bind("pairs_trading", frames, params)
    engine = engine_rows(engine_fills(spec, binding, frames, TERMS))
    assert sum(1 for r in engine if r[2] == "SELL") >= 4, "the fixture must short"
    assert as_rows(walk(spec, binding, frames, TERMS)) == engine


def test_the_paper_state_after_the_walk_is_the_engines():
    frames = basket()
    spec, binding = bind("asset_class_trend", frames, {"window": 60})
    trades = walk(spec, binding, frames)
    last = next(iter(frames.values())).index[-1].date()
    closes = {s: bar(frames, s, last, "Close") for s in frames}
    state = derive_state(trades, CAPITAL, closes, history=history_for(frames, last))
    bt = PortfolioBacktester(
        initial_capital=CAPITAL,
        execution_handler=SimulatedExecutionHandler(slippage_pct=0.0, commission_rate=COST),
        risk_manager=PortfolioRiskManager(1.0, 1.0),
    )
    index = next(iter(frames.values())).index
    portfolio, _ = bt.run(
        frames,
        build_signals(spec, spec.build(binding.params), frames),
        binding.weights,
        cash_accrual=pd.Series(0.0, index=index),
        lot_sizes={s: 1.0 for s in frames},
    )
    assert state.total_equity == pytest.approx(portfolio["total"].iloc[-1], rel=1e-12)


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


def test_the_first_session_is_the_bar_after_attach_and_traded_ones_are_skipped():
    frames = basket(n_bars=10)
    days = {s: list(f.index.date) for s, f in frames.items()}
    _, binding = bind("asset_class_trend", frames)
    ready = pending_sessions(binding, days).ready
    assert ready[0] == (days["SPY"][0], days["SPY"][1])
    later = StrategyBinding(**{**vars(binding), "last_session": days["SPY"][6]})
    assert [s for _, s in pending_sessions(later, days).ready] == days["SPY"][7:]
    assert (
        pending_sessions(
            StrategyBinding(**{**vars(binding), "last_session": days["SPY"][-1]}), days
        ).ready
        == []
    )


def test_a_missing_bar_holds_that_session_and_every_later_one():
    frames = basket(n_bars=10)
    days = {s: list(f.index.date) for s, f in frames.items()}
    gap = days["EFA"].pop(5)
    _, binding = bind("asset_class_trend", frames)
    sessions = pending_sessions(binding, days)
    assert [s for _, s in sessions.ready] == days["SPY"][1:5]
    assert sessions.held == days["SPY"][5:]
    assert sessions.missing == {gap: ["EFA"]}


def test_a_week_caught_up_is_the_week_run_day_by_day():
    frames = basket()
    spec, binding = bind("asset_class_trend", frames, {"window": 60})
    all_at_once = walk(spec, binding, frames)
    # Day by day: re-derive the pending sessions after each one.
    days = {s: list(f.index.date) for s, f in frames.items()}
    trades, current = [], binding
    history = history_for(frames, days["SPY"][-1])
    while True:
        ready = pending_sessions(current, days).ready
        if not ready:
            break
        signal_day, session = ready[0]
        signals = build_signals(
            spec, spec.build(current.params), cut_frames(frames, signal_day, session)
        )
        plan = plan_session(
            current,
            spec.signal_shape,
            signals,
            signal_day,
            session,
            trades=trades,
            initial_cash=CAPITAL,
            opens={s: bar(frames, s, session, "Open") for s in current.symbols},
            signal_closes={s: bar(frames, s, signal_day, "Close") for s in current.symbols},
            history=history,
            terms=None,
            lot_sizes={s: 1.0 for s in current.symbols},
            asset_classes={s: "etf" for s in current.symbols},
            shortable=SHORTABLE,
        )
        trades += plan.fills
        current = StrategyBinding(**{**vars(current), "last_session": session})
    assert as_rows(trades) == as_rows(all_at_once)


# ---------------------------------------------------------------------------
# What a session may see and write
# ---------------------------------------------------------------------------


def test_no_strategy_sees_the_session_it_trades():
    # Only its date: the placeholder row carries the signal day's values.
    frames = basket(n_bars=30)
    day, session = frames["SPY"].index[9].date(), frames["SPY"].index[10].date()
    cut = cut_frames(frames, day, session)
    for symbol, f in cut.items():
        assert [d for d in f.index.date][-2:] == [day, session]
        assert f.iloc[-1].tolist() == frames[symbol].iloc[9].tolist()
        assert f.iloc[:-1].equals(frames[symbol].iloc[:10])


def test_a_monthly_rule_rebalances_on_the_months_last_session():
    # 2022-01-31 is a Monday; the January rebalance must be decided on it.
    frames = basket(n_bars=120)
    spec, binding = bind("asset_class_trend", frames, {"window": 10})
    trades = walk(spec, binding, frames)
    assert trades and trade_day(trades[0]) == date(2022, 2, 1)


def test_fills_are_at_the_raw_open_with_the_backtests_costs_and_tagged():
    frames = basket()
    spec, binding = bind("asset_class_trend", frames, {"window": 60})
    trades = walk(spec, binding, frames)
    first = trades[0]
    assert first.price == bar(frames, first.ticker, trade_day(first), "Open")
    assert first.costs == pytest.approx(1.0 + COST * first.quantity * first.price)
    assert first.broker == "paper-trader"
    assert first.notes.startswith("asset_class_trend: signal 1 on ")


def test_a_fill_the_portfolio_cannot_take_is_refused_not_written():
    # A short on a portfolio without terms: trade_refusal says no.
    frames = cointegrated_pair()
    spec, binding = bind("pairs_trading", frames, {"formation": 100, "refit": 21})
    with pytest.raises(AssertionError, match="no short terms"):
        walk(spec, binding, frames, terms=None)


def test_a_close_below_maintenance_cuts_at_the_next_open():
    frames = cointegrated_pair(n_bars=30)
    spec, binding = bind("pairs_trading", frames, {"formation": 100, "refit": 21})
    days = list(frames["XLE"].index.date)
    # Short 1,500 XLE at the first open, then mark it far higher at the close.
    xle_open = bar(frames, "XLE", days[1], "Open")
    shares = int(150_000 / xle_open)
    trades = [Trade("XLE", "SELL", shares, xle_open, fill_time(days[1], 0))]
    closes = {"XLE": xle_open * 1.3, "XOP": bar(frames, "XOP", days[1], "Close")}
    plan = plan_session(
        binding,
        "target",
        {s: pd.DataFrame({"target": np.nan}, index=frames[s].index) for s in frames},
        days[1],
        days[2],
        trades=trades,
        initial_cash=CAPITAL,
        opens={s: bar(frames, s, days[2], "Open") for s in frames},
        signal_closes=closes,
        history=history_for(frames, days[2]),
        terms=TERMS,
        lot_sizes={"XLE": 1.0, "XOP": 1.0},
        asset_classes={"XLE": "etf", "XOP": "etf"},
        shortable=SHORTABLE,
    )
    equity = CAPITAL + shares * xle_open - shares * closes["XLE"]
    cut = 1 - equity / (TERMS.maintenance * shares * closes["XLE"])
    (cover,) = plan.fills
    assert (cover.ticker, cover.action) == ("XLE", "BUY")
    assert cover.quantity == np.ceil(shares * cut)
    assert cover.notes.startswith("pairs_trading: margin")


def test_ruin_at_the_previous_close_trades_nothing():
    frames = cointegrated_pair(n_bars=30)
    spec, binding = bind("pairs_trading", frames, {"formation": 100, "refit": 21})
    days = list(frames["XLE"].index.date)
    trades = [Trade("XLE", "SELL", 3000, 50.0, fill_time(days[1], 0))]
    plan = plan_session(
        binding,
        "target",
        {s: pd.DataFrame({"target": 0.0}, index=frames[s].index) for s in frames},
        days[1],
        days[2],
        trades=trades,
        initial_cash=CAPITAL,
        opens={s: bar(frames, s, days[2], "Open") for s in frames},
        signal_closes={"XLE": 90.0, "XOP": 50.0},
        history=history_for(frames, days[2]),
        terms=TERMS,
        lot_sizes={"XLE": 1.0, "XOP": 1.0},
        asset_classes={"XLE": "etf", "XOP": "etf"},
        shortable=SHORTABLE,
    )
    assert plan.ruined and plan.fills == []


# ---------------------------------------------------------------------------
# Wanted vs held
# ---------------------------------------------------------------------------


def test_wanted_vs_held_follows_signals_since_attach_and_flags_drift():
    idx = pd.bdate_range("2024-01-02", periods=6, tz="UTC")
    days = list(idx.date)
    signals = {
        "A": pd.DataFrame({"signal": [1, 0, -1, 0, 1, 0]}, index=idx, dtype=float),
        "B": pd.DataFrame({"signal": [1, 0, 0, 0, 0, 0]}, index=idx, dtype=float),
    }
    binding = StrategyBinding(
        "x", {}, ("A", "B"), {"A": 0.5, "B": 0.5}, COST, days[2], days[0], days[2]
    )
    rows = wanted_vs_held(binding, "wide_per_asset", signals, since=days[2], held={"A": 0.0})
    assert [(r.ticker, r.wanted, r.drift) for r in rows] == [
        ("A", "long", True),
        ("B", None, False),
    ]
    rows = wanted_vs_held(
        binding, "wide_per_asset", signals, since=days[2], held={"A": 10.0}, through=days[3]
    )
    assert [(r.ticker, r.wanted, r.drift) for r in rows] == [
        ("A", "flat", True),
        ("B", None, False),
    ]


def test_wanted_vs_held_for_targets_reads_the_last_target_sign():
    idx = pd.bdate_range("2024-01-02", periods=4, tz="UTC")
    days = list(idx.date)
    signals = {
        "A": pd.DataFrame({"target": [np.nan, -0.5, np.nan, np.nan]}, index=idx),
        "B": pd.DataFrame({"target": [np.nan, 0.5, np.nan, 0.0]}, index=idx),
    }
    binding = StrategyBinding("pairs_trading", {}, ("A", "B"), {}, COST, days[0], days[0], days[0])
    rows = wanted_vs_held(binding, "target", signals, since=days[0], held={"A": -10.0, "B": 5.0})
    assert [(r.ticker, r.wanted, r.drift) for r in rows] == [
        ("A", "short", False),
        ("B", "flat", True),
    ]


def test_the_binding_round_trips_through_metadata():
    _, binding = bind("asset_class_trend", basket(n_bars=5), {"window": 60})
    assert binding_of({"strategy": binding.to_metadata()}) == binding
    assert binding_of({}) is None
