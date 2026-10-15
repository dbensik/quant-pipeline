"""
A signal computed on bar t's close fills at bar t+1's OPEN (decision 31,
research/fill-timing-2026-10-15.md), in both backtesters. Until 2026-10-15 it
filled at bar t's own close, a price only known once the market had shut.

Every frame here has an Open that differs from the Close on every bar, so a
fill at the wrong price cannot pass by coincidence. Each test was checked
failing against the same-close code.
"""

import numpy as np
import pandas as pd
import pytest

from alpha_models.base_model import BaseAlphaModel
from backtesting.backtester import Backtester
from backtesting.events import FillEvent
from backtesting.portfolio_backtester import PortfolioBacktester
from backtesting.risk_manager import PortfolioRiskManager
from execution.simulated_handler import SimulatedExecutionHandler

CAPITAL = 100_000.0


class ExactFills(SimulatedExecutionHandler):
    """Fills at exactly the price it is given, free: the price is the test."""

    def __init__(self):
        super().__init__(commission_per_trade=0.0, commission_rate=0.0)

    def execute_order(self, order, current_price):
        return FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=current_price,
            commission=0.0,
        )


class Fixed(BaseAlphaModel):
    def __init__(self, signals):
        self.signals = signals

    def generate_signals(self, price_data):
        return pd.DataFrame({"signal": self.signals}, index=price_data.index)


def frame(n=6):
    idx = pd.bdate_range("2024-01-02", periods=n, tz="UTC")
    close = pd.Series(100.0 + 10.0 * np.arange(n), index=idx)
    # Opens sit halfway between the previous close and this one.
    open_ = close - 5.0
    return pd.DataFrame(
        {"Open": open_, "High": close, "Low": open_, "Close": close, "Volume": 1e6}, index=idx
    )


SIGNALS = [1, 0, 0, -1, 0, 0]


def single(prices, signals=SIGNALS):
    bt = Backtester(initial_capital=CAPITAL, execution_handler=ExactFills())
    results = bt.run(
        price_data=prices,
        model=Fixed(signals),
        cash_accrual=np.zeros(len(prices)),
        lot_size=1.0,
    )
    return results, bt.trade_log


def test_a_signal_fills_at_the_next_bars_open():
    prices = frame()
    results, trades = single(prices)
    assert [(t.timestamp, t.direction, t.fill_price) for t in trades] == [
        (prices.index[1], "BUY", prices["Open"].iloc[1]),
        (prices.index[4], "SELL", prices["Open"].iloc[4]),
    ]
    assert results["position"].iloc[0] == 0.0


def test_the_entry_is_sized_on_that_open():
    prices = frame()
    _, trades = single(prices)
    assert trades[0].quantity == np.floor(CAPITAL / prices["Open"].iloc[1])


def test_positions_are_still_marked_at_the_close():
    prices = frame()
    results, trades = single(prices)
    held = trades[0].quantity
    assert results["holdings"].iloc[2] == held * prices["Close"].iloc[2]


def test_a_signal_on_the_last_bar_does_not_fill():
    _, trades = single(frame(), signals=[0, 0, 0, 0, 0, 1])
    assert trades == []


def test_a_frame_without_an_open_is_refused_not_filled_at_the_close():
    with pytest.raises(ValueError, match="Open"):
        single(frame().drop(columns="Open"))


def basket(prices, signals, weights):
    bt = PortfolioBacktester(
        initial_capital=CAPITAL,
        execution_handler=ExactFills(),
        risk_manager=PortfolioRiskManager(max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=1.0),
    )
    idx = next(iter(prices.values())).index
    return bt.run(
        prices,
        {t: pd.DataFrame({"signal": s}, index=idx) for t, s in signals.items()},
        weights,
        cash_accrual=pd.Series(np.zeros(len(idx)), index=idx),
        lot_sizes={t: 1.0 for t in prices},
    )


def test_the_portfolio_backtester_fills_at_the_next_open_too():
    prices = frame()
    _, trades = basket({"A": prices}, {"A": SIGNALS}, {"A": 1.0})
    assert list(
        zip(trades["timestamp"], trades["direction"], trades["fill_price"], strict=True)
    ) == [
        (prices.index[1], "BUY", prices["Open"].iloc[1]),
        (prices.index[4], "SELL", prices["Open"].iloc[4]),
    ]
    assert trades["quantity"].iloc[0] == np.floor(CAPITAL / prices["Open"].iloc[1])


def test_a_rebalance_is_sized_on_equity_at_the_open():
    """
    A is held from bar 1. B is bought on bar 3 at 30% of the equity — the
    equity the open shows (cash + A at bar 3's open), not the close it has not
    reached. Weights leave cash to spare, so the cash check never trims it.
    """
    a, b = frame(), frame()
    b["Open"] = b["Open"] * 2
    b["Close"] = b["Close"] * 2
    _, trades = basket(
        {"A": a, "B": b},
        {"A": [1, 0, 0, 0, 0, 0], "B": [0, 0, 1, 0, 0, 0]},
        {"A": 0.3, "B": 0.3},
    )
    a_qty = np.floor(CAPITAL * 0.3 / a["Open"].iloc[1])
    cash_after_a = CAPITAL - a_qty * a["Open"].iloc[1]
    equity_at_open = cash_after_a + a_qty * a["Open"].iloc[3]
    b_buy = trades[trades["symbol"] == "B"].iloc[0]
    assert b_buy["timestamp"] == b.index[3]
    assert b_buy["quantity"] * b["Open"].iloc[3] < cash_after_a  # funded, not trimmed
    assert b_buy["quantity"] == np.floor(equity_at_open * 0.3 / b["Open"].iloc[3])
    assert b_buy["quantity"] != np.floor(
        (cash_after_a + a_qty * a["Close"].iloc[3]) * 0.3 / b["Open"].iloc[3]
    )
