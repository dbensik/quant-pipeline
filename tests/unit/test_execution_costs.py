"""
Fills cost something: slippage is adverse and `transaction_cost` is charged.

Until 2026-10-12 slippage was uniform in [-0.05%, +0.05%] whatever the side,
so it averaged to zero and half of all fills beat the price, and
`transaction_cost` (every endpoint's "per-trade cost, 0.001 = 10bps") was
stored and read by nothing: SPY mean_reversion 2018-2025 made 146 trades and
ended at $110,657.17 at a cost of 0 and of 1% alike. Checked failing against
that code.
"""

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from alpha_models import registry
from backtesting.backtester import Backtester
from backtesting.events import OrderEvent
from execution.simulated_handler import SimulatedExecutionHandler

WHEN = datetime(2024, 1, 2, tzinfo=timezone.utc)


def fills(direction: str, n: int = 500):
    handler = SimulatedExecutionHandler(seed=3)
    return [
        handler.execute_order(OrderEvent(WHEN, "X", "MKT", 10, direction), 100.0).fill_price
        for _ in range(n)
    ]


def test_a_buy_never_fills_below_the_price():
    prices = fills("BUY")
    assert min(prices) >= 100.0
    assert max(prices) <= 100.0 * 1.0005
    assert np.mean(prices) > 100.0


def test_a_sell_never_fills_above_the_price():
    prices = fills("SELL")
    assert max(prices) <= 100.0
    assert min(prices) >= 100.0 * (1 - 0.0005)


def test_commission_is_the_fee_plus_the_rate_on_value():
    handler = SimulatedExecutionHandler(seed=1, commission_rate=0.001)
    fill = handler.execute_order(OrderEvent(WHEN, "X", "MKT", 200, "BUY"), 50.0)
    assert fill.commission == pytest.approx(1.0 + 0.001 * 200 * fill.fill_price)
    sold = handler.execute_order(OrderEvent(WHEN, "X", "MKT", 200, "SELL"), 50.0)
    # A sale's proceeds are reduced by the same cost.
    assert sold.total_cost == pytest.approx(200 * sold.fill_price - sold.commission)


def test_affordable_quantity_is_the_most_the_cash_covers():
    handler = SimulatedExecutionHandler(commission_rate=0.001)
    cash, price = 100_000.0, 99.99
    q = handler.affordable_quantity(cash, price)
    cost = lambda n: n * price + handler.commission_for(n, price)  # noqa: E731
    assert cost(q) <= cash < cost(q + 1)


def test_a_resized_fill_recomputes_its_commission():
    handler = SimulatedExecutionHandler(seed=1, commission_rate=0.01)
    fill = handler.execute_order(OrderEvent(WHEN, "X", "MKT", 100, "BUY"), 10.0)
    smaller = handler.resize(fill, 40)
    assert smaller.fill_price == fill.fill_price
    assert smaller.commission == pytest.approx(1.0 + 0.01 * 40 * fill.fill_price)


def test_transaction_cost_reaches_the_backtest():
    idx = pd.bdate_range("2021-01-04", periods=300, tz="UTC")
    close = pd.Series(100 + np.sin(np.arange(300) / 6) * 8, index=idx)
    frame = pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99, "Close": close, "Volume": 1e6}
    )
    finals = []
    for cost in (0.0, 0.01):
        bt = Backtester(transaction_cost=cost, seed=42)
        bt.run(
            price_data=frame,
            model=registry.build("ma_crossover", {"short_window": 5, "long_window": 20}),
            cash_accrual=np.zeros(len(frame)),
        )
        assert len(bt.get_trade_log()) > 4
        finals.append(bt.get_performance_metrics()["Final Value"])
    assert finals[1] < finals[0]
