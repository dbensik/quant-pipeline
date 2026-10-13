"""
An entry whose fill costs more than the cash is resized, not dropped.

Backtester sizes a buy as floor(cash / close). Adverse slippage plus the
commission can make that fill cost more than the cash; until 2026-10-12 the
order was then dropped and retried next bar: 80 unfilled buy bars over 15
real runs (5 symbols x 3 strategies, 2016-2025). The first two tests were
checked failing against the dropping code.
"""

import numpy as np
import pandas as pd
import pytest

from alpha_models import registry
from backtesting.backtester import Backtester
from backtesting.events import FillEvent
from execution.simulated_handler import SimulatedExecutionHandler

CAPITAL = 100_000.0


class AdverseFills(SimulatedExecutionHandler):
    """Every fill 0.1% above the close, $1 commission: no all-cash order fits."""

    def __init__(self):
        super().__init__(commission_per_trade=1.0, commission_rate=0.0)
        self.orders = 0

    def execute_order(self, order, current_price):
        self.orders += 1
        return FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=current_price * 1.001,
            commission=1.0,
        )


def frame(n: int = 20) -> pd.DataFrame:
    idx = pd.bdate_range("2024-01-02", periods=n, tz="UTC")
    close = pd.Series(np.linspace(99.99, 110.0, n), index=idx)
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1e6}, index=idx
    )


def run(handler) -> tuple:
    prices = frame()
    bt = Backtester(initial_capital=CAPITAL, execution_handler=handler)
    results = bt.run(
        price_data=prices,
        model=registry.build("buy_and_hold", {}),
        cash_accrual=np.zeros(len(prices)),
        lot_size=1.0,
    )
    return bt, results


def test_an_unaffordable_entry_buys_what_the_cash_covers():
    """
    floor(100000 / 99.99) = 1000 shares, whose fill costs 100,090.99 + 1.
    Dropped before: buy and hold never bought at all, on any bar.
    """
    handler = AdverseFills()
    bt, results = run(handler)
    trades = bt.get_trade_log()
    assert len(trades) == 1
    fill_price = 99.99 * 1.001
    assert trades["quantity"].iloc[0] == np.floor((CAPITAL - 1.0) / fill_price)
    assert results["position"].iloc[0] == trades["quantity"].iloc[0]
    assert 0.0 <= results["cash"].iloc[0] < fill_price


def test_a_resized_entry_spends_one_slippage_draw():
    """
    The fill is reused, not re-executed: a second draw would move every later
    fill of the run, changing runs this fix should leave alone.
    """
    handler = AdverseFills()
    run(handler)
    assert handler.orders == 1


def test_cash_never_goes_negative():
    _bt, results = run(AdverseFills())
    assert (results["cash"] >= 0).all()
    assert results["total"].iloc[-1] == pytest.approx(
        results["cash"].iloc[-1] + results["position"].iloc[-1] * 110.0
    )
