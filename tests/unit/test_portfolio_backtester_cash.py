"""
PortfolioBacktester never spends more cash than it has, and a rebalance's
sales fund its purchases.

Until 2026-10-12 there was no cash check: orders are sized on equity x weight
at the close, so slippage and commissions overdrew real AAPL+TQQQ baskets by
up to $70 for 60 bars. Orders also executed in ticker order, so a buy could
precede the sale paying for it (27-75 bars a run went overdrawn mid-bar).
Checked failing: the first two against that code; the last two against a
cash check WITHOUT sells-first, which starves the rebalance's purchase.
"""

import numpy as np
import pandas as pd

from backtesting.events import FillEvent
from backtesting.portfolio_backtester import PortfolioBacktester
from backtesting.risk_manager import PortfolioRiskManager
from execution.simulated_handler import SimulatedExecutionHandler

CAPITAL = 100_000.0


class AdverseFills(SimulatedExecutionHandler):
    """Every fill 0.1% above the close, $1 commission."""

    def execute_order(self, order, current_price):
        return FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=current_price * (1.001 if order.direction == "BUY" else 0.999),
            commission=1.0,
        )


def run(a_close, b_close, signals):
    idx = pd.bdate_range("2024-01-02", periods=len(a_close), tz="UTC")

    def frame(close):
        c = pd.Series(close, index=idx, dtype=float)
        return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1e6})

    bt = PortfolioBacktester(
        initial_capital=CAPITAL,
        execution_handler=AdverseFills(),
        risk_manager=PortfolioRiskManager(max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=0.99),
    )
    portfolio, trades = bt.run(
        {"A": frame(a_close), "B": frame(b_close)},
        {"Portfolio": pd.DataFrame({"signal": signals}, index=idx)},
        {"A": 0.5, "B": 0.5},
        cash_accrual=pd.Series(np.zeros(len(idx)), index=idx),
    )
    return portfolio, trades


def test_an_entry_never_overdraws_the_account():
    """
    Half the equity each at 0.1% adverse slippage plus $1 a fill costs more
    than the cash; the second buy is cut to what remains.
    """
    portfolio, trades = run([100.0] * 3, [100.0] * 3, [1, 0, 0])
    assert len(trades) == 2
    assert (portfolio["cash"] >= 0).all()
    assert trades["quantity"].iloc[1] < 500


def test_a_rebalance_sells_before_it_buys():
    """
    A falls and B rises, so the rebalance buys A and sells B. In ticker order
    the purchase of A came first and overdrew the account until B's sale.
    """
    portfolio, trades = run(
        [100.0, 100.0, 60.0, 60.0],
        [100.0, 100.0, 140.0, 140.0],
        [1, 0, 2, 0],
    )
    rebalance = trades[trades["timestamp"] == portfolio.index[2]]
    assert list(rebalance["direction"]) == ["SELL", "BUY"]
    assert list(rebalance["symbol"]) == ["B", "A"]
    assert (portfolio["cash"] >= 0).all()


def test_a_funded_rebalance_buys_the_full_target():
    """
    Sales first means the purchase is not cut short: A ends close to half the
    equity, not starved by a cash check that ran before B's proceeds arrived.
    """
    portfolio, trades = run(
        [100.0, 100.0, 60.0, 60.0],
        [100.0, 100.0, 140.0, 140.0],
        [1, 0, 2, 0],
    )
    bought = trades[(trades["timestamp"] == portfolio.index[2]) & (trades["symbol"] == "A")]
    a_value = (portfolio["A_pos"].iloc[2]) * 60.0
    assert len(bought) == 1
    assert a_value > 0.49 * portfolio["total"].iloc[2]
