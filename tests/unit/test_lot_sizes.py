"""
Backtests trade in lots: whole shares for equities and ETFs, 1e-8 for crypto.

Until 2026-10-12 every backtest traded whole units, so once one coin cost more
than the cash a crypto backtest bought nothing and reported a flat line (the 3
BTC-USD buy bars still unfilled in the 15-run check of 2026-10-12). Checked
failing against that code.
"""

import numpy as np
import pandas as pd
import pytest

from alpha_models import registry
from backtesting.backtester import Backtester
from backtesting.portfolio_backtester import PortfolioBacktester
from backtesting.risk_manager import PortfolioRiskManager
from config.settings import lot_size_for
from execution.simulated_handler import SimulatedExecutionHandler, to_lot

SATOSHI = 1e-8


def frame(close):
    idx = pd.bdate_range("2024-01-02", periods=len(close), tz="UTC")
    c = pd.Series(close, index=idx, dtype=float)
    return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1e6})


def test_lot_sizes_by_asset_class():
    assert lot_size_for("equity") == lot_size_for("etf") == 1.0
    assert lot_size_for("crypto") == SATOSHI
    with pytest.raises(ValueError, match="option"):
        lot_size_for("option")


def test_to_lot_whole_units_are_ints_as_before():
    assert to_lot(12.9, 1.0) == 12
    assert isinstance(to_lot(12.9, 1.0), int)


def test_to_lot_keeps_an_exact_multiple_whole():
    """
    32,659,001 satoshi is stored as 0.32659001, which divides by 1e-8 to
    32659000.999...; a plain floor dropped a satoshi from every such
    holding. 11,741 of the first 200,000 multiples do this.
    """
    held = 32_659_001 * SATOSHI
    assert to_lot(held, SATOSHI) == pytest.approx(held, abs=SATOSHI / 10)
    assert to_lot(15 * SATOSHI, SATOSHI) == pytest.approx(15 * SATOSHI, abs=SATOSHI / 10)
    assert to_lot(0.123456789, SATOSHI) == pytest.approx(0.12345678, abs=SATOSHI / 10)


def test_affordable_quantity_in_satoshi():
    handler = SimulatedExecutionHandler(commission_rate=0.001)
    cash, price = 20_000.0, 61_234.56
    q = handler.affordable_quantity(cash, price, SATOSHI)

    def cost(n):
        return n * price + handler.commission_for(n, price)

    assert 0.3 < q < 1.0
    assert cost(q) <= cash < cost(q + SATOSHI)


def test_a_coin_dearer_than_the_cash_is_still_bought():
    prices = frame(np.linspace(60_000.0, 90_000.0, 60))
    finals = {}
    for lot in (1.0, SATOSHI):
        bt = Backtester(initial_capital=20_000.0, seed=1)
        results = bt.run(
            price_data=prices,
            model=registry.build("buy_and_hold", {}),
            cash_accrual=np.zeros(len(prices)),
            lot_size=lot,
        )
        assert (results["cash"] >= 0).all()
        finals[lot] = (len(bt.get_trade_log()), results["total"].iloc[-1])
    assert finals[1.0] == (0, 20_000.0)
    trades, final = finals[SATOSHI]
    assert trades == 1
    assert final > 29_000.0  # ~20,000 x 1.5, less costs


def test_a_basket_sells_its_whole_fractional_holding():
    """A full sale of 0.3 BTC leaves exactly nothing, not one satoshi of dust."""
    a = frame([100.0] * 4)
    b = frame([61_234.56] * 4)
    bt = PortfolioBacktester(
        initial_capital=40_000.0,
        execution_handler=SimulatedExecutionHandler(seed=1),
        risk_manager=PortfolioRiskManager(max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=0.99),
    )
    idx = a.index
    portfolio, trades = bt.run(
        {"A": a, "B": b},
        {"Portfolio": pd.DataFrame({"signal": [1, 0, -1, 0]}, index=idx)},
        {"A": 0.5, "B": 0.5},
        cash_accrual=pd.Series(np.zeros(len(idx)), index=idx),
        lot_sizes={"A": 1.0, "B": SATOSHI},
    )
    bought = trades[(trades["symbol"] == "B") & (trades["direction"] == "BUY")]
    assert 0.3 < bought["quantity"].iloc[0] < 0.33
    assert portfolio["B_pos"].iloc[-1] == 0.0
    assert portfolio["A_pos"].iloc[1] == int(portfolio["A_pos"].iloc[1]) > 0


def test_a_basket_refuses_a_ticker_without_a_lot_size():
    a = frame([100.0] * 3)
    idx = a.index
    with pytest.raises(ValueError, match="B"):
        PortfolioBacktester().run(
            {"A": a, "B": a},
            {"Portfolio": pd.DataFrame({"signal": [1, 0, 0]}, index=idx)},
            {"A": 0.5, "B": 0.5},
            cash_accrual=pd.Series(np.zeros(len(idx)), index=idx),
            lot_sizes={"A": 1.0},
        )
