"""
The two risk classes, pinned on fixed inputs (brief task 2).

PortfolioRiskManager checks orders BEFORE they trade (the portfolio
backtester and the paper trader use it). RiskManager describes a finished
return series (VaR and CVaR on the allocation response). They live together
in portfolio/risk_manager.py but are not one class. Every expected value
below was computed by the code before the move out of
backtesting/risk_manager.py, so a change of behaviour fails here.
"""

from datetime import datetime

import pandas as pd
import pytest

from backtesting.events import OrderEvent
from portfolio.risk_manager import PortfolioRiskManager, RiskManager

T = datetime(2026, 1, 2)

RETURNS = pd.Series(
    [
        0.012,
        -0.034,
        0.005,
        -0.008,
        0.021,
        -0.017,
        0.003,
        -0.052,
        0.009,
        0.0,
        -0.011,
        0.014,
        -0.026,
        0.007,
        0.018,
        -0.003,
        -0.041,
        0.011,
        0.002,
        -0.006,
    ]
)


def order(quantity, direction="BUY", symbol="AAPL"):
    return OrderEvent(T, symbol, "MKT", quantity, direction)


def state(total, **prices):
    return pd.Series({"total": total, **{f"{s}_price": p for s, p in prices.items()}})


@pytest.mark.parametrize(
    "confidence, var, cvar",
    [
        (0.95, 0.04154999999999999, 0.052),
        (0.90, 0.03470000000000001, 0.0465),
        (0.80, 0.01880000000000001, 0.03825),
    ],
)
def test_var_and_cvar_are_unchanged(confidence, var, cvar):
    risk = RiskManager(RETURNS)
    assert risk.calculate_value_at_risk(confidence) == var
    assert risk.calculate_conditional_value_at_risk(confidence) == cvar


def test_cvar_includes_the_days_exactly_at_var():
    # The 20% quantile is exactly -1%, which three days share.
    risk = RiskManager(pd.Series([-0.03, -0.01, -0.01, -0.01, 0.0, 0.01, 0.02, 0.02, 0.03, 0.04]))
    assert risk.calculate_value_at_risk(0.8) == 0.01
    assert risk.calculate_conditional_value_at_risk(0.8) == 0.015000000000000001


def test_all_risk_metrics_are_the_95_figures():
    assert RiskManager(RETURNS).get_all_risk_metrics() == {
        "Value at Risk (95%)": 0.04154999999999999,
        "Conditional VaR (95%)": 0.052,
    }


def test_empty_returns_are_refused():
    with pytest.raises(ValueError):
        RiskManager(pd.Series([], dtype=float))


@pytest.mark.parametrize(
    "proposed, equity_state, lot, approved",
    [
        (order(10), state(100_000, AAPL=150.0), 1.0, 10),
        # 2% of $100,000 is $2,000: 13 whole shares at $150.
        (order(100), state(100_000, AAPL=150.0), 1.0, 13),
        (order(1.0, symbol="BTC"), state(20_000, BTC=30_000.0), 1e-8, 0.013333330000000001),
        # Whole coins: the 2% cap rounds to nothing, and nothing trades.
        (order(1.0, symbol="BTC"), state(20_000, BTC=30_000.0), 1.0, None),
    ],
)
def test_assess_order_is_unchanged(proposed, equity_state, lot, approved):
    result = PortfolioRiskManager().assess_order(proposed, equity_state, lot_size=lot)
    assert (None if result is None else result.quantity) == approved


def test_the_drawdown_halt_stops_buys_only_and_lifts_at_a_new_high():
    risk = PortfolioRiskManager()
    path = [
        (100_000, order(10), 10),
        (85_000, order(10), 10),
        (79_000, order(10), None),
        (79_000, order(10, "SELL"), 10),
        (101_000, order(10), 10),
    ]
    for total, proposed, approved in path:
        result = risk.assess_order(proposed, state(total, AAPL=150.0), lot_size=1.0)
        assert (None if result is None else result.quantity) == approved
    assert risk.high_water_mark == 101_000


@pytest.mark.parametrize(
    "args, lot, quantity",
    [
        ((100_000, 0.30, 0.15, 150.0), 1.0, 333),
        # Capped at 2x equity, however calm the asset.
        ((100_000, 0.05, 0.15, 150.0), 1.0, 1333),
        ((50_000, 0.60, 0.15, 30_000.0), 1e-8, 0.41666666),
        ((100_000, 0.0, 0.15, 10.0), 1.0, 0),
    ],
)
def test_volatility_sizing_is_unchanged(args, lot, quantity):
    assert (
        PortfolioRiskManager().calculate_volatility_adjusted_size(*args, lot_size=lot) == quantity
    )
