"""
The portfolio optimizer scores a weighting exactly as a backtest of it would.

Until 2026-10-12 it reported mean daily return x 252 as the return and
subtracted the window's mean rate from that for Sharpe, while every backtest
reports the CAGR and a Sharpe against a cash account. The same basket showed
two Sharpes: SPY/AGG/GLD 2023-2025 was 1.82 on the Optimize page and 1.94 in
a backtest. Checked failing against that code.
"""

import numpy as np
import pandas as pd
import pytest

from analysis.performance_analyzer import PerformanceAnalyzer
from portfolio.portfolio_optimizer import PortfolioOptimizer


@pytest.fixture
def prices():
    rng = np.random.default_rng(7)
    idx = pd.bdate_range("2023-01-02", periods=400, tz="UTC")
    steps = rng.normal([0.0006, 0.0002, 0.0004], [0.012, 0.004, 0.009], (400, 3))
    return pd.DataFrame(100 * np.cumprod(1 + steps, axis=0), index=idx, columns=["A", "B", "C"])


@pytest.fixture
def accrual(prices):
    # 4% a year over calendar days, 0 on bar 0, as api/risk_free.py builds it.
    days = np.r_[0, np.diff(prices.index.asi8) / 86_400e9]
    return 0.04 * days / 365


def backtest_metrics(prices, accrual, weights):
    """PerformanceAnalyzer on the daily-rebalanced, cost-free curve."""
    returns = prices.pct_change().fillna(0.0).to_numpy() @ np.array(
        [weights[c] for c in prices.columns]
    )
    history = pd.DataFrame(
        {"total": 100_000 * np.cumprod(1 + returns), "returns": returns}, index=prices.index
    )
    return PerformanceAnalyzer(
        history, initial_capital=100_000.0, risk_free_accrual=accrual
    ).calculate_all_metrics()


def test_every_weighting_scores_as_its_backtest(prices, accrual):
    results, _best, _least = PortfolioOptimizer(prices, accrual, seed=3).simulate_random_portfolios(
        600
    )
    for i in (0, 1, 299, 599):
        row = results.loc[i]
        expected = backtest_metrics(prices, accrual, row["weights"])
        assert row["Sharpe Ratio"] == pytest.approx(expected["Sharpe Ratio"], abs=1e-12)
        assert row["Annualized Return"] == pytest.approx(expected["Annualized Return"], abs=1e-12)
        assert row["Annualized Volatility"] == pytest.approx(
            expected["Annualized Volatility"], abs=1e-12
        )


def test_the_draws_are_the_old_per_row_draws(prices, accrual):
    """Vectorized sampling kept the seeded weights: same stream, same order."""
    results, _, _ = PortfolioOptimizer(prices, accrual, seed=11).simulate_random_portfolios(50)
    rng = np.random.default_rng(11)
    for i in range(50):
        w = rng.random(3)
        w /= w.sum()
        assert list(results.loc[i, "weights"].values()) == pytest.approx(list(w), abs=1e-15)


def test_an_accrual_of_the_wrong_length_is_refused(prices, accrual):
    with pytest.raises(ValueError, match="risk_free_accrual"):
        PortfolioOptimizer(prices, accrual[1:])
