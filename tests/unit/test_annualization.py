"""
Metrics are annualized on the series' own calendar (decision 38,
research/crypto-strategies-2026-10-15.md): 365 bars a year when the series
has weekend bars, 252 when it has none. Until 2026-10-15 everything used 252,
so every crypto Sharpe was sqrt(365/252) = 1.2035x too high.

The 365-day assertions fail against the old code; the 252-day ones are the
guard that equities did not move.
"""

import numpy as np
import pandas as pd
import pytest

from analysis.performance_analyzer import PerformanceAnalyzer, periods_per_year
from analysis.statistical_analyzer import StatisticalAnalyzer
from portfolio.portfolio_optimizer import PortfolioOptimizer


def weekdays(n=300):
    return pd.bdate_range("2024-01-01", periods=n, tz="UTC")


def every_day(n=300):
    return pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC")


def history(index):
    rng = np.random.default_rng(0)
    total = pd.Series(100_000 * np.cumprod(1 + rng.normal(0.0005, 0.02, len(index))), index=index)
    return pd.DataFrame({"total": total, "returns": total.pct_change().fillna(0.0)})


def test_a_calendar_without_weekends_has_252_bars_a_year():
    assert periods_per_year(weekdays()) == 252


def test_a_calendar_with_weekends_has_365():
    assert periods_per_year(every_day()) == 365


def test_one_stray_weekend_bar_does_not_flip_an_equity_series():
    index = weekdays().append(pd.DatetimeIndex([pd.Timestamp("2025-03-01", tz="UTC")]))
    assert pd.Timestamp("2025-03-01").dayofweek == 5
    assert periods_per_year(index) == 252


def test_no_bars_defaults_to_252():
    assert periods_per_year(pd.DatetimeIndex([])) == 252


@pytest.mark.parametrize("index, bars", [(weekdays(), 252), (every_day(), 365)])
def test_volatility_is_annualized_on_the_series_calendar(index, bars):
    h = history(index)
    metrics = PerformanceAnalyzer(h, risk_free_accrual=np.zeros(len(h))).calculate_all_metrics()
    assert metrics["Annualized Volatility"] == pytest.approx(h["returns"].std() * np.sqrt(bars))


def test_a_crypto_sharpe_is_measured_at_365():
    """The same returns on a weekday calendar would show a higher Sharpe."""
    h = history(every_day())
    m = PerformanceAnalyzer(h, risk_free_accrual=np.zeros(len(h))).calculate_all_metrics()
    excess_vol = h["returns"].std() * np.sqrt(365)
    days = (h.index[-1] - h.index[0]).days
    growth = h["total"].iloc[-1] / 100_000
    expected = (growth ** (365.0 / days) - 1) / excess_vol
    assert m["Sharpe Ratio"] == pytest.approx(expected)


@pytest.mark.parametrize("index, bars", [(weekdays(), 252), (every_day(), 365)])
def test_the_portfolio_optimizer_uses_the_same_calendar(index, bars):
    rng = np.random.default_rng(1)
    prices = pd.DataFrame(
        100 * np.cumprod(1 + rng.normal(0.0005, 0.02, (len(index), 2)), axis=0),
        index=index,
        columns=["A", "B"],
    )
    optimizer = PortfolioOptimizer(prices, risk_free_accrual=np.zeros(len(prices)), seed=1)
    results, _, _ = optimizer.simulate_random_portfolios(num_portfolios=10)
    weights = np.array([results["weights"].iloc[0][k] for k in ("A", "B")])
    daily = prices.pct_change().fillna(0.0).to_numpy() @ weights
    assert results["Annualized Volatility"].iloc[0] == pytest.approx(
        daily.std(ddof=1) * np.sqrt(bars)
    )


@pytest.mark.parametrize("index, bars", [(weekdays(), 252), (every_day(), 365)])
def test_alpha_is_annualized_on_the_overlapping_dates(index, bars):
    rng = np.random.default_rng(2)
    bench = pd.Series(rng.normal(0, 0.01, len(index)), index=index)
    asset = 0.001 + 1.2 * bench + pd.Series(rng.normal(0, 0.001, len(index)), index=index)
    result = StatisticalAnalyzer().run_ols_regression(asset, bench)
    _slope, intercept = np.polyfit(bench.to_numpy(), asset.to_numpy(), 1)
    assert result["Alpha (Annualized)"] == pytest.approx(intercept * bars)
