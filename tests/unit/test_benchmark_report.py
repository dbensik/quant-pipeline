"""
analysis/benchmark_report.py (decision 53,
research/benchmark-regime-report-2026-10-20.md): exposure, rolling Sharpe and
regimes of a run against its benchmark. Each test was checked failing
against a mutant of the code it covers.
"""

import numpy as np
import pandas as pd
import pytest
import statsmodels.api as sm

from analysis.benchmark_report import REGIMES, benchmark_report, default_benchmark, regimes_of
from backtesting.parameter_optimizer import window_metrics
from config import settings


def market(n=900, seed=7, weekdays=True):
    rng = np.random.default_rng(seed)
    index = (
        pd.bdate_range("2020-01-01", periods=n, tz="UTC")
        if weekdays
        else pd.date_range("2020-01-01", periods=n, tz="UTC")
    )
    # Volatility that changes over time, so the regimes are not one bucket.
    sigma = 0.006 + 0.02 * (np.sin(np.arange(n) / 60) > 0.5)
    close = 100 * np.cumprod(1 + rng.normal(0.0004, sigma))
    return pd.Series(close, index=index)


def cash_accrual(index, annual=0.04):
    return pd.Series(annual / 252, index=index)


def test_the_benchmark_against_itself_is_beta_one_alpha_zero():
    close = market()
    run = close.iloc[300:] * 1000
    report = benchmark_report(run, cash_accrual(run.index), close, "X")
    assert report.beta == pytest.approx(1.0, abs=1e-12)
    assert report.alpha == pytest.approx(0.0, abs=1e-12)
    assert report.r_squared == pytest.approx(1.0, abs=1e-12)
    assert report.bars == len(run) - 1


def test_a_third_of_the_market_and_the_rest_in_cash_is_beta_point_three():
    close = market()
    accrual = cash_accrual(close.index)
    market_return = close.pct_change().fillna(0.0)
    held = (1 + accrual + 0.3 * (market_return - accrual)).cumprod() * 100_000
    run = held.iloc[300:]
    report = benchmark_report(run, accrual.loc[run.index], close, "X")
    assert report.beta == pytest.approx(0.3, abs=1e-9)
    assert report.alpha == pytest.approx(0.0, abs=1e-9)


def test_alpha_is_annualized_and_its_t_is_newey_west():
    close = market()
    accrual = cash_accrual(close.index)
    rng = np.random.default_rng(1)
    market_return = close.pct_change().fillna(0.0)
    noise = rng.normal(0.0002, 0.004, len(close))
    run = ((1 + accrual + 0.5 * (market_return - accrual) + noise).cumprod() * 1e5).iloc[300:]
    report = benchmark_report(run, accrual.loc[run.index], close, "X")
    y = run.pct_change().iloc[1:] - accrual.loc[run.index].iloc[1:]
    x = close.loc[run.index].pct_change().iloc[1:] - accrual.loc[run.index].iloc[1:]
    fit = sm.OLS(y, sm.add_constant(x)).fit(
        cov_type="HAC", cov_kwds={"maxlags": settings.BENCHMARK_NEWEY_WEST_LAGS}
    )
    assert report.alpha == pytest.approx(fit.params.iloc[0] * 252)
    assert report.alpha_t == pytest.approx(fit.tvalues.iloc[0])
    assert report.alpha_t != pytest.approx(sm.OLS(y, sm.add_constant(x)).fit().tvalues.iloc[0])


def test_regimes_partition_the_ranked_days_once():
    close = market()
    run = close.iloc[300:] * 3
    report = benchmark_report(run, cash_accrual(run.index), close, "X")
    assert [r.name for r in report.regimes] == list(REGIMES)
    assert sum(r.days for r in report.regimes) + report.unranked_days == report.bars
    assert all(r.days > 50 for r in report.regimes)
    assert sum(r.share for r in report.regimes) == pytest.approx(
        1 - report.unranked_days / report.bars
    )


def test_a_days_regime_does_not_depend_on_its_own_or_later_prices():
    close = market()
    labels = regimes_of(close)
    k = 600
    moved = close.copy()
    moved.iloc[k:] = moved.iloc[k:] * np.linspace(1, 3, len(close) - k) ** 4
    assert labels.iloc[: k + 1].equals(regimes_of(moved).iloc[: k + 1])
    assert not labels.iloc[k + 2 :].equals(regimes_of(moved).iloc[k + 2 :])


def test_regimes_start_once_there_is_enough_earlier_history():
    close = market()
    labels = regimes_of(close)
    first = settings.BENCHMARK_REGIME_WINDOW + 1 + settings.BENCHMARK_REGIME_MIN_HISTORY
    assert labels.iloc[:first].isna().all()
    assert labels.iloc[first] in REGIMES


def test_rolling_sharpe_is_the_walk_forward_window_sharpe():
    close = market()
    accrual = cash_accrual(close.index, 0.03)
    rng = np.random.default_rng(3)
    run = pd.Series(1e5 * np.cumprod(1 + rng.normal(0.0005, 0.01, len(close))), index=close.index)
    report = benchmark_report(run, accrual, close, "X")
    window = settings.BENCHMARK_ROLLING_SHARPE_WINDOW
    assert len(report.rolling_sharpe) == len(run) - window
    for i in (window, 500, len(run) - 1):
        time, strategy, benchmark = report.rolling_sharpe[i - window]
        assert time == run.index[i]
        expected = window_metrics(
            run.to_numpy(), None, run.index, accrual.to_numpy(), i - window, i, 1.0
        )["Sharpe Ratio"]
        assert strategy == pytest.approx(expected, rel=1e-9)
        expected = window_metrics(
            close.to_numpy(), None, close.index, accrual.to_numpy(), i - window, i, 1.0
        )["Sharpe Ratio"]
        assert benchmark == pytest.approx(expected, rel=1e-9)


def test_a_crypto_weekend_compounds_into_the_benchmarks_monday():
    close = market(n=900)
    days = pd.date_range(close.index[0], close.index[-1], tz="UTC")
    rng = np.random.default_rng(4)
    coin = pd.Series(1e5 * np.cumprod(1 + rng.normal(0.001, 0.03, len(days))), index=days)
    accrual = pd.Series(0.04 / 365, index=days)
    report = benchmark_report(coin, accrual, close, "X")
    shared = close.index
    cash = (1 + accrual).cumprod().loc[shared]
    y = coin.loc[shared].pct_change().iloc[1:] - cash.pct_change().iloc[1:]
    x = close.pct_change().iloc[1:] - cash.pct_change().iloc[1:]
    fit = sm.OLS(y, sm.add_constant(x)).fit()
    assert report.bars == len(shared) - 1
    assert report.periods_per_year == 252
    assert report.beta == pytest.approx(fit.params.iloc[1])


def test_too_few_shared_bars_is_refused():
    close = market()
    run = close.iloc[-settings.BENCHMARK_MIN_BARS :]
    with pytest.raises(ValueError, match="shares"):
        benchmark_report(run, cash_accrual(run.index), close, "X")


def test_the_default_benchmark_follows_the_asset_classes():
    assert default_benchmark(["equity"]) == "SPY"
    assert default_benchmark(["etf", "etf"]) == "SPY"
    assert default_benchmark(["crypto"]) == "BTC-USD"
    assert default_benchmark(["crypto", "equity"]) == "SPY"
