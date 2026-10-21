"""
analysis/benchmark_report.py

A backtest against a benchmark (decision 53,
research/benchmark-regime-report-2026-10-20.md): how much of the result is
exposure to the benchmark, its Sharpe over time, and how it did in the
benchmark's calm and stressed spells. Pure: equity, cash accrual and the
benchmark's closes in, a report out.

- Exposure: daily returns in excess of the cash account the backtest credits,
  regressed on the benchmark's; alpha annualized, with a Newey-West t.
- Rolling Sharpe: the walk-forward window definition (`window_metrics`) over
  each trailing window, for the strategy and the benchmark.
- Regimes: a day is ranked by the benchmark's trailing volatility as known at
  the close BEFORE it, against every earlier reading only (no look-ahead),
  and falls in the low, middle or high third.

Both series are read on the bars they share. A return between two shared
bars compounds whatever happened between them (a crypto strategy's weekend
into SPY's Monday), and the cash account is compounded on the strategy's own
bars the same way.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from analysis.performance_analyzer import periods_per_year
from config import settings

REGIMES = ("low", "middle", "high")


@dataclass(frozen=True)
class Regime:
    name: str
    days: int
    share: float
    #: Mean excess return a bar, annualized (arithmetic: the days are not contiguous).
    strategy_return: Optional[float]
    benchmark_return: Optional[float]
    strategy_sharpe: Optional[float]
    benchmark_sharpe: Optional[float]
    #: Standard error of each Sharpe, sqrt((1 + SR^2 / 2) / n) a bar, annualized.
    strategy_sharpe_error: Optional[float]
    benchmark_sharpe_error: Optional[float]


@dataclass
class BenchmarkReport:
    symbol: str
    start: pd.Timestamp
    end: pd.Timestamp
    #: Returns in the regression (shared bars less the first).
    bars: int
    periods_per_year: int
    beta: float
    alpha: float
    alpha_t: float
    alpha_low: float
    alpha_high: float
    r_squared: float
    rolling_window: int
    #: (time, strategy Sharpe, benchmark Sharpe) for each full trailing window.
    rolling_sharpe: List[Tuple[pd.Timestamp, Optional[float], Optional[float]]] = field(
        default_factory=list
    )
    regimes: List[Regime] = field(default_factory=list)
    #: Days with too little earlier benchmark history to be ranked.
    unranked_days: int = 0


def regimes_of(close: pd.Series) -> pd.Series:
    """
    "low" / "middle" / "high" for each bar of `close`, None where unranked.

    A bar's reading is the std of the BENCHMARK_REGIME_WINDOW returns that
    ended at the bar before it. It is ranked against every earlier reading
    (the share strictly below it) once there are BENCHMARK_REGIME_MIN_HISTORY
    of them: up to a third is low, up to two thirds middle, above is high.
    """
    readings = (
        close.pct_change(fill_method=None)
        .rolling(settings.BENCHMARK_REGIME_WINDOW)
        .std()
        .shift(1)
        .to_numpy()
    )
    earlier: List[float] = []
    out: List[Optional[str]] = []
    for value in readings:
        if not np.isfinite(value):
            out.append(None)
            continue
        if len(earlier) >= settings.BENCHMARK_REGIME_MIN_HISTORY:
            below = bisect.bisect_left(earlier, value) / len(earlier)
            out.append("low" if below <= 1 / 3 else "middle" if below <= 2 / 3 else "high")
        else:
            out.append(None)
        bisect.insort(earlier, value)
    return pd.Series(out, index=close.index, dtype=object)


def _window_sharpe(values: np.ndarray, cash: np.ndarray, days: int, annualize: float):
    """`window_metrics`' Sharpe for one window: bar 0 is the base (return 0)."""
    from analysis.performance_analyzer import cash_account_sharpe

    returns = values[1:] / values[:-1] - 1
    accrual = cash[1:] / cash[:-1] - 1
    excess = np.concatenate([[0.0], returns - accrual])
    volatility = excess.std(ddof=1) * annualize
    sharpe = float(
        cash_account_sharpe(values[-1] / values[0], cash[-1] / cash[0], days, volatility)
    )
    return sharpe if np.isfinite(sharpe) else None


def _sharpe_and_error(excess: pd.Series, ppy: int):
    n = len(excess)
    if n < 2 or not excess.std() > 0:
        return None, None
    per_bar = excess.mean() / excess.std()
    return float(per_bar * np.sqrt(ppy)), float(np.sqrt((1 + per_bar**2 / 2) / n * ppy))


def benchmark_report(
    total: pd.Series, accrual: pd.Series, benchmark_close: pd.Series, symbol: str
) -> BenchmarkReport:
    """
    `total` is the strategy's equity on its bars, `accrual` the interest one
    unit of cash earned on each of them (the backtest's), `benchmark_close`
    the benchmark's adjusted closes from BENCHMARK_HISTORY_DAYS before the
    run. ValueError when they share fewer than BENCHMARK_MIN_BARS bars.
    """
    import statsmodels.api as sm

    benchmark_close = benchmark_close.dropna()
    regime = regimes_of(benchmark_close)
    by_day = lambda s: s.set_axis(pd.DatetimeIndex(s.index).normalize())  # noqa: E731
    total, accrual = by_day(total), by_day(accrual)
    close, regime = by_day(benchmark_close), by_day(regime)
    cash = (1.0 + accrual).cumprod()

    shared = total.index.intersection(close.index)
    shared = shared[(shared >= total.index[0]) & (shared <= total.index[-1])]
    if len(shared) - 1 < settings.BENCHMARK_MIN_BARS:
        raise ValueError(
            f"The run shares {len(shared)} bars with {symbol}; a regression needs "
            f"{settings.BENCHMARK_MIN_BARS + 1}."
        )
    s, c, b = total.loc[shared], cash.loc[shared], close.loc[shared]
    cash_return = c.pct_change().iloc[1:]
    strategy = s.pct_change().iloc[1:] - cash_return
    market = b.pct_change().iloc[1:] - cash_return
    ppy = periods_per_year(shared)

    model = sm.OLS(strategy, sm.add_constant(market.rename("benchmark"))).fit(
        cov_type="HAC", cov_kwds={"maxlags": settings.BENCHMARK_NEWEY_WEST_LAGS}
    )
    low, high = model.conf_int().loc["const"]
    report = BenchmarkReport(
        symbol=symbol,
        start=shared[0],
        end=shared[-1],
        bars=len(strategy),
        periods_per_year=ppy,
        beta=float(model.params["benchmark"]),
        alpha=float(model.params["const"] * ppy),
        alpha_t=float(model.tvalues["const"]),
        alpha_low=float(low * ppy),
        alpha_high=float(high * ppy),
        r_squared=float(model.rsquared),
        rolling_window=settings.BENCHMARK_ROLLING_SHARPE_WINDOW,
    )

    window, annualize = report.rolling_window, np.sqrt(ppy)
    sv, cv, bv = s.to_numpy(), c.to_numpy(), b.to_numpy()
    for i in range(window, len(shared)):
        days = (shared[i] - shared[i - window]).days
        span = slice(i - window, i + 1)
        report.rolling_sharpe.append(
            (
                shared[i],
                _window_sharpe(sv[span], cv[span], days, annualize),
                _window_sharpe(bv[span], cv[span], days, annualize),
            )
        )

    labels = regime.reindex(strategy.index)
    report.unranked_days = int(labels.isna().sum())
    for name in REGIMES:
        days = labels == name
        strategy_sharpe, strategy_error = _sharpe_and_error(strategy[days], ppy)
        market_sharpe, market_error = _sharpe_and_error(market[days], ppy)
        n = int(days.sum())
        report.regimes.append(
            Regime(
                name=name,
                days=n,
                share=n / len(strategy),
                strategy_return=float(strategy[days].mean() * ppy) if n else None,
                benchmark_return=float(market[days].mean() * ppy) if n else None,
                strategy_sharpe=strategy_sharpe,
                benchmark_sharpe=market_sharpe,
                strategy_sharpe_error=strategy_error,
                benchmark_sharpe_error=market_error,
            )
        )
    return report


def default_benchmark(asset_classes) -> str:
    """BENCHMARK_BY_ASSET_CLASS when every asset is of that one class, else the default."""
    classes = set(asset_classes)
    if len(classes) == 1:
        return settings.BENCHMARK_BY_ASSET_CLASS.get(
            next(iter(classes)), settings.BENCHMARK_DEFAULT
        )
    return settings.BENCHMARK_DEFAULT
