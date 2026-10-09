"""
pricing/volatility.py

Realised volatility from daily OHLC, and the volatility cone.

Estimators (per-day variance, then annualised by REALISED_VOL_DAYS_PER_YEAR):

    close_to_close   sample variance of log close-to-close returns
    parkinson        (ln H/L)^2 / (4 ln 2)                       — high/low only
    garman_klass     0.5 (ln H/L)^2 - (2 ln 2 - 1)(ln C/O)^2      — OHLC, no gaps
    rogers_satchell  ln(H/C) ln(H/O) + ln(L/C) ln(L/O)           — drift-robust
    yang_zhang       overnight + k * open-to-close + (1 - k) * rogers_satchell,
                     k = 0.34 / (1.34 + (n + 1)/(n - 1))          — gaps and drift

Parkinson and Garman-Klass assume zero drift and no overnight gap; with a
strong trend they are biased while Rogers-Satchell and Yang-Zhang are not.
Every function takes a WINDOW of bars and returns one annualised number;
`rolling` applies one over a series, and `vol_cone` summarises the rolling
values per window: min, p25, median, p75, max.

Annualising uses trading days. Option pricing time is ACT/365; the two are
never mixed inside one function, and a caller comparing implied with realised
should know they are on different clocks.
"""

from __future__ import annotations

from typing import Callable, Dict, Sequence

import numpy as np

from config.settings import REALISED_VOL_DAYS_PER_YEAR, REALISED_VOL_WINDOWS

LN2 = np.log(2.0)


def _arrays(*xs):
    arrs = [np.asarray(x, dtype=float) for x in xs]
    n = arrs[0].shape
    for a in arrs:
        if a.ndim != 1 or a.shape != n:
            raise ValueError("inputs must be 1-D arrays of equal length")
        if not np.all(np.isfinite(a)) or np.any(a <= 0):
            raise ValueError("prices must be finite and positive")
    return arrs


def _annualise(daily_variance: float, days_per_year: int) -> float:
    return float(np.sqrt(max(daily_variance, 0.0) * days_per_year))


def close_to_close(close, days_per_year: int = REALISED_VOL_DAYS_PER_YEAR) -> float:
    (c,) = _arrays(close)
    if c.size < 3:
        raise ValueError("need at least 3 closes")
    r = np.diff(np.log(c))
    return _annualise(r.var(ddof=1), days_per_year)


def parkinson(high, low, days_per_year: int = REALISED_VOL_DAYS_PER_YEAR) -> float:
    h, lo = _arrays(high, low)
    return _annualise(np.mean(np.log(h / lo) ** 2) / (4.0 * LN2), days_per_year)


def garman_klass(open_, high, low, close, days_per_year: int = REALISED_VOL_DAYS_PER_YEAR) -> float:
    o, h, lo, c = _arrays(open_, high, low, close)
    term = 0.5 * np.log(h / lo) ** 2 - (2.0 * LN2 - 1.0) * np.log(c / o) ** 2
    return _annualise(term.mean(), days_per_year)


def _rs_terms(o, h, lo, c):
    return np.log(h / c) * np.log(h / o) + np.log(lo / c) * np.log(lo / o)


def rogers_satchell(open_, high, low, close, days_per_year: int = REALISED_VOL_DAYS_PER_YEAR) -> float:
    o, h, lo, c = _arrays(open_, high, low, close)
    return _annualise(_rs_terms(o, h, lo, c).mean(), days_per_year)


def yang_zhang_k(n: int) -> float:
    """The weight on open-to-close variance, for n bars (alpha = 1.34)."""
    return 0.34 / (1.34 + (n + 1) / (n - 1))


def yang_zhang(open_, high, low, close, days_per_year: int = REALISED_VOL_DAYS_PER_YEAR) -> float:
    """
    Needs n + 1 bars: the first contributes only its close, as the previous
    close for the first overnight return.
    """
    o, h, lo, c = _arrays(open_, high, low, close)
    if c.size < 3:
        raise ValueError("need at least 3 bars")
    overnight = np.log(o[1:] / c[:-1])
    open_close = np.log(c[1:] / o[1:])
    n = overnight.size
    k = yang_zhang_k(n)
    rs = _rs_terms(o[1:], h[1:], lo[1:], c[1:]).mean()
    variance = overnight.var(ddof=1) + k * open_close.var(ddof=1) + (1.0 - k) * rs
    return _annualise(variance, days_per_year)


ESTIMATORS: Dict[str, Callable] = {
    "close_to_close": lambda o, h, lo, c: close_to_close(c),
    "parkinson": lambda o, h, lo, c: parkinson(h, lo),
    "garman_klass": garman_klass,
    "rogers_satchell": rogers_satchell,
    "yang_zhang": yang_zhang,
}


def rolling(estimator: str, open_, high, low, close, window: int) -> np.ndarray:
    """
    The estimator over each trailing `window` of returns, aligned to the last
    bar of the window; NaN where the window is not yet full. Estimators that
    use a previous close (close_to_close, yang_zhang) read window + 1 bars.
    """
    if estimator not in ESTIMATORS:
        raise ValueError(f"unknown estimator {estimator!r}; one of {sorted(ESTIMATORS)}")
    if window < 2:
        raise ValueError("window must be >= 2")
    o, h, lo, c = _arrays(open_, high, low, close)
    fn = ESTIMATORS[estimator]
    needs_prev = estimator in ("close_to_close", "yang_zhang")
    span = window + 1 if needs_prev else window
    out = np.full(c.size, np.nan)
    for end in range(span, c.size + 1):
        sl = slice(end - span, end)
        out[end - 1] = fn(o[sl], h[sl], lo[sl], c[sl])
    return out


CONE_STATS = ("min", "p25", "median", "p75", "max")


def vol_cone(
    open_,
    high,
    low,
    close,
    windows: Sequence[int] = REALISED_VOL_WINDOWS,
    estimator: str = "yang_zhang",
) -> Dict[int, Dict[str, float]]:
    """Per window: the distribution of rolling realised vol over the history."""
    cone: Dict[int, Dict[str, float]] = {}
    for w in windows:
        values = rolling(estimator, open_, high, low, close, w)
        values = values[np.isfinite(values)]
        if values.size == 0:
            continue
        cone[w] = dict(
            zip(CONE_STATS, (float(x) for x in np.percentile(values, [0, 25, 50, 75, 100])))
        )
    return cone
