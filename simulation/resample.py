"""
simulation/resample.py
Draw (n_paths, horizon) arrays of daily SIMPLE returns from one observed series.

Three methods. The stationary bootstrap is the default because it keeps
volatility clustering — the thing that makes drawdown tails real. iid
resampling destroys it and understates the tails; GBM assumes normal log
returns and understates them further. Both are kept so the difference is
visible beside the default, never reported alone.
"""

from __future__ import annotations

from typing import Callable, Dict

import numpy as np

METHODS = ("stationary", "iid", "gbm")


def _check_returns(returns: np.ndarray) -> np.ndarray:
    r = np.asarray(returns, dtype=float)
    if r.ndim != 1:
        raise ValueError(f"returns must be 1-D, got shape {r.shape}")
    if r.size < 2:
        raise ValueError("returns needs at least 2 observations")
    if not np.all(np.isfinite(r)):
        raise ValueError("returns contains NaN or Inf")
    if np.any(r <= -1.0):
        raise ValueError("a simple return of -100% or worse cannot be compounded")
    return r


def _check_shape(n_paths: int, horizon: int) -> None:
    if n_paths < 1:
        raise ValueError("n_paths must be >= 1")
    if horizon < 1:
        raise ValueError("horizon must be >= 1")


def iid_bootstrap(
    returns: np.ndarray, n_paths: int, horizon: int, rng: np.random.Generator
) -> np.ndarray:
    """Resample single days with replacement. Loses all serial dependence."""
    r = _check_returns(returns)
    _check_shape(n_paths, horizon)
    return r[rng.integers(0, r.size, size=(n_paths, horizon))]


def stationary_bootstrap(
    returns: np.ndarray,
    n_paths: int,
    horizon: int,
    mean_block_days: float,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    Politis & Romano (1994). Each step either continues the current block
    (next observed day, wrapping at the end) or, with probability
    1/mean_block_days, starts a new block at a uniformly random day. Block
    lengths are therefore geometric with the given mean, and the resampled
    series is stationary — unlike fixed-length blocks, whose joints fall at
    the same offsets on every path.

    One numpy step per horizon day over all paths at once: 2,520 days x
    10,000 paths runs in well under a second.
    """
    r = _check_returns(returns)
    _check_shape(n_paths, horizon)
    if mean_block_days < 1:
        raise ValueError("mean_block_days must be >= 1")
    n = r.size
    p_restart = 1.0 / mean_block_days
    idx = np.empty((n_paths, horizon), dtype=np.int64)
    idx[:, 0] = rng.integers(0, n, size=n_paths)
    restart = rng.random(size=(n_paths, horizon)) < p_restart
    fresh = rng.integers(0, n, size=(n_paths, horizon))
    for t in range(1, horizon):
        idx[:, t] = np.where(restart[:, t], fresh[:, t], (idx[:, t - 1] + 1) % n)
    return r[idx]


def gbm(
    returns: np.ndarray, n_paths: int, horizon: int, rng: np.random.Generator
) -> np.ndarray:
    """
    Geometric Brownian motion fitted to the series: daily log returns drawn
    i.i.d. normal with the observed log-return mean and standard deviation.
    Returned as simple returns so every method has the same shape and units.
    """
    r = _check_returns(returns)
    _check_shape(n_paths, horizon)
    log_r = np.log1p(r)
    mu, sigma = float(log_r.mean()), float(log_r.std(ddof=1))
    return np.expm1(rng.normal(mu, sigma, size=(n_paths, horizon)))


def draw(
    method: str,
    returns: np.ndarray,
    n_paths: int,
    horizon: int,
    rng: np.random.Generator,
    mean_block_days: float = 20.0,
) -> np.ndarray:
    """Dispatch on `method`, one of METHODS."""
    if method == "stationary":
        return stationary_bootstrap(returns, n_paths, horizon, mean_block_days, rng)
    if method == "iid":
        return iid_bootstrap(returns, n_paths, horizon, rng)
    if method == "gbm":
        return gbm(returns, n_paths, horizon, rng)
    raise ValueError(f"unknown method {method!r}; expected one of {METHODS}")
