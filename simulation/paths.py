"""
simulation/paths.py
Equity curves and drawdowns, vectorised over paths (axis 0 = path, axis 1 = time).

Conventions match analysis/performance_analyzer.py so a simulated drawdown is
comparable to the historical one: depth is NEGATIVE (-0.25 = a 25% fall from
the running peak) and duration is the longest run of consecutive bars strictly
below the prior peak.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np


def equity_from_returns(return_paths: np.ndarray, initial: float) -> np.ndarray:
    """
    (n_paths, horizon) simple returns -> (n_paths, horizon + 1) equity, column
    0 = `initial` on every path so a fan chart starts where the historical
    curve starts.
    """
    paths = np.asarray(return_paths, dtype=float)
    if paths.ndim != 2:
        raise ValueError(f"return_paths must be 2-D, got shape {paths.shape}")
    if initial <= 0:
        raise ValueError("initial must be > 0")
    growth = np.cumprod(1.0 + paths, axis=1)
    return np.hstack([np.full((paths.shape[0], 1), float(initial)), initial * growth])


def max_drawdown(equity: np.ndarray) -> np.ndarray:
    """Deepest fall from the running peak, per path. <= 0."""
    eq = np.asarray(equity, dtype=float)
    peak = np.maximum.accumulate(eq, axis=1)
    return (eq / peak - 1.0).min(axis=1)


def max_drawdown_duration(equity: np.ndarray) -> np.ndarray:
    """Longest run of consecutive bars strictly below the running peak, per path."""
    eq = np.asarray(equity, dtype=float)
    under = (eq < np.maximum.accumulate(eq, axis=1)).astype(np.int64)
    # Run lengths: cumulative count, reset at every bar that is not under water.
    cum = np.cumsum(under, axis=1)
    reset = np.maximum.accumulate(np.where(under == 0, cum, 0), axis=1)
    return (cum - reset).max(axis=1)


def terminal_wealth(equity: np.ndarray) -> np.ndarray:
    return np.asarray(equity, dtype=float)[:, -1]


def bands(equity: np.ndarray, percentiles: Sequence[float]) -> np.ndarray:
    """
    Per-step percentiles across paths: (horizon + 1, len(percentiles)).
    Column order follows `percentiles` as given.
    """
    pct = list(percentiles)
    if not pct:
        raise ValueError("percentiles must not be empty")
    if any(p < 0 or p > 100 for p in pct):
        raise ValueError("percentiles must lie in [0, 100]")
    return np.percentile(np.asarray(equity, dtype=float), pct, axis=0).T
