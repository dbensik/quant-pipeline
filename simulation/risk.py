"""
simulation/risk.py
Tail-risk summaries over simulated paths. Losses are reported as POSITIVE
numbers (VaR 0.08 = an 8% loss), the convention risk tables use.
"""

from __future__ import annotations

import numpy as np


def horizon_returns(return_paths: np.ndarray, days: int) -> np.ndarray:
    """Compounded simple return over the first `days` steps of each path."""
    paths = np.asarray(return_paths, dtype=float)
    if days < 1 or days > paths.shape[1]:
        raise ValueError(f"days must be in [1, {paths.shape[1]}], got {days}")
    return np.prod(1.0 + paths[:, :days], axis=1) - 1.0


def var(returns: np.ndarray, level: float = 0.95) -> float:
    """Value at risk: the loss exceeded with probability 1 - level."""
    _check_level(level)
    # `+ 0.0` turns a -0.0 (flat paths) into 0.0 so JSON never shows "-0.0".
    return float(-np.percentile(np.asarray(returns, dtype=float), 100.0 * (1.0 - level)) + 0.0)


def cvar(returns: np.ndarray, level: float = 0.95) -> float:
    """Expected shortfall: the mean loss in the worst 1 - level of outcomes."""
    _check_level(level)
    r = np.asarray(returns, dtype=float)
    cutoff = np.percentile(r, 100.0 * (1.0 - level))
    tail = r[r <= cutoff]
    return float(-tail.mean() + 0.0)


def prob_ruin(equity: np.ndarray, threshold: float) -> float:
    """Share of paths whose equity ever falls below `threshold` x its start."""
    if not 0.0 < threshold < 1.0:
        raise ValueError("threshold must be in (0, 1)")
    eq = np.asarray(equity, dtype=float)
    return float(np.mean(eq.min(axis=1) < threshold * eq[:, 0]))


def prob_worse_drawdown(max_drawdowns: np.ndarray, historical: float) -> float:
    """Share of paths whose max drawdown is deeper than the historical one (both <= 0)."""
    return float(np.mean(np.asarray(max_drawdowns, dtype=float) < historical))


def _check_level(level: float) -> None:
    if not 0.0 < level < 1.0:
        raise ValueError("level must be in (0, 1)")
