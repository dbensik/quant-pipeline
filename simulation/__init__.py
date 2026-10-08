"""
simulation — Monte Carlo over one observed return series.

Pure numpy. No I/O, no settings, no FastAPI: every function takes its
parameters explicitly and its randomness from a `numpy.random.Generator`, so
a router seeds once per request and the result is reproducible. Defaults
(block length, percentiles, caps) live in `config/settings.py` and are read
by the router, not here.

Plan: research/monte-carlo-plan-2026-10-07.md (approved 2026-10-07).
"""

from simulation.paths import (
    bands,
    equity_from_returns,
    max_drawdown,
    max_drawdown_duration,
    terminal_wealth,
)
from simulation.resample import METHODS, draw, gbm, iid_bootstrap, stationary_bootstrap
from simulation.risk import cvar, horizon_returns, prob_ruin, prob_worse_drawdown, var

__all__ = [
    "METHODS",
    "bands",
    "cvar",
    "draw",
    "equity_from_returns",
    "gbm",
    "horizon_returns",
    "iid_bootstrap",
    "max_drawdown",
    "max_drawdown_duration",
    "prob_ruin",
    "prob_worse_drawdown",
    "stationary_bootstrap",
    "terminal_wealth",
    "var",
]
