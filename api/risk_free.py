"""
api/risk_free.py

The risk-free rate a Sharpe ratio is in excess of, resolved once per request.

Every router that reports a Sharpe ratio (backtest, compare, optimize,
portfolio backtest, simulate, and their websockets) resolves it here, so they
all subtract the same thing: the stored ^IRX bond-equivalent rate averaged
over the window's sessions (core.rates.risk_free_over). A rate in the request
is used as given and the store is never read. A window the stored series does
not cover is refused, never filled with a constant: a 0.02 default and a
silent 0 are what this replaced (2026-10-12).

Backtests also CREDIT idle cash with the same rate, bar by bar (`accrual`),
and measure Sharpe against that cash account, so a strategy that never trades
scores 0 rather than being charged for interest it was never paid.
"""

from __future__ import annotations

from datetime import date
from typing import List, NamedTuple, Optional

import numpy as np
import pandas as pd
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db
from core.rates import RateRepository, RiskFreeCoverageError, period_accruals, risk_free_over
from db.repositories.rates import TimescaleRateRepo

__all__ = [
    "RISK_FREE_RATE_DESCRIPTION",
    "RISK_FREE_SOURCE_DESCRIPTION",
    "ResolvedRiskFree",
    "RiskFreeCoverageError",
    "constant_accruals",
    "get_rate_repo",
    "resolve_risk_free",
    "risk_free_refusal",
]

RISK_FREE_RATE_DESCRIPTION = (
    "Annual risk-free rate the Sharpe ratio is in excess of. Omitted, it is the "
    "stored ^IRX bond-equivalent rate averaged over the window's sessions."
)
RISK_FREE_SOURCE_DESCRIPTION = (
    "Where risk_free_rate came from: the request, or ^IRX over the window"
)


async def get_rate_repo(session: AsyncSession = Depends(get_db)) -> RateRepository:
    return TimescaleRateRepo(session)


class ResolvedRiskFree(NamedTuple):
    """The rate for one request: (mean annual rate, where from, per-bar accrual)."""

    rate: float
    source: str
    #: Interest one unit of cash earns on each bar of the index it was
    #: resolved for (core.rates.period_accruals); 0 on the first bar.
    accrual: np.ndarray


def _dates(index: pd.Index) -> List[date]:
    return [ts.date() for ts in pd.DatetimeIndex(index)]


def constant_accruals(index: pd.Index, annual_rate: float) -> np.ndarray:
    """Per-bar accrual at one annual rate: a supplied rate, or synthetic bars."""
    dates = _dates(index)
    return period_accruals(dates, [annual_rate] * max(len(dates) - 1, 0))


async def resolve_risk_free(
    override: Optional[float], index: pd.Index, rates: RateRepository
) -> ResolvedRiskFree:
    """
    The rate for a window of price dates, and the accrual on each of them.

    Each period is credited the bill rate known at its START, so the window's
    last print is not needed and nothing is read ahead: N prices make N-1
    periods, starting on the first N-1 dates. A rate in the request is used
    as given and the store is never read. Raises RiskFreeCoverageError on a gap.
    """
    if override is not None:
        return ResolvedRiskFree(
            override, "supplied in the request", constant_accruals(index, override)
        )
    if len(index) < 2:
        # No period, so nothing accrues and no ratio for a rate to enter.
        return ResolvedRiskFree(
            0.0, "no return periods in the window: no rate applies", np.zeros(len(index))
        )
    dates = _dates(index)
    window = await risk_free_over(rates, dates[:-1])
    return ResolvedRiskFree(
        window.rate, window.describe(), period_accruals(dates, window.per_session)
    )


def risk_free_refusal(exc: RiskFreeCoverageError) -> str:
    return (
        f"No stored risk-free rate covers this window ({exc}). Pass risk_free_rate to supply one."
    )
