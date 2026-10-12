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
"""

from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db
from core.rates import RateRepository, RiskFreeCoverageError, risk_free_over
from db.repositories.rates import TimescaleRateRepo

__all__ = [
    "RISK_FREE_RATE_DESCRIPTION",
    "RISK_FREE_SOURCE_DESCRIPTION",
    "RiskFreeCoverageError",
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


async def resolve_risk_free(
    override: Optional[float], index: pd.Index, rates: RateRepository
) -> Tuple[float, str]:
    """
    The annual rate and a line saying where it came from.

    `index` is the price dates of the window. Each return period is set
    against the bill rate known at its START, so the window's last print is
    not needed and nothing is read ahead: N prices make N-1 returns, starting
    on the first N-1 dates. Raises RiskFreeCoverageError on a gap.
    """
    if override is not None:
        return override, "supplied in the request"
    if len(index) < 2:
        # No return period, so no ratio for a rate to enter.
        return 0.0, "no return periods in the window: no rate applies"
    sessions = sorted({ts.date() for ts in pd.DatetimeIndex(index[:-1])})
    window = await risk_free_over(rates, sessions)
    return window.rate, window.describe()


def risk_free_refusal(exc: RiskFreeCoverageError) -> str:
    return (
        f"No stored risk-free rate covers this window ({exc}). Pass risk_free_rate to supply one."
    )
