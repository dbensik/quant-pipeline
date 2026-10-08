"""
core/rates.py

The risk-free rate: ingest of a daily rate series, and conversion on read.

Tier 2 phase 0 of research/option-pricing-plan-2026-10-08.md (decision 1).
The series is ^IRX, the 13-week Treasury bill rate as Yahoo serves it. Tier 3
(research/dcf-plan-2026-10-08.md) reads the same series for its discount rate.

THE QUOTING CONVENTION, CHECKED RATHER THAN ASSUMED
    ^IRX is a BANK-DISCOUNT rate in percent. On 2026-10-01..07 it read
    3.982-4.037 against Treasury's 13-week bank-discount close of 4.00-4.05
    (within ~0.02: Yahoo's is an intraday snapshot) and sat ~0.11 below the
    coupon-equivalent column. A discount rate is not a yield: it is quoted on
    face value over a 360-day year. A pricer needs a continuously compounded
    rate, so the stored value is converted on read, never on write.

        price              P = 1 - d * t / 360
        bond-equivalent    y = 365 * d / (360 - d * t)          (t <= 182)
        continuous         r = -ln(P) * 365 / t

    At d = 4.05%, t = 91 the bond-equivalent is 4.149%; Treasury published
    4.15 for that day. That is the known-answer test.

WRITE RULES (the served-prices rule, applied to rates)
    * Stored exactly as served, in the series' own convention. Negative
      values are real (seven days in 2020) and kept.
    * Insert-only: an observation is written once, never rewritten.
    * Today's New York date is never stored. yfinance treats `end` as
      exclusive and the bar for an unfinished session is a moving intraday
      value; storing it would freeze it, since nothing overwrites.
    * Each run re-requests INGEST_OVERLAP_DAYS before the newest stored date,
      so a day a run missed is filled by the next one, as price ingest does.

READ RULE
    The value ON OR BEFORE the requested date, never an exact-date join. The
    bond market closes on Columbus Day and Veterans Day while stocks trade,
    and both fall inside the option archive's life. The observation date is
    returned with the rate, and a rate older than RISK_FREE_RATE_MAX_AGE_DAYS
    is marked stale rather than silently used.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, List, Optional, Protocol, Tuple
from zoneinfo import ZoneInfo

from config.settings import (
    INGEST_OVERLAP_DAYS,
    RISK_FREE_RATE_HISTORY_START,
    RISK_FREE_RATE_MAX_AGE_DAYS,
    RISK_FREE_RATE_SERIES,
    RISK_FREE_RATE_TENOR_DAYS,
)

logger = logging.getLogger(__name__)

NEW_YORK = ZoneInfo("America/New_York")
SOURCE = "yfinance"


# ---------------------------------------------------------------------------
# Conversions (pure)
# ---------------------------------------------------------------------------

def discount_price(discount_pct: float, days: int = RISK_FREE_RATE_TENOR_DAYS) -> float:
    """Price per unit face of a bill quoted at `discount_pct` (bank discount)."""
    return 1.0 - (discount_pct / 100.0) * days / 360.0


def discount_to_bond_equivalent(
    discount_pct: float, days: int = RISK_FREE_RATE_TENOR_DAYS
) -> float:
    """
    Bond-equivalent (coupon-equivalent) yield, as a decimal, for a bill of up
    to 182 days. The convention Treasury publishes beside the discount rate.
    """
    if not 0 < days <= 182:
        raise ValueError(f"the bond-equivalent formula needs 0 < days <= 182; got {days}")
    d = discount_pct / 100.0
    return 365.0 * d / (360.0 - d * days)


def discount_to_continuous(
    discount_pct: float, days: int = RISK_FREE_RATE_TENOR_DAYS
) -> float:
    """Continuously compounded annual rate, as a decimal, ACT/365."""
    if days <= 0:
        raise ValueError(f"days must be positive; got {days}")
    price = discount_price(discount_pct, days)
    if price <= 0:
        raise ValueError(f"a discount of {discount_pct}% over {days} days prices the bill at {price}")
    return -math.log(price) * 365.0 / days


# ---------------------------------------------------------------------------
# Domain objects
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RateObservation:
    series: str
    obs_date: date
    #: In the series' own convention: percent, bank discount for ^IRX.
    value: float
    source: str
    fetched_at: datetime


@dataclass(frozen=True)
class RiskFreeRate:
    """The rate a reader should use for `as_of`, with where it came from."""

    series: str
    as_of: date
    #: The observation actually used: on or before `as_of`.
    obs_date: date
    quoted_pct: float
    continuous: float
    bond_equivalent: float
    age_days: int
    stale: bool


def risk_free_from_observation(
    observation: RateObservation,
    as_of: date,
    days: int = RISK_FREE_RATE_TENOR_DAYS,
    max_age_days: int = RISK_FREE_RATE_MAX_AGE_DAYS,
) -> RiskFreeRate:
    """Pure: convert one stored observation into the rate for `as_of`."""
    if observation.obs_date > as_of:
        raise ValueError(
            f"observation {observation.obs_date} is after the requested date {as_of}: "
            "that would be look-ahead"
        )
    age = (as_of - observation.obs_date).days
    return RiskFreeRate(
        series=observation.series,
        as_of=as_of,
        obs_date=observation.obs_date,
        quoted_pct=observation.value,
        continuous=discount_to_continuous(observation.value, days),
        bond_equivalent=discount_to_bond_equivalent(observation.value, days),
        age_days=age,
        stale=age > max_age_days,
    )


# ---------------------------------------------------------------------------
# Repository protocol (db/repositories/rates.py implements it; tests fake it)
# ---------------------------------------------------------------------------

class RateRepository(Protocol):
    async def insert(self, observations: List[RateObservation]) -> int:
        """Insert rows not already stored; return how many were new."""
        ...

    async def newest_date(self, series: str) -> Optional[date]:
        ...

    async def latest_on_or_before(self, series: str, as_of: date) -> Optional[RateObservation]:
        ...


async def risk_free_rate(
    repo: RateRepository,
    as_of: date,
    series: str = RISK_FREE_RATE_SERIES,
) -> Optional[RiskFreeRate]:
    """The rate for `as_of`, or None when nothing is stored on or before it."""
    observation = await repo.latest_on_or_before(series, as_of)
    if observation is None:
        return None
    return risk_free_from_observation(observation, as_of)


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

#: (series, start, end_exclusive) -> [(date, value)] with NaN already dropped.
RateFetcher = Callable[[str, date, date], List[Tuple[date, float]]]


def yfinance_rate_fetcher(series: str, start: date, end_exclusive: date) -> List[Tuple[date, float]]:
    """Daily closes from Yahoo, as served. `end_exclusive` is not returned."""
    import yfinance as yf

    frame = yf.download(
        series,
        start=start.isoformat(),
        end=end_exclusive.isoformat(),
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=False,
    )
    if frame is None or frame.empty:
        return []
    close = frame["Close"]
    if hasattr(close, "columns"):  # yfinance >= 0.2 returns a column per ticker
        close = close.iloc[:, 0]
    out: List[Tuple[date, float]] = []
    for stamp, value in close.items():
        if value is None or value != value:
            continue
        day = stamp.date() if hasattr(stamp, "date") else stamp
        if day < end_exclusive:  # belt and braces: never today's moving value
            out.append((day, float(value)))
    return out


@dataclass
class RateIngestReport:
    series: str
    start: date
    end_exclusive: date
    fetched: int
    inserted: int
    newest: Optional[date]


class RateIngestError(RuntimeError):
    pass


def new_york_today(now: Optional[datetime] = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(NEW_YORK).date()


async def ingest_rates(
    repo: RateRepository,
    series: str = RISK_FREE_RATE_SERIES,
    fetcher: RateFetcher = yfinance_rate_fetcher,
    start: Optional[date] = None,
    now: Optional[datetime] = None,
) -> RateIngestReport:
    """
    Fetch and insert the observations a run should have. Insert-only.

    An empty fetch is an ERROR, not "nothing new": the window always spans at
    least INGEST_OVERLAP_DAYS of business days, and a T-bill series does not
    go two weeks without a print. Empty means the provider lost the key.
    """
    end_exclusive = new_york_today(now)
    newest = await repo.newest_date(series)
    if start is None:
        start = (
            newest - timedelta(days=INGEST_OVERLAP_DAYS)
            if newest is not None
            else date.fromisoformat(RISK_FREE_RATE_HISTORY_START)
        )

    rows = await asyncio.to_thread(fetcher, series, start, end_exclusive)
    if not rows:
        raise RateIngestError(
            f"{series}: the provider returned no observations for "
            f"{start}..{end_exclusive} (exclusive). Nothing was written."
        )

    fetched_at = now or datetime.now(timezone.utc)
    observations = [
        RateObservation(series, day, value, SOURCE, fetched_at)
        for day, value in rows
        if day < end_exclusive
    ]
    inserted = await repo.insert(observations)
    return RateIngestReport(
        series=series,
        start=start,
        end_exclusive=end_exclusive,
        fetched=len(observations),
        inserted=inserted,
        newest=await repo.newest_date(series),
    )
