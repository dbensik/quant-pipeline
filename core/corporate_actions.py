"""
core/corporate_actions.py
Detecting when stored prices have gone stale against corporate actions.

THE PROBLEM, precisely. yfinance's `auto_adjust=True` restates a whole series
for splits AS OF THE FETCH DATE. Nothing here is ever "unadjusted" — both the
legacy SQLite pipeline and the current adapter pass it. The failure is subtler:
a symbol whose bars were fetched at two different times has two segments
adjusted to two different as-of dates, and they do not line up.

Measured on 2026-08-09, before the fix: NFLX closed 1260.27 on 2025-07-15 and
125.03 on 2025-07-16, because a 10:1 split in November 2025 had been applied to
the newer segment only. Fourteen S&P names were affected. To a strategy that is
a -90% day, and nothing anywhere reported a problem.

WHAT THIS MODULE DOES AND DOES NOT DO
    Does: tell you which symbols have a split newer than the last time their
    series was restated, and which look delisted rather than merely current.
    Does not: fix anything. The fix is a full backfill, which is a write the
    caller should choose to make.

    Renames are NOT handled. FI is Fiserv's post-rename ticker and FISV still
    holds its history, but nothing in the provider says they are the same
    company — a link column would be one nobody could populate. Open.

Pure of FastAPI and of the database, so the rules can be tested directly.

Phase 5 — corporate actions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Sequence

logger = logging.getLogger(__name__)

#: A symbol whose newest bar is older than this, and which fetches nothing, is
#: treated as no longer trading rather than as up to date. Generous on purpose:
#: a long market holiday plus a weekend is under a week, and a thin ticker can
#: legitimately go quiet for a while.
STALE_AFTER = timedelta(days=21)


@dataclass(frozen=True)
class Split:
    date: datetime
    ratio: float


@dataclass
class DriftReport:
    symbol: str
    #: Splits newer than the last full refresh — the reason the series is wrong.
    splits: List[Split]
    last_full_refresh_at: Optional[datetime]

    @property
    def needs_refresh(self) -> bool:
        return bool(self.splits)

    def describe(self) -> str:
        when = (
            self.last_full_refresh_at.date().isoformat()
            if self.last_full_refresh_at
            else "never"
        )
        events = ", ".join(
            f"{s.ratio:g}:1 on {s.date.date().isoformat()}" for s in self.splits
        )
        return (
            f"{self.symbol}: {events} — series last restated {when}, so bars "
            "before and after are adjusted to different dates."
        )


def splits_since(
    all_splits: Sequence[Split], since: Optional[datetime]
) -> List[Split]:
    """
    Splits that postdate the last full refresh.

    `since=None` means the series has never been restated, in which case only
    splits are reported that could plausibly straddle stored data — all of
    them, since we cannot know what the bars were adjusted against.
    """
    if since is None:
        return list(all_splits)
    return [s for s in all_splits if s.date > since]


def detect_drift(
    symbol: str,
    last_full_refresh_at: Optional[datetime],
    fetch_splits: Callable[[str], List[Split]],
) -> DriftReport:
    """
    Whether `symbol`'s stored series is adjusted to a stale as-of date.

    `fetch_splits` is injected so this is testable without the network.
    """
    try:
        all_splits = fetch_splits(symbol)
    except Exception as exc:  # noqa: BLE001 — a provider failure is not drift
        logger.warning("Could not read splits for %s: %s", symbol, exc)
        all_splits = []

    return DriftReport(
        symbol=symbol,
        splits=splits_since(all_splits, last_full_refresh_at),
        last_full_refresh_at=last_full_refresh_at,
    )


def looks_delisted(
    newest_bar: Optional[datetime],
    fetched_rows: int,
    now: Optional[datetime] = None,
) -> bool:
    """
    True when a fetch returning nothing means "no longer trades" rather than
    "already current".

    Both cases report `written: 0`, which is why eleven acquired or
    taken-private names (ANSS, BK, CTRA, DAY, FI, HES, HOLX, IPG, K, MMC, WBA)
    sat at 2025-07-15 after a full run looking exactly like healthy symbols.

    Deliberately conservative: it requires BOTH an empty fetch and a newest bar
    well in the past. A symbol that is genuinely current fetches nothing too,
    but its newest bar is recent.
    """
    if fetched_rows > 0:
        return False
    if newest_bar is None:
        # Never had a bar at all: that is "not backfilled", not "delisted".
        return False
    now = now or datetime.now(timezone.utc)
    if newest_bar.tzinfo is None:
        newest_bar = newest_bar.replace(tzinfo=timezone.utc)
    return (now - newest_bar) > STALE_AFTER


def yfinance_splits(symbol: str) -> List[Split]:
    """Real split history. The network call this module keeps injectable."""
    import yfinance as yf

    series = yf.Ticker(symbol).splits
    if series is None or len(series) == 0:
        return []
    return [
        Split(
            date=(
                stamp.to_pydatetime().astimezone(timezone.utc)
                if stamp.tzinfo
                else stamp.to_pydatetime().replace(tzinfo=timezone.utc)
            ),
            ratio=float(ratio),
        )
        for stamp, ratio in series.items()
    ]
