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
    series was restated, and which the provider no longer resolves.
    Does not: fix anything. The fix is a full backfill, which is a write the
    caller should choose to make. Nor does it say WHY a symbol stopped
    resolving — see `looks_unresolved`.

    Renames are NOT handled, and on 2026-08-09 that gap bit: BK→BNY, FI→FISV
    and MMC→MRSH were each flagged as delisted and lost 13 months of bars.
    (Note FI/FISV runs both ways — Fiserv went FISV→FI in 2023 and back to
    FISV in 2025. A rename is not one-way or permanent.)

    Renames ARE now detected — see the successor section at the foot of this
    module. The constant-ratio test confirms a candidate, and index membership
    churn generates one: a symbol that left the index and a symbol that joined
    it are a pair worth testing. That closes the "generating the candidate"
    gap the 2026-08-09 research left open, for the case where the successor
    joins an index we snapshot. A company that changes ticker without ever
    touching a tracked index is still invisible.

    What the test does NOT do is find mergers. A rename is one instrument
    re-keyed; a stock-for-stock acquisition genuinely ends the target. Both
    look like "the provider stopped serving this ticker", and only the first
    has a successor series. The distinction, and the merger-arbitrage false
    positive that makes the threshold tight, are documented below.

Pure of FastAPI and of the database, so the rules can be tested directly.

Phase 5 — corporate actions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, List, Mapping, Optional, Sequence

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


def looks_unresolved(
    newest_bar: Optional[datetime],
    fetched_rows: int,
    now: Optional[datetime] = None,
) -> bool:
    """
    True when a fetch returning nothing means "the provider no longer serves
    this symbol" rather than "already current".

    WHAT THIS DOES NOT TELL YOU: why. An unresolved symbol may be delisted, or
    merely RENAMED. This function cannot distinguish them, and was previously
    named `looks_delisted`, which asserted the stronger claim its evidence
    could not support.

    Measured 2026-08-09, the control that settles it — FRCB (seized 2023,
    trading as a $0.0004 shell) returns 1255 rows from yfinance, while SIVBQ
    (same event class) returns zero, and BK returned zero while being a live
    S&P 500 constituent. Yahoo `/v8/finance/chart/BK` 404s byte-identically to
    TWTR. So availability tracks whether the SYMBOL KEY still exists, not what
    happened to the issuer. An empty result is a string-lookup miss, and a
    corporate fact must not be inferred from one.

    That mistake was live here: of the eleven symbols this flagged, eight were
    genuinely delisted (ANSS 2025-07-17, HES 2025-07-18, WBA 2025-08-28,
    IPG 2025-11-26, K 2025-12-11, DAY 2026-02-04, HOLX 2026-04-07,
    CTRA 2026-05-19 — each confirmed against a Form 25-NSE, Form 15 or
    exchange notice) but three were RENAMES: BK→BNY, FI→FISV, MMC→MRSH. Those
    three are live constituents that sat 13 months without bars, marked dead.
    See `research/delisting-findings-2026-08-09.md`.

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


# ---------------------------------------------------------------------------
# Successors — renames, and why they are not the same thing as mergers
# ---------------------------------------------------------------------------
#
# WHAT THIS SECTION DETECTS, precisely: a RENAME — one instrument whose price
# history the provider has re-keyed under a new ticker. It shows up as a
# CONSTANT multiplicative offset between the old stored series and the new
# fetched one, because both describe the same stream of prices adjusted to two
# different as-of dates.
#
# It does NOT detect mergers in general, and conflating the two is the mistake
# this module has already paid for once:
#
#   rename (BK->BNY, EQR->VMRK)     one instrument, history re-keyed.  FOUND.
#   cash acquisition (HES)          no successor series exists.        Correctly
#                                   a delisting; nothing to find.
#   stock-for-stock (AVB into VMRK) the target's series ENDS. The acquirer's
#                                   series is not a multiple of the target's.
#                                   Correctly not a successor — the target
#                                   really did stop existing.
#   merger into a brand-new ticker  both predecessors end. Nothing to find,
#                                   and that is the right answer, not a bug.
#
# THE FALSE POSITIVE THAT FORCES A TIGHT THRESHOLD. Measured 2026-09-15 on the
# AVB/EQR combination: in the weeks before a stock-for-stock deal closes,
# merger arbitrage pins the target to the acquirer at very nearly the exchange
# ratio. AVB vs VMRK over the last 20 days before the close held a ratio of
# ~2.7928 with a spread of 4.6e-03 — tight enough to fool a loose threshold,
# and only ~3x better than the BK<->JPM control. Over a full year the same
# pair spreads to 2.1e-01 and never once matches to the cent.
#
# So the separation is still there, but it is between 1e-7 and 5e-3, not
# between 1e-7 and 1e-2 as the 2026-08-09 research assumed:
#
#   same instrument   BK<->BNY 1.65e-07   FI<->FISV 0.0
#                     MMC<->MRSH 1.67e-07 EQR<->VMRK 0.0
#   NOT the same      AVB<->VMRK 4.6e-03  <-- merger arb, the near miss
#                     MMC<->AON  1.28e-02  BK<->JPM  2.16e-02
#
# 1e-4 sits an order of magnitude below the near miss and three above the
# worst true match.
#
# WHY A SHORT WINDOW. The old series may itself straddle two adjustment
# regimes (that is the drift this module's first half exists to find), and a
# long window then reports a spread that describes OUR bookkeeping rather than
# the market. Measured: EQR vs VMRK is 0.0 over the last 20 clean days and
# 4.5e-02 over 405 days, for the same pair, because a dividend re-adjustment
# falls in the middle. Compare over a short window ending at the old series'
# last trustworthy bar.

#: Relative spread at or below which two series are one instrument. See the
#: measured table above before loosening this.
SUCCESSOR_MAX_SPREAD = 1e-4

#: Bars to compare, taken from the END of the overlap.
SUCCESSOR_WINDOW = 20

#: Below this many overlapping bars the statistic is not evidence of anything:
#: a spread over two points is ~0 for any two series that happen to cross.
SUCCESSOR_MIN_OVERLAP = 15


@dataclass(frozen=True)
class SuccessorEvidence:
    """One old-symbol/candidate comparison. Evidence, not a verdict."""

    old_symbol: str
    candidate: str
    overlap: int
    #: Median old/new over the window. None when there was too little overlap.
    ratio: Optional[float] = None
    #: (max - min) / median of that ratio. None when there was too little overlap.
    spread: Optional[float] = None
    #: Whether the caller supplied an event date, so post-event bars were cut.
    #: Without one the statistic cannot be acted on — see `proposable`.
    trimmed: bool = False

    @property
    def enough_overlap(self) -> bool:
        return self.overlap >= SUCCESSOR_MIN_OVERLAP and self.spread is not None

    @property
    def same_instrument(self) -> bool:
        """The statistic alone. Necessary for a rename, NOT sufficient."""
        return self.enough_overlap and self.spread <= SUCCESSOR_MAX_SPREAD

    @property
    def proposable(self) -> bool:
        """
        Safe to put to a human as a rename.

        Requires the tail to have been trimmed at a known event date, because
        an UNTRIMMED comparison inverts once the contamination outlasts the
        window: every bar in it is then the successor's price under both
        tickers, so old == new exactly, the ratio is 1.000000 and the spread is
        0.0 — perfect-looking evidence that an ACQUIRED company was merely
        renamed. A real rename (EQR->VMRK) scores identically, so the ratio
        cannot separate them. Only knowing when the event happened can.
        """
        return self.same_instrument and self.trimmed

    def describe(self) -> str:
        if not self.enough_overlap:
            return (
                f"{self.old_symbol} -> {self.candidate}: only {self.overlap} "
                f"overlapping bars, need {SUCCESSOR_MIN_OVERLAP}. No evidence "
                "either way."
            )
        if self.same_instrument:
            verdict = (
                "SAME INSTRUMENT"
                if self.trimmed
                else "matches, but UNVERIFIED — no event date, tail not trimmed"
            )
        else:
            verdict = "different"
        return (
            f"{self.old_symbol} -> {self.candidate}: ratio {self.ratio:.6f}, "
            f"spread {self.spread:.2e} over {self.overlap} bars — {verdict}."
        )


def _relative_spread(values: Sequence[float]) -> Optional[float]:
    """(max - min) / median. None when the median is not usable as a scale."""
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    median = (
        ordered[middle]
        if len(ordered) % 2
        else (ordered[middle - 1] + ordered[middle]) / 2
    )
    if median <= 0:
        return None
    return (ordered[-1] - ordered[0]) / median


def compare_series(
    old_symbol: str,
    candidate: str,
    old_closes: Mapping[date, float],
    new_closes: Mapping[date, float],
    before: Optional[date] = None,
    window: int = SUCCESSOR_WINDOW,
) -> SuccessorEvidence:
    """
    Whether `candidate`'s series is `old_symbol`'s, re-keyed.

    `before` drops bars on or after a date — use the day the symbol left the
    index. Bars a provider serves under a dead ticker after a corporate action
    are not that ticker's prices: measured 2026-09-15, stored AVB carried
    VMRK's post-merger closes from 2026-08-17, and stored EQR simply repeated
    its last close four times. Including either turns a clean comparison into
    noise.
    """
    shared = set(old_closes) & set(new_closes)
    if before is not None:
        shared = {d for d in shared if d < before}
    # The END of the overlap: closest to the event, and least likely to
    # straddle an adjustment boundary in the stored series.
    dates = sorted(shared)[-window:]

    ratios = [
        old_closes[d] / new_closes[d]
        for d in dates
        if old_closes[d] > 0 and new_closes[d] > 0
    ]
    trimmed = before is not None
    if len(ratios) < SUCCESSOR_MIN_OVERLAP:
        return SuccessorEvidence(
            old_symbol, candidate, overlap=len(ratios), trimmed=trimmed
        )

    spread = _relative_spread(ratios)
    ordered = sorted(ratios)
    middle = len(ordered) // 2
    median = (
        ordered[middle]
        if len(ordered) % 2
        else (ordered[middle - 1] + ordered[middle]) / 2
    )
    return SuccessorEvidence(
        old_symbol,
        candidate,
        overlap=len(ratios),
        ratio=median,
        spread=spread,
        trimmed=trimmed,
    )


def rank_successors(
    old_symbol: str,
    old_closes: Mapping[date, float],
    candidates: Mapping[str, Mapping[date, float]],
    before: Optional[date] = None,
) -> List[SuccessorEvidence]:
    """
    Every candidate scored, tightest spread first.

    Returns ALL of them, including the ones that fail — a near miss is worth
    seeing, because that is what a pending stock-for-stock merger looks like
    and it is the thing most likely to be mistaken for a rename.
    """
    scored = [
        compare_series(old_symbol, name, old_closes, closes, before=before)
        for name, closes in candidates.items()
    ]
    # Unusable comparisons sort last rather than first.
    return sorted(
        scored, key=lambda e: e.spread if e.spread is not None else float("inf")
    )


# ---------------------------------------------------------------------------
# Generating the candidate — the half the 2026-08-09 research left open
# ---------------------------------------------------------------------------
#
# The ratio test confirms a successor; it cannot discover one. BK->BNY,
# FI->FISV and MMC->MRSH were all found by hand, by company-name search.
#
# Index membership churn generates candidates automatically, because the event
# that renames a ticker is usually the same event that changes the index. A
# symbol that STOPPED appearing in the snapshots and a symbol that STARTED
# appearing are a pair worth testing. Measured 2026-09-15: since snapshots
# began, exactly two names left the S&P 500 (AVB, EQR, both last seen
# 2026-08-17) and exactly two joined (RDDT, VMRK, both 2026-08-25) — four
# comparisons, one of which (EQR->VMRK) is the rename.
#
# This inherits the limits of `universe_membership`: it only knows what has
# been snapshotted, from 2026-08-09 forward, for the indexes we track. A
# rename outside those indexes stays invisible, and so does one that predates
# the first snapshot.


@dataclass(frozen=True)
class Membership:
    """One row of point-in-time index membership."""

    symbol: str
    first_seen: datetime
    last_seen: datetime


@dataclass(frozen=True)
class IndexChurn:
    #: Present at the first snapshot, absent from the latest — left the index.
    left: List[str]
    #: Absent at the first snapshot, present later — joined the index.
    joined: List[str]


def index_churn(rows: Sequence[Membership]) -> IndexChurn:
    """
    Which symbols entered and left, judged against the snapshot window itself.

    `last_seen` is compared to the newest `last_seen` across all rows rather
    than to "today", so a day the snapshot did not run does not read as the
    whole index delisting at once.
    """
    if not rows:
        return IndexChurn(left=[], joined=[])
    newest = max(r.last_seen for r in rows)
    oldest = min(r.first_seen for r in rows)
    return IndexChurn(
        left=sorted(r.symbol for r in rows if r.last_seen < newest),
        joined=sorted(r.symbol for r in rows if r.first_seen > oldest),
    )


def normalize_symbol(symbol: str) -> str:
    """
    Index scrapes spell class shares with a dot, this project with a dash.

    Wikipedia lists BRK.B and BF.B; the registry and Yahoo both use BRK-B and
    BF-B. Without this, every class share reads as a symbol missing from the
    registry and so as a successor candidate — two guaranteed false leads on
    every run.
    """
    return symbol.strip().upper().replace(".", "-")


#: A hole longer than this means the series stopped and something later resumed
#: under the same ticker. Well past any holiday or trading suspension, so it
#: fires on reassignment rather than on a quiet month.
MAX_CONTINUOUS_HOLE = 30


def max_gap(days: Sequence[date]) -> tuple[int, Optional[date]]:
    """
    Longest run of calendar days with no bar, and the date it ends on.

    WHY THIS IS THE SIGNAL. A dead ticker often keeps returning bars — they
    just belong to whatever inherited the symbol. Comparing our tail against
    the provider's cannot catch that, because once the wrong bars are ingested
    the two agree perfectly. Measured 2026-09-17: stored PARA and live PARA
    matched exactly, and both were the wrong company. The hole is what gives it
    away — real bars to 2025-07-15, 388 days of nothing, then an unrelated
    penny stock from 2026-08-07.

    Must be computed over the WHOLE series. PARA's hole ends outside any
    recent comparison window, so a windowed version sees a tidy series and
    misses the only fact that matters.
    """
    worst, at = 0, None
    for earlier, later in zip(days, list(days)[1:]):
        span = (later - earlier).days
        if span > worst:
            worst, at = span, later
    return worst, at
