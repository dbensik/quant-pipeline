"""
core/price_bounds.py
Is a stored series inside the range the asset has actually traded in?

WHY THIS TEST AND NOT A DAY-OVER-DAY THRESHOLD. Six rounds of crypto cleanup
were decided by comparing stored bars against the coin's published all-time
high and low, and nothing else came close. A multiplier on consecutive closes
cannot do this job:

    AAVE-USD  moved 102.9x in a day and is CORRECT — the 100:1 LEND->AAVE
              redenomination, a corporate action.
    USDE-USD  moved 2,121x and was fabricated — a dollar-pegged stablecoin
              whose true range is $0.929486-$1.034.

No single threshold separates those two, because the question is not "is this
move large" but "has this asset ever been worth that". Bounds answer it
directly, and they also catch the case a jump test structurally cannot see: a
WHOLLY WRONG series sitting at a plausible-looking level, with no jump in it at
all. That is exactly what the seventeen wrong assets were.

Pure of the network and the database. `scripts/audit_crypto_bounds.py` wires it
to CoinGecko and TimescaleDB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import List, Optional, Sequence, Tuple

#: Bars are judged against the published range widened by this fraction, so an
#: edge bar is not condemned for a rounding difference between two providers.
#: Needed in practice: after trimming, OP-USD's low was $0.0820 against a
#: published all-time low of $0.080689 — legitimate, and 1.6% outside it.
#: Every real violation found was 100x or more, so a few percent costs nothing.
BOUNDS_TOLERANCE = 0.05


class Bounds(str, Enum):
    #: Every bar is inside the published range.
    CLEAN = "clean"
    #: Violations form a contiguous PREFIX and real bars remain after it — two
    #: instruments spliced, the shape of TIA, OP and WLD. Trim the prefix.
    TRIM = "trim"
    #: Every bar violates. There is nothing to keep.
    UNUSABLE = "unusable"
    #: Violations exist but are scattered through the series, so no single cut
    #: separates them. USDE-USD's shape. Needs a human: removing scattered bars
    #: leaves SEAMS whose implied moves are still impossible.
    SCATTERED = "scattered"


@dataclass(frozen=True)
class CoinBounds:
    """The range an asset has actually traded in, per the reference source."""

    coingecko_id: str
    low: float
    high: float

    def widened(self, tolerance: float = BOUNDS_TOLERANCE) -> Tuple[float, float]:
        return self.low * (1 - tolerance), self.high * (1 + tolerance)


@dataclass
class BoundsReport:
    symbol: str
    verdict: Bounds
    total: int = 0
    outside: int = 0
    #: For TRIM: the first date whose bars are inside the range.
    valid_from: Optional[date] = None
    lowest: Optional[float] = None
    highest: Optional[float] = None
    examples: List[Tuple[date, float]] = field(default_factory=list)

    @property
    def pct_outside(self) -> float:
        return (100.0 * self.outside / self.total) if self.total else 0.0

    def describe(self) -> str:
        if self.verdict is Bounds.CLEAN:
            return f"{self.symbol}: clean — {self.total} bars all within range."
        head = (
            f"{self.symbol}: {self.verdict.value.upper()} — {self.outside} of "
            f"{self.total} bars ({self.pct_outside:.1f}%) outside the published "
            f"range"
        )
        if self.verdict is Bounds.TRIM:
            head += f"; all before {self.valid_from}, trim there"
        sample = ", ".join(f"{d}={p:g}" for d, p in self.examples[:3])
        return head + (f". e.g. {sample}" if sample else ".")


def _prices(bar: Tuple) -> List[float]:
    """Every price in a bar: (date, close) or (date, close, low, high).

    Zero and None are dropped rather than judged. A stub bar carries open=0
    and low=0, and a zero is an absent price, not a claim that the asset was
    worthless."""
    return [p for p in bar[1:] if p]


def check_bounds(
    symbol: str,
    bars: Sequence[Tuple],
    bounds: CoinBounds,
    tolerance: float = BOUNDS_TOLERANCE,
) -> BoundsReport:
    """
    Classify a stored series against the range the asset has really traded in.

    `bars` is `(date, close)` or `(date, close, low, high)`, in any order — it
    is sorted here, because judging "is this a prefix" against fetch order
    rather than date order would invent splices the series does not contain.

    PASS LOW AND HIGH WHEN YOU HAVE THEM. A close-only check cannot see a bar
    that STRADDLES a redenomination, and that bar is exactly the one left
    behind by a naive cut. Measured on AAVE-USD: 2020-10-03 opened at $0.5238
    on the old LEND basis and closed at $53.15 on the new AAVE basis — a
    124.7x intraday range. Its close is in range, so close-only judged it
    clean and proposed cutting at that very bar, which would have made a
    phantom 124.7x high/low the first bar of the series.
    """
    ordered = sorted(bars, key=lambda b: b[0])
    if not ordered:
        return BoundsReport(symbol, Bounds.CLEAN)

    low, high = bounds.widened(tolerance)
    flags = [
        any(not (low <= p <= high) for p in _prices(bar)) if _prices(bar) else False
        for bar in ordered
    ]
    outside = sum(flags)

    closes = [bar[1] for bar in ordered]
    report = BoundsReport(
        symbol=symbol,
        verdict=Bounds.CLEAN,
        total=len(ordered),
        outside=outside,
        lowest=min(closes),
        highest=max(closes),
        examples=[(bar[0], bar[1]) for bar, bad in zip(ordered, flags) if bad][:5],
    )
    if outside == 0:
        return report
    if outside == len(ordered):
        report.verdict = Bounds.UNUSABLE
        return report

    # A prefix means every violation precedes every survivor.
    last_bad = max(i for i, bad in enumerate(flags) if bad)
    first_good = min(i for i, bad in enumerate(flags) if not bad)
    if last_bad < first_good:
        report.verdict = Bounds.TRIM
        report.valid_from = ordered[first_good][0]
    else:
        report.verdict = Bounds.SCATTERED
    return report
