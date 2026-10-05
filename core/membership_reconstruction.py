"""
core/membership_reconstruction.py

Read index member lists out of PAST revisions of a Wikipedia page.

Pure parsing and judgement only: no network and no database, so every rule
here is testable. scripts/reconstruct_sp500_membership.py does the fetching
and the writing.

WHY REVISIONS AND NOT THE "SELECTED CHANGES" TABLE
    The S&P 500 page used to carry a dated table of additions and removals,
    and walking it backward from today's list is the obvious reconstruction.
    Two things are wrong with it. The table was deleted from the live page
    between 2026-07-21 and 2026-08-19, so it is no longer a source that can be
    re-read. And it records each change under the ticker of the day, so a
    company renamed AFTER it joined (added as SATS, listed today as ECHO)
    cannot be removed again on the way back and is carried into every earlier
    year as a member it never was.

    A past revision has neither problem: it is the full list, under the
    tickers in use at the time. Measured 2026-10-04, the two methods agree at
    twelve year-ends apart from that rename class, and the revisions agree
    with Clenow's independent 1996-2019 file apart from renames.

WHAT CAN GO WRONG WITH A REVISION
    Anyone can edit the page, so the revision current at some instant may be
    vandalised, half-edited, or laid out differently. `judge` is the guard,
    and it has the same job as `_implausible` in data_pipeline/
    dynamic_universe.py: a wrong list that looks healthy is the dangerous
    case, because once stored it becomes the answer for that month.
"""

from __future__ import annotations

import io
import re
from datetime import date
from typing import Optional, Sequence

import pandas as pd

#: BRK.B, BF.B and plain tickers. Anything else in the symbol column means the
#: wrong table or a mangled row.
_TICKER = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


def parse_constituents(html: str) -> list[str]:
    """
    The symbols in the constituents table of one revision, in page order.

    The table is found by its symbol column, not by position or id: the
    header was "Ticker symbol" until 2018 and "Symbol" after, and old
    revisions have no `id="constituents"`. The FIRST matching table is the
    constituents; the changes table's headers are "Added"/"Removed" pairs and
    never match.

    Returns [] when no table matches. Never raises on a page with no tables.
    """
    try:
        # lxml named explicitly: left to choose, pandas answers a page with no
        # tables by retrying under html5lib, which is not installed, and the
        # blanked-page case raises ImportError instead of "No tables found".
        tables = pd.read_html(io.StringIO(html), flavor="lxml")
    except ValueError:  # "No tables found"
        return []

    for table in tables:
        for column in table.columns:
            if isinstance(column, str) and column.strip().lower() in (
                "symbol",
                "ticker symbol",
            ):
                return [
                    str(value).strip().upper()
                    for value in table[column].tolist()
                    if str(value).strip() and str(value).lower() != "nan"
                ]
    return []


def judge(
    symbols: Sequence[str],
    previous: Optional[Sequence[str]],
    expected_range: tuple[int, int],
    max_change: int,
) -> Optional[str]:
    """
    Why this member list should be rejected, or None when it is usable.

    `previous` is the last ACCEPTED list (the month before), or None for the
    first month. The month-over-month test is what catches a revision whose
    count is right and whose contents are not; without a previous list only
    the shape tests apply, which is why the first month is worth reading by
    eye.
    """
    low, high = expected_range
    if not low <= len(symbols) <= high:
        return f"{len(symbols)} symbols, expected {low}-{high}"

    if len(set(symbols)) != len(symbols):
        seen: set[str] = set()
        repeated = sorted({s for s in symbols if s in seen or seen.add(s)})
        return f"repeated symbols: {', '.join(repeated[:5])}"

    malformed = [s for s in symbols if not _TICKER.match(s)]
    if malformed:
        return f"not tickers: {', '.join(malformed[:5])}"

    if previous is not None:
        changed = len(set(symbols) ^ set(previous))
        if changed > max_change:
            return (
                f"{changed} symbols differ from the previous month, "
                f"more than {max_change}"
            )
    return None


def month_ends(start: date, end: date) -> list[date]:
    """Every calendar month-end from `start` to `end`, both inclusive."""
    stamps = pd.date_range(start=start, end=end, freq="ME")
    return [stamp.date() for stamp in stamps]
