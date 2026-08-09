"""
core/corporate_actions.py — detecting drift and delistings.

No network: split history is injected.

The bug these rules exist for, measured on 2026-08-09 before the fix:
NFLX closed 1260.27 on 2025-07-15 and 125.03 on 2025-07-16, because a 10:1
split in November 2025 had been applied only to bars fetched after it. Both
segments WERE adjusted — just to different as-of dates. Fourteen S&P names
were affected and nothing reported a problem.

Phase 5 — corporate actions
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.corporate_actions import (
    STALE_AFTER,
    Split,
    detect_drift,
    looks_delisted,
    splits_since,
)

NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)
MIGRATION = datetime(2025, 7, 15, tzinfo=timezone.utc)

NFLX_SPLIT = Split(date=datetime(2025, 11, 17, tzinfo=timezone.utc), ratio=10.0)
OLD_SPLIT = Split(date=datetime(2015, 7, 15, tzinfo=timezone.utc), ratio=7.0)


# ---------------------------------------------------------------------------
# splits_since
# ---------------------------------------------------------------------------

def test_a_split_after_the_last_refresh_is_drift():
    assert splits_since([OLD_SPLIT, NFLX_SPLIT], MIGRATION) == [NFLX_SPLIT]


def test_a_split_before_the_last_refresh_is_not():
    """
    The refresh already restated the whole series for it — which is why NVDA's
    2024 split and AAPL's 2020 split show no discontinuity in the migrated
    history: that segment was backfilled after them.
    """
    assert splits_since([OLD_SPLIT], NOW) == []


def test_a_split_exactly_at_the_refresh_is_not_drift():
    assert splits_since([Split(date=MIGRATION, ratio=2.0)], MIGRATION) == []


def test_never_refreshed_reports_every_split():
    """
    With no refresh timestamp there is no way to know what the bars were
    adjusted against, so nothing can be ruled out.
    """
    assert splits_since([OLD_SPLIT, NFLX_SPLIT], None) == [OLD_SPLIT, NFLX_SPLIT]


def test_no_splits_is_no_drift():
    assert splits_since([], MIGRATION) == []


# ---------------------------------------------------------------------------
# detect_drift
# ---------------------------------------------------------------------------

def test_drift_is_reported_with_the_offending_splits():
    report = detect_drift("NFLX", MIGRATION, lambda _s: [NFLX_SPLIT])
    assert report.needs_refresh is True
    assert report.splits == [NFLX_SPLIT]


def test_a_clean_symbol_needs_no_refresh():
    report = detect_drift("KO", MIGRATION, lambda _s: [OLD_SPLIT])
    assert report.needs_refresh is False


def test_the_description_names_the_split_and_the_refresh_date():
    detail = detect_drift("NFLX", MIGRATION, lambda _s: [NFLX_SPLIT]).describe()
    assert "10:1 on 2025-11-17" in detail
    assert "2025-07-15" in detail


def test_a_provider_failure_is_not_reported_as_drift():
    """
    Being unable to READ splits is not evidence that any occurred. Treating it
    as drift would send a caller into an expensive full backfill for nothing.
    """
    def explode(_symbol):
        raise RuntimeError("upstream down")

    assert detect_drift("NFLX", MIGRATION, explode).needs_refresh is False


# ---------------------------------------------------------------------------
# looks_delisted
# ---------------------------------------------------------------------------

def test_an_empty_fetch_with_a_stale_newest_bar_is_delisted():
    """
    Eleven symbols — ANSS, BK, CTRA, DAY, FI, HES, HOLX, IPG, K, MMC, WBA —
    sat at 2025-07-15 after a full run, all acquired or taken private, all
    reporting exactly what an up-to-date symbol reports.
    """
    assert looks_delisted(MIGRATION, fetched_rows=0, now=NOW) is True


def test_an_empty_fetch_with_a_RECENT_newest_bar_is_just_current():
    """The far more common case; it must not be labelled delisted."""
    recent = NOW - timedelta(days=2)
    assert looks_delisted(recent, fetched_rows=0, now=NOW) is False


def test_a_symbol_that_returned_rows_is_never_delisted():
    assert looks_delisted(MIGRATION, fetched_rows=250, now=NOW) is False


def test_a_symbol_with_no_bars_at_all_is_not_delisted():
    """That is "never backfilled", which is a different problem."""
    assert looks_delisted(None, fetched_rows=0, now=NOW) is False


def test_the_threshold_is_applied_at_its_boundary():
    just_inside = NOW - STALE_AFTER + timedelta(days=1)
    just_outside = NOW - STALE_AFTER - timedelta(days=1)
    assert looks_delisted(just_inside, 0, NOW) is False
    assert looks_delisted(just_outside, 0, NOW) is True


def test_a_naive_timestamp_is_treated_as_utc():
    naive = MIGRATION.replace(tzinfo=None)
    assert looks_delisted(naive, fetched_rows=0, now=NOW) is True
