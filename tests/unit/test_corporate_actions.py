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
    looks_unresolved,
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
# looks_unresolved
# ---------------------------------------------------------------------------

def test_an_empty_fetch_with_a_stale_newest_bar_is_unresolved():
    """
    Eleven symbols — ANSS, BK, CTRA, DAY, FI, HES, HOLX, IPG, K, MMC, WBA —
    sat at 2025-07-15 after a full run, all reporting exactly what an
    up-to-date symbol reports.

    They were NOT all "acquired or taken private", as this test used to claim.
    Eight were delisted; BK, FI and MMC were renamed to BNY, FISV and MRSH and
    are live S&P 500 constituents. Hence `unresolved`, not `delisted` — see
    test_unresolved_does_not_mean_delisted below.
    """
    assert looks_unresolved(MIGRATION, fetched_rows=0, now=NOW) is True


def test_unresolved_does_not_mean_delisted():
    """
    The distinction this function CANNOT make, pinned so the name is not
    quietly strengthened back into a claim about corporate status.

    A rename and a delisting are indistinguishable here: both produce an empty
    fetch against a stale bar. BK (renamed to BNY, still trading) and HES
    (genuinely delisted 2025-07-18) are the same input to this function, so
    they must produce the same output — the caller has to disambiguate.
    """
    renamed_but_live = MIGRATION      # BK, which became BNY
    genuinely_delisted = MIGRATION    # HES, Form 25-NSE 2025-07-18
    assert (
        looks_unresolved(renamed_but_live, fetched_rows=0, now=NOW)
        is looks_unresolved(genuinely_delisted, fetched_rows=0, now=NOW)
        is True
    )


def test_an_empty_fetch_with_a_RECENT_newest_bar_is_just_current():
    """The far more common case; it must not be labelled unresolved."""
    recent = NOW - timedelta(days=2)
    assert looks_unresolved(recent, fetched_rows=0, now=NOW) is False


def test_a_symbol_that_returned_rows_is_never_unresolved():
    assert looks_unresolved(MIGRATION, fetched_rows=250, now=NOW) is False


def test_a_symbol_with_no_bars_at_all_is_not_unresolved():
    """That is "never backfilled", which is a different problem."""
    assert looks_unresolved(None, fetched_rows=0, now=NOW) is False


def test_the_threshold_is_applied_at_its_boundary():
    just_inside = NOW - STALE_AFTER + timedelta(days=1)
    just_outside = NOW - STALE_AFTER - timedelta(days=1)
    assert looks_unresolved(just_inside, 0, NOW) is False
    assert looks_unresolved(just_outside, 0, NOW) is True


def test_a_naive_timestamp_is_treated_as_utc():
    naive = MIGRATION.replace(tzinfo=None)
    assert looks_unresolved(naive, fetched_rows=0, now=NOW) is True


# ---------------------------------------------------------------------------
# Successor detection — renames
#
# No live fixtures are possible here. BK/FI/MMC were repaired by renaming the
# asset rows in place on 2026-08-10, so the pre-rename stored bars are gone and
# yfinance 404s the old tickers. The series below are synthetic; the MAGNITUDES
# they assert against are the measured ones from the module docstring.
# ---------------------------------------------------------------------------

from datetime import date

from core.corporate_actions import (  # noqa: E402
    MAX_CONTINUOUS_HOLE,
    SUCCESSOR_MIN_OVERLAP,
    IndexChurn,
    Membership,
    compare_series,
    index_churn,
    max_gap,
    normalize_symbol,
    rank_successors,
)

DAY0 = date(2026, 7, 1)


def series(values, start=DAY0):
    """Closes keyed by consecutive dates."""
    return {start + timedelta(days=i): v for i, v in enumerate(values)}


#: A plausible price path, long enough to clear the overlap floor.
WALK = [100.0, 101.5, 99.8, 103.2, 102.1, 104.7, 103.9, 105.6, 104.2, 106.8,
        107.3, 105.1, 108.4, 109.2, 107.6, 110.1, 111.5, 109.8, 112.3, 113.0]


def test_a_renamed_series_is_one_instrument():
    """
    The whole point: old = new * k for a constant k is a rename. k=1.022846 is
    the measured BK->BNY factor.
    """
    new = series(WALK)
    old = series([v * 1.022846 for v in WALK])
    evidence = compare_series("BK", "BNY", old, new)
    assert evidence.same_instrument
    assert evidence.ratio == pytest.approx(1.022846, rel=1e-9)
    assert evidence.spread < 1e-6


def test_an_identical_series_is_one_instrument():
    """EQR->VMRK measured 0.0 exactly — Yahoo re-keyed the series untouched."""
    evidence = compare_series("EQR", "VMRK", series(WALK), series(WALK))
    assert evidence.same_instrument
    assert evidence.spread == 0.0


def test_two_different_stocks_are_not():
    other = series([v * 1.02 + (i % 5) * 0.9 for i, v in enumerate(WALK)])
    evidence = compare_series("BK", "JPM", series(WALK), other)
    assert not evidence.same_instrument


def test_merger_arbitrage_does_not_read_as_a_rename():
    """
    The false positive that sets the threshold. Before a stock-for-stock deal
    closes, arbitrage pins the target to the acquirer at nearly the exchange
    ratio — AVB vs VMRK held ~2.7928 with a spread of 4.6e-03, tight enough to
    fool a 1e-2 threshold. It must still read as `different`, and the near miss
    must remain visible rather than being rounded to zero.
    """
    acquirer = series(WALK)
    # 2.7928 with a residual basis that wanders by a few tenths of a percent.
    target = series(
        [v * 2.7928 * (1 + 0.0018 * ((i % 7) - 3) / 3) for i, v in enumerate(WALK)]
    )
    evidence = compare_series("AVB", "VMRK", target, acquirer)
    assert not evidence.same_instrument
    assert 1e-4 < evidence.spread < 1e-2  # the measured near-miss band
    assert evidence.ratio == pytest.approx(2.7928, rel=1e-3)


def test_too_little_overlap_is_not_evidence():
    """
    A spread over two points is ~0 for any two series, so a short overlap must
    refuse to answer rather than confirm everything put to it.
    """
    two_days = {DAY0: 10.0, DAY0 + timedelta(days=1): 11.0}
    other = {DAY0: 20.0, DAY0 + timedelta(days=1): 22.0}  # exactly 0.5x
    evidence = compare_series("A", "B", two_days, other)
    assert evidence.overlap < SUCCESSOR_MIN_OVERLAP
    assert not evidence.enough_overlap
    assert not evidence.same_instrument
    assert evidence.spread is None


def test_bars_after_the_event_are_excluded_by_before():
    """
    A provider serves the SUCCESSOR's prices under the dead ticker: stored AVB
    carried VMRK's closes from 2026-08-17. Those bars must not enter the
    comparison — with them the rename above stops being detectable.
    """
    new = series(WALK)
    contaminated = dict(series([v * 1.022846 for v in WALK]))
    event = DAY0 + timedelta(days=len(WALK))
    for i in range(6):  # six bars of the successor's prices, unscaled
        contaminated[event + timedelta(days=i)] = 200.0 + i
        new[event + timedelta(days=i)] = 200.0 + i

    assert not compare_series("AVB", "VMRK", contaminated, new).same_instrument
    assert compare_series(
        "AVB", "VMRK", contaminated, new, before=event
    ).same_instrument


def test_ranking_puts_the_tightest_first_and_keeps_the_misses():
    old = series([v * 1.022846 for v in WALK])
    evidence = rank_successors(
        "BK",
        old,
        {
            "JPM": series([v * 1.02 + (i % 5) * 0.9 for i, v in enumerate(WALK)]),
            "BNY": series(WALK),
            "SHORT": {DAY0: 1.0},
        },
    )
    assert [e.candidate for e in evidence][:2] == ["BNY", "JPM"]
    assert evidence[-1].candidate == "SHORT"  # unusable sorts last, not first
    assert len(evidence) == 3


def test_zero_prices_are_dropped_not_divided_by():
    new = dict(series(WALK))
    old = dict(series([v * 2.0 for v in WALK]))
    new[DAY0] = 0.0
    evidence = compare_series("A", "B", old, new)
    assert evidence.overlap == len(WALK) - 1
    assert evidence.same_instrument


# ---------------------------------------------------------------------------
# index_churn — generating the candidate
# ---------------------------------------------------------------------------

def stamp(day):
    return datetime(2026, 8, day, tzinfo=timezone.utc)


def test_churn_names_the_leavers_and_the_joiners():
    """
    The measured 2026-09-15 shape: AVB and EQR stop appearing, VMRK starts.
    """
    rows = [
        Membership("AAPL", stamp(9), stamp(25)),
        Membership("AVB", stamp(9), stamp(17)),
        Membership("EQR", stamp(9), stamp(17)),
        Membership("VMRK", stamp(25), stamp(25)),
    ]
    assert index_churn(rows) == IndexChurn(left=["AVB", "EQR"], joined=["VMRK"])


def test_a_skipped_snapshot_is_not_a_mass_delisting():
    """
    `last_seen` is judged against the newest row, not against today. Snapshots
    cannot be backdated, so a day the job did not run must not read as every
    constituent leaving the index at once.
    """
    rows = [Membership(s, stamp(9), stamp(17)) for s in ("AAPL", "MSFT")]
    assert index_churn(rows) == IndexChurn(left=[], joined=[])


def test_churn_of_nothing_is_empty():
    assert index_churn([]) == IndexChurn(left=[], joined=[])


def test_class_shares_normalize_to_the_registry_spelling():
    """Wikipedia says BRK.B; the registry and Yahoo say BRK-B."""
    assert normalize_symbol("BRK.B") == "BRK-B"
    assert normalize_symbol(" bf.b ") == "BF-B"
    assert normalize_symbol("AAPL") == "AAPL"


def test_contamination_outlasting_the_window_fakes_a_perfect_match():
    """
    The false positive that `proposable` exists for, and the reason the
    statistic alone must never be acted on.

    When a provider serves the successor's prices under a dead ticker for
    LONGER than the comparison window, every bar in that window is the same
    price under both tickers. old == new exactly, ratio 1.000000, spread 0.0 —
    evidence indistinguishable from a real rename, for a company that was
    acquired outright. EQR->VMRK scores exactly this, so no threshold can
    separate them; only knowing the event date can.
    """
    new = dict(series(WALK))
    old = dict(series([v * 1.022846 for v in WALK]))
    event = DAY0 + timedelta(days=len(WALK))
    for i in range(len(WALK) + 5):  # contamination longer than SUCCESSOR_WINDOW
        day, price = event + timedelta(days=i), 200.0 + i
        old[day] = new[day] = price

    evidence = compare_series("ACQUIRED", "ACQUIRER", old, new)
    # The statistic really is fooled — that is the point.
    assert evidence.same_instrument
    assert evidence.spread == 0.0
    # But with no event date it must not be offered as a rename.
    assert not evidence.trimmed
    assert not evidence.proposable
    assert "UNVERIFIED" in evidence.describe()


def test_a_trimmed_match_is_proposable():
    """The same comparison, given the event date, is actionable."""
    new = dict(series(WALK))
    old = dict(series([v * 1.022846 for v in WALK]))
    event = DAY0 + timedelta(days=len(WALK))
    for i in range(len(WALK) + 5):
        day, price = event + timedelta(days=i), 200.0 + i
        old[day] = new[day] = price

    evidence = compare_series("BK", "BNY", old, new, before=event)
    assert evidence.proposable
    assert evidence.trimmed
    assert "SAME INSTRUMENT" in evidence.describe()


def test_a_trimmed_non_match_is_still_not_proposable():
    """`proposable` tightens `same_instrument`; it must not bypass it."""
    other = series([v * 1.02 + (i % 5) * 0.9 for i, v in enumerate(WALK)])
    evidence = compare_series(
        "AVB", "VMRK", series(WALK), other, before=DAY0 + timedelta(days=99)
    )
    assert evidence.trimmed
    assert not evidence.same_instrument
    assert not evidence.proposable


# ---------------------------------------------------------------------------
# max_gap — the only signal that catches a reassigned ticker
# ---------------------------------------------------------------------------

def test_a_long_hole_is_found_with_the_date_it_ends():
    """
    PARA's shape: real bars, a 388-day hole, then an unrelated instrument.
    Comparing tails cannot catch this — once the wrong bars are stored, ours
    and the provider's agree exactly. The hole is the evidence.
    """
    days = [date(2025, 7, 14), date(2025, 7, 15), date(2026, 8, 7), date(2026, 8, 10)]
    gap, at = max_gap(days)
    assert gap == 388
    assert at == date(2026, 8, 7)
    assert gap > MAX_CONTINUOUS_HOLE


def test_a_normal_series_has_no_meaningful_hole():
    """A weekend is three days; a long holiday weekend four. Neither fires."""
    days = [date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 14), date(2026, 9, 15)]
    gap, _ = max_gap(days)
    assert gap == 3
    assert gap <= MAX_CONTINUOUS_HOLE


def test_max_gap_of_a_short_series_is_empty():
    assert max_gap([]) == (0, None)
    assert max_gap([date(2026, 9, 10)]) == (0, None)
