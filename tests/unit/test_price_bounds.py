"""
core/price_bounds.py — has this asset ever actually been worth that?

Every case below is a shape that really occurred during the 2026-09 crypto
cleanup, because the test this module has to pass is separating two things a
day-over-day threshold cannot: a 102.9x move that is CORRECT (the LEND->AAVE
redenomination) from a 2,121x move that was fabricated (a dollar-pegged
stablecoin).
"""

from datetime import date, timedelta

from core.price_bounds import (
    BOUNDS_TOLERANCE,
    Bounds,
    CoinBounds,
    check_bounds,
)

DAY0 = date(2024, 1, 1)


def series(values, start=DAY0):
    return [(start + timedelta(days=i), v) for i, v in enumerate(values)]


#: Celestia, measured: $0.279235 - $20.85.
CELESTIA = CoinBounds("celestia", 0.279235, 20.85)
#: Ethena USDe, measured: $0.929486 - $1.034.
USDE = CoinBounds("ethena-usde", 0.929486, 1.034)


def test_a_spliced_prefix_is_a_trim():
    """
    TIA-USD's shape: a micro-cap around a cent for years, then Celestia. The
    cut date is what the caller needs, so it is reported.
    """
    bars = series([0.007, 0.006, 0.007] + [3.0, 3.2, 2.9, 3.1])
    report = check_bounds("TIA-USD", bars, CELESTIA)
    assert report.verdict is Bounds.TRIM
    assert report.outside == 3
    assert report.valid_from == DAY0 + timedelta(days=3)
    assert "trim there" in report.describe()


def test_scattered_violations_are_not_a_trim():
    """
    USDE-USD's shape — corruption in every quarter. No single cut separates
    it, and removing the bad bars piecemeal leaves seams whose implied moves
    are still impossible, so this must refuse to propose a cut.
    """
    bars = series([1.0, 0.00002, 1.0, 1.0, 0.06, 1.0])
    report = check_bounds("USDE-USD", bars, USDE)
    assert report.verdict is Bounds.SCATTERED
    assert report.valid_from is None


def test_a_wholly_wrong_series_is_unusable():
    """The seventeen wrong assets: every bar belongs to another coin."""
    report = check_bounds("UNI-USD", series([0.0001] * 5), CELESTIA)
    assert report.verdict is Bounds.UNUSABLE
    assert report.outside == report.total


def test_a_real_redenomination_is_NOT_flagged():
    """
    THE case that justifies this module over a jump threshold. AAVE moved
    102.9x in one day on the 100:1 LEND->AAVE conversion. Both sides of that
    move are inside the asset's real range, so bounds say clean — while any
    day-over-day rule large enough to catch USDe's 2,121x also condemns this.
    """
    aave = CoinBounds("aave", 25.0, 660.0)
    bars = series([0.5 * 103, 55.0, 60.0, 58.0])  # post-conversion prices
    assert check_bounds("AAVE-USD", bars, aave).verdict is Bounds.CLEAN


def test_a_wrong_series_at_a_plausible_level_is_still_caught():
    """
    The failure a jump test structurally CANNOT see: a different coin's series
    with no jump in it at all, sitting at a level the real asset never traded.
    """
    bars = series([0.05, 0.051, 0.049, 0.052])  # smooth, and far below Celestia
    assert check_bounds("SMOOTH", bars, CELESTIA).verdict is Bounds.UNUSABLE


def test_an_edge_bar_just_BELOW_the_published_low_is_tolerated():
    """
    Two providers do not agree to the last decimal, so a bar fractionally
    outside the published range must not be condemned — that would undo a
    correct repair. 0.0799 is 1.0% below Optimism's published all-time low.

    Deliberately BELOW the bound: an earlier version of this test used a bar
    ABOVE it, which is inside the range whether or not a tolerance exists, so
    it proved nothing. Setting BOUNDS_TOLERANCE to 0 left it passing.
    """
    op = CoinBounds("optimism", 0.080689, 4.84)
    assert 0.0799 < op.low
    assert check_bounds("OP-USD", series([0.0799, 1.0, 2.0]), op).verdict is (
        Bounds.CLEAN
    )


def test_the_tolerance_is_not_a_blank_cheque():
    """A bar an order of magnitude out is still a violation."""
    op = CoinBounds("optimism", 0.080689, 4.84)
    assert check_bounds("OP-USD", series([0.0004, 1.0, 2.0]), op).verdict is (
        Bounds.TRIM
    )
    assert BOUNDS_TOLERANCE < 0.2


def test_bars_are_judged_in_date_order():
    """
    Fetch order must not invent a splice. Shuffled, these are still a prefix.
    """
    bars = series([3.0, 3.2, 0.007, 0.006])
    shuffled = [bars[2], bars[3], bars[0], bars[1]]  # bad ones first by index
    report = check_bounds("TIA-USD", shuffled, CELESTIA)
    # By DATE the violations are last, so this is scattered/not a clean prefix.
    assert report.verdict is Bounds.SCATTERED


def test_an_empty_series_is_clean_not_an_error():
    assert check_bounds("EMPTY", [], CELESTIA).verdict is Bounds.CLEAN
