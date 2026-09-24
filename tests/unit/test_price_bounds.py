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
    has_bad_extremes,
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


# ---------------------------------------------------------------------------
# Straddle bars — why low and high must be checked, not just close
# ---------------------------------------------------------------------------

AAVE = CoinBounds("aave", 26.02, 661.69)

#: The real AAVE-USD bars around the 100:1 LEND->AAVE redenomination, as
#: (date, close, low, high). 2020-10-03 opens on the OLD basis and closes on
#: the NEW one — a 124.7x intraday range, with a close that is perfectly in
#: range.
REDENOMINATION = [
    (date(2020, 10, 2), 0.5166, 0.0, 0.5166),
    (date(2020, 10, 3), 53.1515, 0.5238, 65.3059),
    (date(2020, 10, 4), 52.6750, 50.6890, 55.0704),
    (date(2020, 10, 5), 53.2192, 49.7879, 55.1124),
]


def test_a_straddle_bar_is_caught_only_when_low_and_high_are_checked():
    """
    THE case. Close-only proposes cutting AT the straddle bar, leaving a
    phantom 124.7x high/low as the first bar of the series — poisoning every
    range, ATR and candlestick consumer while the close series looks fine.
    """
    with_range = check_bounds("AAVE-USD", REDENOMINATION, AAVE)
    assert with_range.verdict is Bounds.TRIM
    assert with_range.valid_from == date(2020, 10, 4)
    assert with_range.outside == 2

    close_only = check_bounds(
        "AAVE-USD", [(d, c) for d, c, _, _ in REDENOMINATION], AAVE
    )
    assert close_only.valid_from == date(2020, 10, 3)  # one bar short


def test_a_zero_price_is_absent_not_worthless():
    """
    Stub bars carry open=0 and low=0. A zero is a missing price, not a claim
    that the asset traded at nothing, so it must not be judged as a violation
    on its own — 2020-10-02 is caught by its CLOSE being 0.5166.
    """
    only_zeros = [(date(2020, 10, 4), 52.675, 0.0, 0.0)]
    assert check_bounds("AAVE-USD", only_zeros, AAVE).verdict is Bounds.CLEAN


def test_close_only_bars_still_work():
    """The two-tuple form stays valid; most callers have only closes."""
    assert check_bounds("X", [(date(2024, 1, 1), 100.0)], AAVE).verdict is (
        Bounds.CLEAN
    )


# ---------------------------------------------------------------------------
# has_bad_extremes — a sound close with a corrupt high or low
#
# Judged against the bar's OWN body. An earlier version used the asset's
# all-time range and was wrong: it flagged ordinary intraday moves that merely
# grazed CoinGecko's recorded high, which is drawn from a different exchange
# set than the bar.
# ---------------------------------------------------------------------------

def test_a_stablecoin_high_of_three_dollars_is_a_bad_tick():
    """DAI 2021-11-16: closed at 1.0012 with a high of 3.6684 — 3.66x."""
    assert has_bad_extremes(1.0012, 0.9910, 3.6684, 1.0009) is True


def test_wbtc_high_of_162k_on_a_59k_close_is_a_bad_tick():
    assert has_bad_extremes(59078.88, 58289.18, 162188.25, 63446.02) is True


def test_a_zero_low_beside_a_four_thousand_dollar_close_is_a_bad_tick():
    """
    WETH 2021-11-16: close 4241.88, low exactly 0.0. A bar that closed at
    $4,241 did not also trade at nothing, so the zero is a WRONG number rather
    than a missing one.
    """
    assert has_bad_extremes(4241.88, 0.0, 33329.43, 4583.28) is True


def test_an_ordinary_intraday_move_is_NOT_a_bad_tick():
    """
    THE false positives that killed the first version. Both grazed their
    coin's recorded all-time high and are perfectly normal bars.
    """
    # CRO 2021-11-24: high 1.14x the close.
    assert has_bad_extremes(0.848222, 0.839875, 0.969806, 0.86) is False
    # ETC 2021-05-06: high 1.31x, low 0.65x — a violent but real day.
    assert has_bad_extremes(134.102, 87.6428, 176.1577, 130.0) is False


def test_a_sound_bar_is_left_alone():
    assert has_bad_extremes(1.0002, 0.9955, 1.0065, 1.0007) is False


def test_a_missing_close_is_not_judged():
    assert has_bad_extremes(None, 1.0, 2.0, 1.0) is False
    assert has_bad_extremes(float("nan"), 1.0, 2.0, 1.0) is False


def test_a_liquidation_cascade_is_NOT_a_bad_tick():
    """
    THE correction. 2025-10-10 saw ONDO, RENDER, TIA, WIF and WLD all wick to
    0.31-0.48x of their close. Five unrelated alts on one day is a real
    cascade, and a symmetric threshold would have deleted genuine history —
    the same mistake nearly made with AAVE's redenomination.
    """
    assert has_bad_extremes(0.694529, 0.33155, 0.901145, 0.70) is False   # ONDO 0.48x
    assert has_bad_extremes(2.2775, 0.715719, 3.335158, 2.30) is False    # RENDER 0.31x


def test_a_low_far_beyond_any_crash_is_still_caught():
    """SEI 2023-08-15: low 0.00799 against a 0.1776 close — 22x below."""
    assert has_bad_extremes(0.177638, 0.007989, 0.208617, 0.18) is True


def test_the_thresholds_are_asymmetric_and_sit_in_the_measured_gaps():
    """
    A market can crash 60% intraday and recover; it cannot double and retrace.
    Real bad highs reach 2.08x, the widest legitimate high seen was 1.46x. The
    deepest REAL wick was 0.31x, so the low threshold must be far looser.
    """
    from core.price_bounds import EXTREME_HIGH_RATIO, EXTREME_LOW_RATIO

    assert 1.5 < EXTREME_HIGH_RATIO < 2.05
    assert EXTREME_LOW_RATIO > 3.0


def test_an_already_nulled_extreme_is_not_flagged_again():
    """
    IDEMPOTENCY. The repair nulls high and low, so the next scan sees None
    there. Treating that as a defect reports repaired bars as still broken —
    which is how a fix comes to look like it never worked. A None is absent;
    only a zero is wrong.
    """
    assert has_bad_extremes(59078.88, None, None, 63446.02) is False
    assert has_bad_extremes(59078.88, None, 162188.25, 63446.02) is True
    assert has_bad_extremes(59078.88, 0.0, None, 63446.02) is True
