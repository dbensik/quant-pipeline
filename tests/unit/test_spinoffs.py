"""
Decision 58 (research/spinoff-basis-2026-10-23.md): a spin-off is not a
split, and a valuation across one is refused until the statements describe
the company the price is for. The cash flows are Honeywell's as filed
($M): Aerospace was spun 2026-06-29 and moved into discontinued operations
by the Q3 10-Q of 2026-10-22, after the FY2025 10-K had only Solstice there.
"""

from datetime import date, datetime, timezone

import pytest

from config import settings
from core.fundamentals import Fact
from core.line_concept_check import unreviewed_splits
from core.valuation_service import share_splits
from modeling.dcf import Beta
from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs

BETA = Beta(1.05, 0.05, 756)
CONT = "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
TOTAL = "NetCashProvidedByUsedInOperatingActivities"
TEN_K = date(2026, 2, 17)
Q2 = date(2026, 7, 23)
Q3 = date(2026, 10, 22)


def fact(concept, value, end, filed, start=None, form="10-Q"):
    return Fact(
        773840,
        "dei" if concept == "EntityCommonStockSharesOutstanding" else "us-gaap",
        concept,
        "shares" if "Shares" in concept else "USD",
        start,
        end,
        value * 1e6,
        None,
        None,
        form,
        filed,
        f"a-{concept}-{start}-{end}-{filed}",
        None,
        datetime(2026, 10, 23, tzinfo=timezone.utc),
    )


def honeywell():
    y25, y26 = date(2025, 1, 1), date(2026, 1, 1)
    flows = [
        # FY2025 10-K: Solstice discontinued, Aerospace still in.
        fact(TOTAL, 6408, date(2025, 12, 31), TEN_K, y25, "10-K"),
        fact(CONT, 6075, date(2025, 12, 31), TEN_K, y25, "10-K"),
        fact("PaymentsToAcquirePropertyPlantAndEquipment", 1100, date(2025, 12, 31), TEN_K, y25),
        # Q2 10-Q: Aerospace not yet discontinued in either column.
        fact(TOTAL, 1916, date(2025, 6, 30), Q2, y25),
        fact(CONT, 1442, date(2025, 6, 30), Q2, y25),
        fact(TOTAL, 626, date(2026, 6, 30), Q2, y26),
        fact(CONT, 626, date(2026, 6, 30), Q2, y26),
        # Q3 10-Q: Aerospace discontinued for all periods presented.
        fact(TOTAL, 5204, date(2025, 9, 30), Q3, y25),
        fact(CONT, 2610, date(2025, 9, 30), Q3, y25),
        fact(TOTAL, 743, date(2026, 9, 30), Q3, y26),
        fact(CONT, 932, date(2026, 9, 30), Q3, y26),
        fact("EntityCommonStockSharesOutstanding", 633.653, date(2026, 3, 31), date(2026, 4, 23)),
        fact("EntityCommonStockSharesOutstanding", 316.940, date(2026, 6, 30), Q2),
        fact("EntityCommonStockSharesOutstanding", 317.359, date(2026, 9, 30), Q3),
    ]
    return flows


def test_the_prior_ytd_carrying_more_discontinued_cash_than_the_year_is_found():
    st = Statements(honeywell())
    fy_disc, prior_disc, fy_total, prior = st.discontinued_cash(date(2026, 10, 23))
    assert (round(fy_disc / 1e6), round(prior_disc / 1e6)) == (333, 2594)
    assert (fy_total.period_end, prior.period_end) == (date(2025, 12, 31), date(2025, 9, 30))
    # As of the Q2 10-Q both terms were on one basis: Solstice's 6M $474M
    # against the year's $333M is a timing difference, not a new business.
    fy_disc, prior_disc, _, _ = st.discontinued_cash(date(2026, 7, 24))
    assert (round(fy_disc / 1e6), round(prior_disc / 1e6)) == (333, 474)


def test_a_trailing_year_across_two_bases_refuses():
    st = Statements(honeywell())
    out = build_inputs(st, date(2026, 10, 23), 218.31, 0.041, BETA)
    assert isinstance(out, Refusal)
    assert "mixes two bases" in out.reason and "2,594M" in out.reason and "333M" in out.reason
    # The same filer on one basis is valued: 6,075 + 626 - 1,442.
    v = build_inputs(st, date(2026, 10, 22), 218.31, 0.041, BETA)
    assert not isinstance(v, Refusal)
    assert v.lines["cfo"] == (pytest.approx(5259e6), "TTM to 2026-06-30")


def test_a_filer_without_discontinued_operations_is_not_refused():
    st = Statements([f for f in honeywell() if f.concept != CONT])
    assert not isinstance(build_inputs(st, date(2026, 10, 23), 218.31, 0.041, BETA), Refusal)


def test_a_valuation_from_a_spin_until_a_period_after_it_is_filed_refuses():
    spin = date(2026, 6, 29)
    st = Statements(honeywell(), spinoffs=[spin])
    # The Q2 10-Q is dated 2026-06-30 but its quarter ended before the
    # distribution: within SPINOFF_FILING_MIN_DAYS it does not count.
    out = build_inputs(st, date(2026, 8, 1), 218.31, 0.041, BETA)
    assert isinstance(out, Refusal) and "spun off a business on 2026-06-29" in out.reason
    # Before the spin, the same statements are valued.
    assert not isinstance(build_inputs(st, date(2026, 6, 28), 218.31, 0.041, BETA), Refusal)
    # The Q3 10-Q (period 2026-09-30) ends the spin refusal; HON is then
    # refused for the mixed trailing year instead.
    out = build_inputs(st, date(2026, 10, 23), 218.31, 0.041, BETA)
    assert isinstance(out, Refusal) and "mixes two bases" in out.reason


def test_a_spin_does_not_multiply_share_counts_but_its_reverse_split_does():
    rows = [(date(2025, 10, 30), 1.061), (date(2026, 6, 29), 0.9535), (date(2027, 1, 4), 2.0)]
    splits, spinoffs = share_splits("HON", rows)
    assert spinoffs == [date(2025, 10, 30), date(2026, 6, 29)]
    # The 1-for-2 reverse split done with the Aerospace distribution
    # (cover shares 634M -> 317M) is a share change; Solstice is not; an
    # ordinary split passes through.
    assert splits == [(date(2026, 6, 29), 0.5), (date(2027, 1, 4), 2.0)]


def test_ctvas_cover_count_is_not_multiplied_by_its_separation():
    count = fact(
        "EntityCommonStockSharesOutstanding", 667.196, date(2026, 7, 24), date(2026, 7, 31)
    )
    rows = [(date(2026, 10, 1), 6.665)]
    as_of = date(2026, 10, 23)
    fixed = Statements([count], *share_splits("CTVA", rows)[:1]).instant(
        "shares_outstanding", as_of
    )
    assert fixed.value == pytest.approx(667.196e6)
    # What it was: the separation's price factor read as a 6.665-for-1 split.
    raw = Statements([count], rows).instant("shares_outstanding", as_of)
    assert raw.value == pytest.approx(667.196e6 * 6.665)


def test_an_unreviewed_odd_split_stays_a_split_and_is_flagged():
    rows = [(date(2027, 3, 1), 1.118), (date(2026, 7, 1), 1.057), (date(2027, 6, 1), 1.5)]
    splits, spinoffs = share_splits("SPGI", rows)
    assert splits == [(date(2027, 3, 1), 1.118), (date(2027, 6, 1), 1.5)]
    assert spinoffs == [date(2026, 7, 1)]
    flags = unreviewed_splits("SPGI", rows)
    assert len(flags) == 1 and flags[0].startswith("SPLIT? SPGI 2027-03-01 ratio 1.118")


@pytest.mark.parametrize("ratio", [2.0, 1.5, 1.25, 0.25, 0.1, 4.0, 1 / 3, 20.0])
def test_ordinary_split_ratios_are_not_odd(ratio):
    assert not settings.odd_split_ratio(ratio)


@pytest.mark.parametrize("ratio", [1.061, 0.9535, 6.665, 1.0027, 0.3775])
def test_spin_factors_are_odd(ratio):
    assert settings.odd_split_ratio(ratio)


def test_every_reviewed_row_is_well_formed():
    for (symbol, ex_date), (kind, share_ratio, note) in settings.SPLIT_ROWS_REVIEWED.items():
        date.fromisoformat(ex_date)
        assert symbol == symbol.upper() and note
        assert kind in ("spin-off", "split", "payment", "other")
        assert share_ratio > 0
        if kind in ("payment", "other"):
            assert share_ratio == 1.0
