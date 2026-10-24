"""
Decision 62: a share count in the wrong unit is set aside. ON's 2012 covers
say 455 trillion shares, CRM's 2011 cover 134 (millions), COP's diluted
averages are in thousands. Each count is checked against witnesses in its
own filing (cover, weighted averages, net income / diluted EPS) and the
newest count of its kind kept from an earlier filing; it is set aside only
when far from all of them while they agree. The flows are Honeywell's (as
in test_spinoffs).
"""

from dataclasses import replace
from datetime import date

import pytest

from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs
from tests.unit.test_spinoffs import BETA, honeywell
from tests.unit.test_spinoffs import fact as hon_fact

AS_OF = date(2026, 10, 23)
Q2, Q3 = date(2026, 7, 23), date(2026, 10, 22)
COVER = "EntityCommonStockSharesOutstanding"
DILUTED = "WeightedAverageNumberOfDilutedSharesOutstanding"


def filed_with(accn, concept, value, end, filed, start=None):
    return replace(hon_fact(concept, value, end, filed, start), accn=accn)


def filing(accn, filed, end, cover, diluted, eps=None):
    """One 10-Q's share witnesses, in millions; eps None leaves earnings out."""
    start = date(end.year, end.month - 2, 1)
    out = [
        filed_with(accn, COVER, cover, end, filed),
        filed_with(accn, DILUTED, diluted, end, filed, start),
    ]
    if eps is not None:
        out += [
            filed_with(accn, "NetIncomeLoss", 1_500, end, filed, start),
            replace(filed_with(accn, "EarningsPerShareDiluted", eps, end, filed, start), value=eps),
        ]
    return out


def flows():
    # Honeywell without its covers: each test files its own.
    return [f for f in honeywell() if "Continuing" not in f.concept and f.concept != COVER]


def value(facts):
    return build_inputs(Statements(facts), AS_OF, 218.31, 0.041, BETA)


Q2_FILING = filing("q2", Q2, date(2026, 6, 30), 316.94, 318.0, 4.72)


def test_a_cover_in_thousands_of_its_witnesses_is_set_aside():
    # ON's: the cover a million times the averages, earnings and the last cover.
    q3 = filing("q3", Q3, date(2026, 9, 30), 317.359e6, 318.5, 4.71)
    st = Statements([*flows(), *Q2_FILING, *q3])
    assert [f.accn for f in st.off_scale] == ["q3"]
    v = value([*flows(), *Q2_FILING, *q3])
    assert not isinstance(v, Refusal)
    assert v.lines["shares"][0] == pytest.approx(316.94e6)
    assert any(
        n.startswith(f"{COVER} filed 2026-10-22 is 317,359,000,000,000, in another unit")
        for n in v.notes
    )


def test_a_cover_is_kept_when_only_its_own_filing_disagrees():
    # ATO's 2011 10-K: averages AND EPS in thousands (one table), the cover
    # right. Without an earlier cover agreeing with them it is not condemned.
    q3 = filing("q3", Q3, date(2026, 9, 30), 317.359, 0.3185, 4710)
    v = value([*flows(), *q3])
    assert v.lines["shares"][0] == pytest.approx(317.359e6)
    assert Statements([*flows(), *q3]).off_scale == []


def test_a_shell_count_before_a_real_one_condemns_nothing():
    # Linde plc's shell filed 25,000 shares, and its first 10-Q's earnings
    # over EPS gave 25,000 too, beside a real 551M cover. SW's shell said 100.
    shell = filing("shell", Q2, date(2026, 6, 30), 0.025, 0.025)
    shell = [f for f in shell if f.concept == COVER]
    q3 = filing("q3", Q3, date(2026, 9, 30), 317.359, 1, 60_000)
    q3 = [f for f in q3 if f.concept != DILUTED]
    st = Statements([*flows(), *shell, *q3])
    assert st.off_scale == []
    assert value([*flows(), *shell, *q3]).lines["shares"][0] == pytest.approx(317.359e6)


def test_a_diluted_average_in_thousands_is_set_aside_and_not_read():
    # COP's: the averages in thousands beside the cover and earnings. With
    # the cover filed as 0 (decision 60) the reader falls back to the
    # averages, and takes the last one in the right unit.
    q3 = filing("q3", Q3, date(2026, 9, 30), 0.0, 0.3185, 4.71)
    st = Statements([*flows(), *Q2_FILING, *q3])
    assert {(f.concept, f.accn) for f in st.off_scale} == {(DILUTED, "q3")}
    v = value([*flows(), *Q2_FILING, *q3])
    assert v.lines["shares"][0] == pytest.approx(318.0e6)
    assert any(n.startswith(f"{DILUTED} filed 2026-10-22 is 318,500") for n in v.notes)


def test_one_witness_is_not_enough():
    # ECHO's 10-Qs: averages in thousands, no cover, no earnings tagged; only
    # the 10-K before disagrees. Kept: one witness cannot tell which is wrong.
    q2 = [f for f in filing("q2", Q2, date(2026, 6, 30), 1, 318.0) if f.concept == DILUTED]
    q3 = [f for f in filing("q3", Q3, date(2026, 9, 30), 1, 0.3185) if f.concept == DILUTED]
    assert Statements([*flows(), *q2, *q3]).off_scale == []


def test_an_average_tagged_as_an_instant_does_not_break_the_check():
    # CZR, GEV and MTCH tag a weighted average with no period start; ordering
    # the filing's counts compared None with a date and every valuation crashed.
    instant = filed_with("q3", DILUTED, 318.4, date(2026, 9, 30), Q3)
    v = value(
        [*flows(), *Q2_FILING, *filing("q3", Q3, date(2026, 9, 30), 317.359, 318.5, 4.71), instant]
    )
    assert v.lines["shares"][0] == pytest.approx(317.359e6)
