"""
Decision 60: a cover-page share count filed as 0 is not a count. ROP's Q3
2015 10-Q (filed 2015-11-03) says 0 shares outstanding beside a diluted
average of 101.6M; the market value came to 0, wacc() raised, and the whole
history endpoint answered 500. The flows here are Honeywell's (as in
test_spinoffs); only the share counts matter.
"""

from datetime import date

import pytest

from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs
from tests.unit.test_spinoffs import BETA, honeywell
from tests.unit.test_spinoffs import fact as hon_fact

AS_OF = date(2026, 10, 23)
Q3 = date(2026, 10, 22)
COVER = "EntityCommonStockSharesOutstanding"
DILUTED = "WeightedAverageNumberOfDilutedSharesOutstanding"


def flows():
    # Total CFO only and no spin: a filer valued on its Q3 view.
    return [f for f in honeywell() if f.concept != COVER and "Continuing" not in f.concept]


def value(facts):
    return build_inputs(Statements(facts), AS_OF, 218.31, 0.041, BETA)


def test_a_zero_cover_count_falls_back_to_the_diluted_average():
    cover = hon_fact(COVER, 0, date(2026, 9, 30), Q3)
    diluted = hon_fact(DILUTED, 318.5, date(2026, 9, 30), Q3, start=date(2026, 7, 1))
    v = value([*flows(), cover, diluted])
    assert not isinstance(v, Refusal)
    assert v.lines["shares"][0] == pytest.approx(318.5e6)
    assert "latest diluted weighted average" in v.lines["shares"][1]
    assert any("filed as 0: diluted average used" in n for n in v.notes)


def test_a_zero_cover_count_without_an_average_refuses_and_never_raises():
    cover = hon_fact(COVER, 0, date(2026, 9, 30), Q3)
    zero_avg = hon_fact(DILUTED, 0, date(2026, 9, 30), Q3, start=date(2026, 7, 1))
    out = value([*flows(), cover, zero_avg])
    assert isinstance(out, Refusal) and "no share count on file" in out.reason


def test_a_positive_cover_count_is_still_used():
    cover = hon_fact(COVER, 317.359, date(2026, 9, 30), Q3)
    diluted = hon_fact(DILUTED, 318.5, date(2026, 9, 30), Q3, start=date(2026, 7, 1))
    v = value([*flows(), cover, diluted])
    assert v.lines["shares"][0] == pytest.approx(317.359e6)
