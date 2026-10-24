"""
Decision 64. The capex list is read in one order for every filer, and for
some filers a concept it ranks first is a smaller line: ETR's "Payment for
purchase of plant" ($0 for FY2009) ahead of its "Construction/capital
expenditures" ($1,931M). settings.LINE_CONCEPTS_FIRST_BY_SYMBOL puts the
statement's concept first; the rest of the list still follows, so a filer
that later moves to the list's first concept (ETR from FY2022) keeps its
newest years.
"""

from datetime import date

import pytest

from config import settings
from core.valuation_service import filer_line_concepts
from modeling.statements import FirstOf, Statements
from tests.unit.test_statements import _fact

D = date
OTHER = "PaymentsToAcquireOtherPropertyPlantAndEquipment"
CAPITAL = "PaymentsForCapitalImprovements"
PPE = "PaymentsToAcquirePropertyPlantAndEquipment"
AS_OF = D(2026, 10, 24)


def entergy():
    return [
        _fact(OTHER, 0.0, D(2009, 12, 31), D(2010, 2, 26), D(2009, 1, 1), "fy09"),
        _fact(CAPITAL, 1.931e9, D(2009, 12, 31), D(2010, 2, 26), D(2009, 1, 1), "fy09"),
        _fact(PPE, 4.838e9, D(2024, 12, 31), D(2025, 2, 21), D(2024, 1, 1), "fy24"),
    ]


def test_a_concept_named_first_is_read_ahead_of_the_list_and_the_list_still_follows():
    first = Statements(entergy(), line_concepts={"capex": FirstOf((CAPITAL,))})
    got = [(v.period_end.year, v.value, v.concept) for v in first.annual("capex", AS_OF)]
    assert got == [(2009, 1.931e9, CAPITAL), (2024, 4.838e9, PPE)]
    # The list alone: the $0 line for FY2009.
    assert [v.value for v in Statements(entergy()).annual("capex", AS_OF)] == [0.0, 4.838e9]
    # A sum of the same concept would drop FY2024, which it does not report.
    summed = Statements(entergy(), line_concepts={"capex": (CAPITAL,)})
    assert [v.period_end.year for v in summed.annual("capex", AS_OF)] == [2009]


def test_the_trailing_year_reads_the_quarters_of_the_concept_named_first():
    # FY2010 + 9M 2011 - 9M 2010, all construction spending; the "other" line
    # tags the same periods and would otherwise be read for all three.
    facts = [
        _fact(OTHER, 0.0, D(2010, 12, 31), D(2011, 2, 28), D(2010, 1, 1), "fy10"),
        _fact(CAPITAL, 1.974e9, D(2010, 12, 31), D(2011, 2, 28), D(2010, 1, 1), "fy10"),
        _fact(OTHER, 0.0, D(2010, 9, 30), D(2010, 11, 5), D(2010, 1, 1), "q310"),
        _fact(CAPITAL, 1.4e9, D(2010, 9, 30), D(2010, 11, 5), D(2010, 1, 1), "q310"),
        _fact(OTHER, 0.646e9, D(2011, 9, 30), D(2011, 11, 4), D(2011, 1, 1), "q311"),
        _fact(CAPITAL, 1.5e9, D(2011, 9, 30), D(2011, 11, 4), D(2011, 1, 1), "q311"),
    ]
    st = Statements(facts, line_concepts={"capex": FirstOf((CAPITAL,))})
    total, used = st.ttm("capex", D(2011, 12, 1))
    assert total == pytest.approx(2.074e9)
    assert {v.concept for v in used} == {CAPITAL}
    plain, _ = Statements(facts).ttm("capex", D(2011, 12, 1))
    assert plain == pytest.approx(0.646e9)


def test_a_filer_gets_its_order_beside_its_sums():
    assert filer_line_concepts("etr") == {"capex": FirstOf((CAPITAL,))}
    assert isinstance(filer_line_concepts("ETR")["capex"], FirstOf)
    lnt = filer_line_concepts("LNT")
    assert lnt == settings.LINE_CONCEPTS_BY_SYMBOL["LNT"]
    assert not isinstance(lnt["capex"], FirstOf)
    assert filer_line_concepts("AAPL") == {}
