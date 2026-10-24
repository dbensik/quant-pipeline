"""
Decision 65. Growth's start years are read on the latest year's concept
(Statements.annual_before), which protects a total from a part that shares
its period (ADM). Where one concept means different lines in different years
that is the wrong rule: HAS tags its total as Revenues from FY2016 and its US
and Canada segment as Revenues for FY2013-15, beside SalesRevenueNet, the
statement's "Net revenues". A filer with a FirstOf order for the line reads
each start year as annual() does, by the order.
"""

from datetime import date

from config import settings
from core.valuation_service import filer_line_concepts
from modeling.statements import FirstOf, Statements
from tests.unit.test_statements import _fact

D = date
REVENUES = "Revenues"
SALES = "SalesRevenueNet"
AS_OF = D(2019, 2, 27)


def hasbro():
    return [
        # FY2015 10-K: the statement's line and a segment's.
        _fact(SALES, 4.448e9, D(2015, 12, 27), D(2016, 2, 24), D(2014, 12, 29), "fy15"),
        _fact(REVENUES, 1.972e9, D(2015, 12, 27), D(2016, 2, 24), D(2014, 12, 29), "fy15"),
        # FY2018 10-K: the total, under Revenues only.
        _fact(REVENUES, 4.580e9, D(2018, 12, 30), D(2019, 2, 26), D(2018, 1, 1), "fy18"),
    ]


def test_a_start_year_is_read_by_the_order_not_by_the_latest_years_concept():
    st = Statements(hasbro(), line_concepts={"revenue": FirstOf((SALES,))})
    latest = st.annual("revenue", AS_OF)[-1]
    assert (latest.concept, latest.value) == (REVENUES, 4.580e9)
    start = st.annual_before("revenue", latest, 3, AS_OF)
    assert (start.concept, start.value) == (SALES, 4.448e9)


def test_without_the_order_the_start_year_is_the_segment():
    st = Statements(hasbro())
    latest = st.annual("revenue", AS_OF)[-1]
    start = st.annual_before("revenue", latest, 3, AS_OF)
    assert (start.concept, start.value) == (REVENUES, 1.972e9)


def test_a_sum_keeps_the_latest_years_concept_rule():
    # Only a FirstOf reads per period; a LINE_CONCEPTS_BY_SYMBOL sum is not one.
    st = Statements(hasbro(), line_concepts={"revenue": (REVENUES,)})
    latest = st.annual("revenue", AS_OF)[-1]
    assert st.annual_before("revenue", latest, 3, AS_OF).value == 1.972e9


def test_the_filers_read_off_their_statements_have_revenue_orders():
    for sym in ("ETN", "HAS", "HSY", "LHX", "PG", "TDY"):
        assert filer_line_concepts(sym)["revenue"] == FirstOf((SALES,)), sym
    assert filer_line_concepts("EXC")["revenue"] == FirstOf((REVENUES,))
    uri = filer_line_concepts("URI")
    assert uri["revenue"] == FirstOf((REVENUES,))
    assert uri["capex"] == FirstOf(settings.LINE_CONCEPTS_FIRST_BY_SYMBOL["URI"]["capex"])
