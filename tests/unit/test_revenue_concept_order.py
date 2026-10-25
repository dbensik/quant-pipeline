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


CONTRACT = "RevenueFromContractWithCustomerExcludingAssessedTax"


def ebay():
    """Decision 72: FY2014 as first filed with PayPal, as recast without it, and FY2019."""
    return [
        _fact(REVENUES, 17.902e9, D(2014, 12, 31), D(2015, 2, 6), D(2014, 1, 1), "fy14"),
        _fact(SALES, 17.902e9, D(2014, 12, 31), D(2015, 2, 6), D(2014, 1, 1), "fy14"),
        _fact(SALES, 8.790e9, D(2014, 12, 31), D(2016, 2, 1), D(2014, 1, 1), "fy15"),
        _fact(CONTRACT, 10.800e9, D(2019, 12, 31), D(2020, 1, 31), D(2019, 1, 1), "fy19"),
    ]


def test_ebays_start_year_is_the_recast_net_revenues():
    as_of = D(2020, 2, 1)
    for lines, want in (({}, 17.902e9), (filer_line_concepts("EBAY"), 8.790e9)):
        st = Statements(ebay(), line_concepts=lines)
        latest = st.annual("revenue", as_of)[-1]
        assert st.annual_before("revenue", latest, 5, as_of).value == want


def test_cags_part_is_never_a_start_year():
    # "Net sales" $11,643M (FY2016) is on a concept not stored; Revenues is a part.
    facts = [
        _fact(REVENUES, 1.5e9, D(2016, 5, 29), D(2016, 7, 15), D(2015, 6, 1), "fy16"),
        _fact(CONTRACT, 9.538e9, D(2019, 5, 26), D(2019, 7, 19), D(2018, 5, 28), "fy19"),
    ]
    as_of = D(2019, 7, 20)
    for lines, want in (({}, 1.5e9), (filer_line_concepts("CAG"), None)):
        st = Statements(facts, line_concepts=lines)
        latest = st.annual("revenue", as_of)[-1]
        start = st.annual_before("revenue", latest, 3, as_of)
        assert (start and start.value) == want


def test_decision_72s_filers_have_their_revenue_lines():
    assert filer_line_concepts("EBAY")["revenue"] == FirstOf((CONTRACT, SALES))
    assert filer_line_concepts("GEN")["revenue"][-1] == SALES
    assert filer_line_concepts("FE")["revenue"] == FirstOf((REVENUES,))
    assert filer_line_concepts("MA")["revenue"] == FirstOf((REVENUES,))
    assert filer_line_concepts("CAG")["revenue"] == (CONTRACT,)
