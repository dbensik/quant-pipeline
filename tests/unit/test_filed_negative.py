"""
Decision 67 (research/negative-capex-2026-10-24.md): ten filers' 2009-2011
10-Ks file capex below zero (CSX FY2009 -1,447, filed 1,426-1,427 by the
next two), and FCF = CFO - capex then ADDED it: CSX 2010-02-20 was valued at
$207.32 against a $46.77 price. A payment filed negative is set aside, never
flipped; the line falls to the next concept or the valuation refuses, and
says which filing it set aside. The flows are Honeywell's (as in
test_reviewed_aside), with its capex filed negative.
"""

from dataclasses import replace

import pytest

from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs
from tests.unit.test_dated_after_filing import AS_OF, flows
from tests.unit.test_reviewed_aside import FY, PPE, ppe_accn, productive
from tests.unit.test_spinoffs import BETA, TEN_K, fact

NET = "PaymentsForProceedsFromProductiveAssets"


def negative():
    """Honeywell's flows with its FY2025 capex filed as -1,100."""
    return [replace(f, value=-f.value) if f.concept == PPE else f for f in flows()]


def value(facts):
    return build_inputs(Statements(facts), AS_OF, 218.31, 0.041, BETA)


def test_a_payment_filed_negative_is_not_read():
    st = Statements([*negative(), productive()])
    read = st.annual("capex", AS_OF)[-1]
    assert (read.concept, read.value) == (
        "PaymentsToAcquireProductiveAssets",
        pytest.approx(1_300e6),
    )
    assert [f.concept for f in st.filed_negative] == [PPE]


def test_alone_it_refuses_rather_than_adding_to_cash_flow():
    v = value(negative())
    assert isinstance(v, Refusal)
    assert "capital expenditure" in v.reason


def test_the_valuation_names_the_filing_it_set_aside():
    v = value([*negative(), productive()])
    assert not isinstance(v, Refusal)
    assert v.lines["capex"][0] == pytest.approx(1_300e6)
    assert (
        f"{PPE} in {ppe_accn()} (filed {TEN_K}) set aside: filed negative ($-1,100M), "
        "and a payment is an outflow"
    ) in v.notes


def test_a_later_filing_of_the_period_is_read_and_ends_the_note():
    later = fact(PPE, 1_100, FY[1], AS_OF.replace(day=1), FY[0], "10-K/A")
    v = value([*negative(), later])
    assert v.lines["capex"][0] == pytest.approx(1_100e6)
    assert not any("filed negative" in n for n in v.notes)


def test_a_net_of_sale_proceeds_may_be_negative():
    net = fact(NET, -50, FY[1], TEN_K, FY[0], "10-K")
    st = Statements([*flows(), net])
    assert st.filed_negative == []
    assert st._periods("capex", AS_OF, (350, 380), (NET,))[FY].value == pytest.approx(-50e6)


def test_a_refusal_names_the_filing_it_set_aside():
    # Decision 73: a refusal carries no notes, so CSX 2010-02-20 read only
    # "not reported", not which filing was set aside and why.
    v = value(negative())
    assert v.reason.endswith(
        f"; {PPE} in {ppe_accn()} (filed {TEN_K}) set aside: filed negative ($-1,100M), "
        "and a payment is an outflow"
    )


def test_a_refusal_of_another_line_does_not_name_it():
    cfo = "NetCashProvidedByUsedInOperatingActivities"
    v = value([f for f in negative() if f.concept != cfo])
    assert isinstance(v, Refusal)
    assert v.reason == "no operating cash flow on file before this date"
