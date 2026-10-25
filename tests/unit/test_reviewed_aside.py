"""
Decision 66 (research/eqt-capex-meaning-2026-10-24.md): EQT's 10-Ks filed
2015 and 2016 tag "Capital expenditures for acquisitions" as
PaymentsToAcquirePropertyPlantAndEquipment, the concept every other EQT
filing uses for capex. A (concept, accession) set aside by review is not
read: the line falls to the next concept, and the valuation says so until
a later filing reports the period on that concept. The flows are
Honeywell's (as in test_dated_after_filing), with its capex filing set aside.
"""

from dataclasses import replace
from datetime import date

import pytest

from config import settings
from core.valuation_service import filer_line_concepts
from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs
from tests.unit.test_dated_after_filing import AS_OF, flows
from tests.unit.test_spinoffs import BETA, TEN_K, fact

PPE = "PaymentsToAcquirePropertyPlantAndEquipment"
PRODUCTIVE = "PaymentsToAcquireProductiveAssets"
FY = (date(2025, 1, 1), date(2025, 12, 31))


def ppe_accn():
    (f,) = [f for f in flows() if f.concept == PPE]
    return f.accn


ASIDE = {(PPE, "x"): "a 10-K's acquisitions line"}


def aside():
    return {(PPE, ppe_accn()): "a 10-K's acquisitions line"}


def productive(value=1_300):
    return fact(PRODUCTIVE, value, FY[1], TEN_K, FY[0], "10-K")


def value(facts, reviewed=None):
    st = Statements(facts, reviewed_aside=reviewed)
    return build_inputs(st, AS_OF, 218.31, 0.041, BETA)


def test_a_fact_set_aside_by_review_is_not_read():
    facts = [*flows(), productive()]
    read = Statements(facts).annual("capex", AS_OF)[-1]
    assert (read.concept, read.value) == (PPE, pytest.approx(1_100e6))
    read = Statements(facts, reviewed_aside=aside()).annual("capex", AS_OF)[-1]
    assert (read.concept, read.value) == (PRODUCTIVE, pytest.approx(1_300e6))


def test_the_valuation_names_what_it_set_aside_and_why():
    v = value([*flows(), productive()], aside())
    assert not isinstance(v, Refusal)
    assert v.lines["capex"][0] == pytest.approx(1_300e6)
    assert any(
        n.startswith(f"{PPE} in {ppe_accn()} (filed {TEN_K}) set aside: a 10-K's acquisitions")
        for n in v.notes
    )


def test_a_later_filing_reporting_the_period_ends_the_note():
    later = fact(PPE, 1_150, FY[1], date(2026, 9, 1), FY[0], "10-K/A")
    v = value([*flows(), productive(), later], aside())
    assert v.lines["capex"][0] == pytest.approx(1_150e6)
    assert not any("set aside" in n for n in v.notes)


def test_another_accession_of_the_concept_is_read():
    v = value([*flows(), productive()], ASIDE)
    assert v.lines["capex"][0] == pytest.approx(1_100e6)
    assert not any("set aside" in n for n in v.notes)


def test_eqt_entries_name_three_filings_of_one_concept():
    entries = settings.FACTS_SET_ASIDE_BY_SYMBOL["EQT"]
    assert sorted(a for _, a in entries) == [
        "0000033213-14-000004",
        "0000033213-15-000004",
        "0000033213-16-000018",
    ]
    assert {c for c, _ in entries} == {PPE}


NET = "PaymentsForProceedsFromProductiveAssets"
COP_Q1 = "0001163165-22-000006"


def cop_like():
    """
    Decision 70: COP's FY2021 10-K files gross capex; its 10-Qs from 2022 file
    asset-sale proceeds (negative) on the net concept, and capex only on its
    own concept. As filed (millions): FY2021 5,324; Q1 2021 17; Q1 2022 -2,332.
    """
    y21, y22 = date(2021, 1, 1), date(2022, 1, 1)
    q1_filed = date(2022, 5, 5)
    return [
        fact(PRODUCTIVE, 5_324, date(2021, 12, 31), date(2022, 2, 17), y21, "10-K"),
        replace(fact(NET, 17, date(2021, 3, 31), q1_filed, y21), accn=COP_Q1),
        replace(fact(NET, -2_332, date(2022, 3, 31), q1_filed, y22), accn=COP_Q1),
    ]


def test_asset_sale_proceeds_set_aside_leave_the_fiscal_year():
    as_of = date(2022, 5, 6)
    value, parts = Statements(cop_like()).ttm("capex", as_of)
    assert value == pytest.approx(2_975e6)
    assert NET in {p.concept for p in parts}
    reviewed = settings.FACTS_SET_ASIDE_BY_SYMBOL["COP"]
    st = Statements(cop_like(), reviewed_aside=reviewed)
    assert st.ttm("capex", as_of) is None
    read = st.annual("capex", as_of)[-1]
    assert (read.concept, read.value) == (PRODUCTIVE, pytest.approx(5_324e6))


def test_cop_entries_name_nine_10qs_of_one_concept():
    entries = settings.FACTS_SET_ASIDE_BY_SYMBOL["COP"]
    assert len(entries) == 9
    assert {c for c, _ in entries} == {NET}
    assert COP_Q1 in {a for _, a in entries}


COP_OWN = "PaymentToAcquireProductiveAssetsAndInvestments"


def test_cops_own_capex_concept_is_read_ahead_of_the_list():
    # Decision 70 B: "Capital expenditures and investments" on COP's own
    # concept, Q1 2022 3,161 and Q1 2021 1,200 in the Q1 2022 10-Q.
    own = [
        replace(fact(COP_OWN, v, date(y, 3, 31), date(2022, 5, 5), date(y, 1, 1)), taxonomy="cop")
        for y, v in ((2021, 1_200), (2022, 3_161))
    ]
    st = Statements(
        cop_like() + own,
        line_concepts=filer_line_concepts("COP"),
        reviewed_aside=settings.FACTS_SET_ASIDE_BY_SYMBOL["COP"],
    )
    value, parts = st.ttm("capex", date(2022, 5, 6))
    assert value == pytest.approx(5_324e6 + 3_161e6 - 1_200e6)
    assert [p.concept for p in parts] == [COP_OWN, PRODUCTIVE, COP_OWN]
