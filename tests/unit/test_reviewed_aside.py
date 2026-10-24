"""
Decision 66 (research/eqt-capex-meaning-2026-10-24.md): EQT's 10-Ks filed
2015 and 2016 tag "Capital expenditures for acquisitions" as
PaymentsToAcquirePropertyPlantAndEquipment, the concept every other EQT
filing uses for capex. A (concept, accession) set aside by review is not
read: the line falls to the next concept, and the valuation says so until
a later filing reports the period on that concept. The flows are
Honeywell's (as in test_dated_after_filing), with its capex filing set aside.
"""

from datetime import date

import pytest

from config import settings
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
