"""
Decision 74 (research/reverse-mergers-2026-10-25.md). In a reverse merger
the legal registrant keeps its CIK and ticker but is acquired for
accounting: KDP's filer was Dr Pepper Snapple, JCI's is Tyco. Its 10-Ks
before the close are its own past; from the close on, it files the
accounting acquirer's (Keurig, Johnson Controls Inc.). A growth start year
filed before the close is never read against a year filed after it, and a
trailing year built on the registrant's own last fiscal year refuses.
"""

from datetime import date

import pytest

from config import settings
from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs, default_growth
from tests.unit.test_spinoffs import BETA, CONT, fact, honeywell
from tests.unit.test_statements import _fact

D = date
SALES = "SalesRevenueNet"
CONTRACT = "RevenueFromContractWithCustomerExcludingAssessedTax"
CLOSE = D(2018, 7, 9)


def kdp():
    """KDP's revenue as filed ($): DPS's own 10-Ks, then Keurig Dr Pepper's."""
    return [
        _fact(SALES, 6.282e9, D(2015, 12, 31), D(2018, 2, 14), D(2015, 1, 1), "dps17"),
        _fact(SALES, 6.440e9, D(2016, 12, 31), D(2018, 2, 14), D(2016, 1, 1), "dps17"),
        _fact(SALES, 6.690e9, D(2017, 12, 31), D(2018, 2, 14), D(2017, 1, 1), "dps17"),
        # The FY2018 10-K: Keurig's fiscal year to 2017-09-30, then 2018.
        _fact(CONTRACT, 4.269e9, D(2017, 9, 30), D(2019, 2, 28), D(2016, 9, 25), "kdp18"),
        _fact(CONTRACT, 7.442e9, D(2018, 12, 31), D(2019, 2, 28), D(2018, 1, 1), "kdp18"),
        _fact(CONTRACT, 11.120e9, D(2019, 12, 31), D(2020, 2, 27), D(2019, 1, 1), "kdp19"),
    ]


def test_a_start_year_dps_filed_is_not_read_against_kdps_years():
    as_of = D(2020, 2, 28)
    before = Statements(kdp())
    latest = before.annual("revenue", as_of)[-1]
    assert before.annual_before("revenue", latest, 3, as_of).value == 6.440e9
    st = Statements(kdp(), reverse_merger=CLOSE)
    assert st.annual_before("revenue", latest, 3, as_of) is None
    assert default_growth(st, as_of)[1].startswith("too little revenue history")


def test_dps_own_points_still_read_its_own_years():
    as_of = D(2018, 2, 15)
    st = Statements(kdp(), reverse_merger=CLOSE)
    latest = st.annual("revenue", as_of)[-1]
    assert st.annual_before("revenue", latest, 2, as_of).value == 6.282e9


def test_a_year_filed_after_the_close_is_a_start_year():
    # FY2021 against FY2018 as KDP filed it: growth by acquisition, labelled so.
    fy21 = _fact(CONTRACT, 12.683e9, D(2021, 12, 31), D(2022, 2, 25), D(2021, 1, 1), "kdp21")
    st = Statements([*kdp(), fy21], reverse_merger=CLOSE)
    as_of = D(2022, 2, 26)
    latest = st.annual("revenue", as_of)[-1]
    assert st.annual_before("revenue", latest, 3, as_of).value == 7.442e9


def test_a_trailing_year_on_the_registrants_own_year_refuses():
    close = D(2026, 6, 29)
    total_only = [f for f in honeywell() if f.concept != CONT]
    st = Statements(total_only, reverse_merger=close)
    out = build_inputs(st, D(2026, 10, 23), 218.31, 0.041, BETA)
    assert isinstance(out, Refusal)
    assert "a reverse merger closed on 2026-06-29" in out.reason
    assert "fiscal year to 2025-12-31" in out.reason
    # Before the close the same statements are valued.
    assert not isinstance(build_inputs(st, D(2026, 6, 28), 218.31, 0.041, BETA), Refusal)
    # A 10-K for a year ending after the close ends it.
    fy26 = fact(
        "NetCashProvidedByUsedInOperatingActivities",
        5100,
        D(2026, 12, 31),
        D(2027, 2, 12),
        D(2026, 1, 1),
        "10-K",
    )
    v = build_inputs(
        Statements([*total_only, fy26], reverse_merger=close), D(2027, 2, 13), 218.31, 0.041, BETA
    )
    assert not isinstance(v, Refusal)
    assert v.lines["cfo"] == (pytest.approx(5100e6), "FY to 2026-12-31")


def test_kdp_and_jci_are_listed():
    assert settings.REVERSE_MERGERS["KDP"][0] == "2018-07-09"
    assert settings.REVERSE_MERGERS["JCI"][0] == "2016-09-02"
