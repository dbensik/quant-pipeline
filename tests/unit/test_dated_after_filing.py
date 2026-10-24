"""
Decision 61: a period cannot end after the filing that reports it. ROP's Q1
and Q2 2015 10-Qs tag their 2014-12-31 cash as "2015-12-31", so through its
Q3 10-Q the DCF took $610M as the newest cash balance. Such a fact is set
aside; a cover count dated a few days past its filing (NEM, NRG, ON, XEL)
is a real count and is kept. The flows are Honeywell's (as in
test_spinoffs).
"""

from datetime import date

import pytest

from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs
from tests.unit.test_spinoffs import BETA, honeywell
from tests.unit.test_spinoffs import fact as hon_fact

AS_OF = date(2026, 10, 23)
Q2, Q3 = date(2026, 7, 24), date(2026, 10, 22)
CASH = "CashAndCashEquivalentsAtCarryingValue"
COVER = "EntityCommonStockSharesOutstanding"


def flows():
    # Total CFO only and no spin: a filer valued on its Q3 view.
    return [f for f in honeywell() if "Continuing" not in f.concept]


def value(facts):
    return build_inputs(Statements(facts), AS_OF, 218.31, 0.041, BETA)


def test_a_balance_dated_after_its_filing_is_not_the_newest_balance():
    real = hon_fact(CASH, 9_000, date(2026, 9, 30), Q3)
    misdated = hon_fact(CASH, 610, date(2026, 12, 31), Q2)
    st = Statements([*flows(), real, misdated])
    cash = st.instant("cash", AS_OF)
    assert (cash.period_end, cash.value) == (date(2026, 9, 30), pytest.approx(9_000e6))
    v = value([*flows(), real, misdated])
    assert not isinstance(v, Refusal)
    assert any(
        f"{CASH} filed 2026-07-24 is dated 2026-12-31, after its own filing: set aside" in n
        for n in v.notes
    )


def test_a_misdated_balance_alone_is_not_on_file():
    # Never moved to another date, never a zero: the line is unreported.
    st = Statements([*flows(), hon_fact(CASH, 610, date(2026, 12, 31), Q2)])
    assert st.instant("cash", AS_OF) is None


def test_a_cover_count_dated_days_after_its_filing_is_kept():
    late = hon_fact(COVER, 318.0, date(2026, 10, 31), Q3)
    v = value([*flows(), late])
    assert v.lines["shares"][0] == pytest.approx(318.0e6)
    assert not any("set aside" in n for n in v.notes)


def test_a_cover_count_dated_months_after_its_filing_is_set_aside():
    # CL's FY2009 10-K (filed 2010-02-25) dates its count 2010-12-31.
    late = hon_fact(COVER, 999.0, date(2027, 6, 30), Q3)
    v = value([*flows(), late])
    assert v.lines["shares"][0] == pytest.approx(317.359e6)
