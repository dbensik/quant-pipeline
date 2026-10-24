"""
Decision 63. A predecessor that keeps filing after its successor's first
10-Q/10-K is a subsidiary from then on: Apache's 10-Qs say 1,000 shares
(owned by APA) beside its own cash and CFO, and on each shared filing day
they stood beside APA's rows. A cover count or diluted average under 1M
shares is set aside: Fox's only untagged cover was its registration's 1
share, read ahead of its 621M diluted average. The flows are Honeywell's (as
in test_spinoffs).
"""

from dataclasses import replace
from datetime import date

import pytest

from config import settings
from core.valuation_service import successor_facts
from modeling.statements import Statements
from modeling.valuation import Refusal, build_inputs
from tests.unit.test_spinoffs import BETA, honeywell
from tests.unit.test_spinoffs import fact as hon_fact

AS_OF = date(2026, 10, 23)
Q2, Q3 = date(2026, 7, 23), date(2026, 10, 22)
COVER = "EntityCommonStockSharesOutstanding"
DILUTED = "WeightedAverageNumberOfDilutedSharesOutstanding"
CASH = "CashAndCashEquivalentsAtCarryingValue"
OWN, PRED = 2, 1


def at(cik, concept, value, end, filed, start=None, form="10-Q"):
    f = hon_fact(concept, value, end, filed, start, form)
    return replace(f, cik=cik, accn=f"{cik}-{f.accn}")


def flows():
    return [f for f in honeywell() if "Continuing" not in f.concept and f.concept != COVER]


def value(facts):
    return build_inputs(Statements(facts), AS_OF, 218.31, 0.041, BETA)


def chain():
    # The predecessor's history, then the successor's first 10-Q (Q2) and a
    # predecessor 10-Q the same day and after, as the subsidiary.
    return [
        at(PRED, CASH, 7_000, date(2026, 3, 31), date(2026, 4, 24)),
        at(OWN, CASH, 8_000, date(2026, 6, 30), Q2),
        at(PRED, CASH, 900, date(2026, 6, 30), Q2),
        at(PRED, CASH, 950, date(2026, 9, 30), Q3),
    ]


def test_a_predecessor_is_read_only_before_its_successor_files():
    kept = successor_facts("APA", [OWN, PRED], chain())
    assert sorted((f.cik, f.value) for f in kept) == [(PRED, 7_000e6), (OWN, 8_000e6)]


def test_a_predecessor_named_read_through_is_read_to_that_day(monkeypatch):
    # WBA: its first 10-Q was the pre-merger shell's; Walgreen Co's that day
    # is the business.
    monkeypatch.setitem(settings.CIK_PREDECESSOR_READ_THROUGH, "WBA", Q2.isoformat())
    kept = successor_facts("WBA", [OWN, PRED], chain())
    assert sorted(f.value for f in kept if f.cik == PRED) == [900e6, 7_000e6]


def test_a_successor_with_no_filing_yet_reads_its_whole_predecessor():
    facts = [f for f in chain() if f.cik == PRED]
    assert successor_facts("XOM", [OWN, PRED], facts) == facts


def test_a_shell_cover_is_set_aside_and_the_diluted_average_read():
    # Fox's 2019 registration: 1 share, then no untagged cover.
    shell = at(OWN, COVER, 1e-6, date(2026, 6, 30), Q2)
    avg = at(OWN, DILUTED, 621.0, date(2026, 9, 30), Q3, date(2026, 7, 1))
    st = Statements([*flows(), shell, avg])
    assert st.too_small == [shell] and st.off_scale == []
    v = value([*flows(), shell, avg])
    assert not isinstance(v, Refusal)
    assert v.lines["shares"][0] == pytest.approx(621e6)
    assert any(
        n.startswith(f"{COVER} filed 2026-07-23 is 1, under 1,000,000 shares") for n in v.notes
    )


def test_a_diluted_average_in_thousands_is_set_aside_without_witnesses():
    # ECHO's 10-Qs: averages in thousands, no cover; the last in units is read.
    right = at(OWN, DILUTED, 270.8, date(2026, 6, 30), Q2, date(2026, 4, 1))
    thousands = at(OWN, DILUTED, 0.271519, date(2026, 9, 30), Q3, date(2026, 7, 1))
    v = value([*flows(), right, thousands])
    assert v.lines["shares"][0] == pytest.approx(270.8e6)
    assert any(n.startswith(f"{DILUTED} filed 2026-10-22 is 271,519, under") for n in v.notes)
