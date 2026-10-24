"""
Decision 68 (research/ttm-quarter-mix-2026-10-24.md): the four-quarter TTM
took each quarter from whichever concept had one. FIS tags property alone
(PaymentsToAcquirePropertyPlantAndEquipment) in its 10-K and Q1 10-Q, and a
three-month property-plus-software figure (PaymentsToAcquireProductiveAssets)
in its Q2 and Q3 10-Qs: its derived Q4 went below zero and the trailing year
read two lines. A quarter from a concept that also reports its fiscal year at
a different value ends the four-quarter TTM, as `_ttm_ytd` already refused.
"""

from datetime import date

import pytest

from modeling.statements import Statements
from tests.unit.test_statements import _fact

PPE = "PaymentsToAcquirePropertyPlantAndEquipment"
PRODUCTIVE = "PaymentsToAcquireProductiveAssets"
D = date
AS_OF = D(2026, 8, 15)


def fis(productive_fy=1_390.0):
    """FY2025 and H1 2026: property YTD-only beside a three-month property-plus-software line."""
    return [
        _fact(PPE, 60.0, D(2025, 3, 31), D(2025, 5, 1), D(2025, 1, 1), "q1"),
        _fact(PPE, 130.0, D(2025, 6, 30), D(2025, 8, 1), D(2025, 1, 1), "q2"),
        _fact(PRODUCTIVE, 350.0, D(2025, 6, 30), D(2025, 8, 1), D(2025, 4, 1), "q2"),
        _fact(PPE, 200.0, D(2025, 9, 30), D(2025, 11, 1), D(2025, 1, 1), "q3"),
        _fact(PRODUCTIVE, 360.0, D(2025, 9, 30), D(2025, 11, 1), D(2025, 7, 1), "q3"),
        _fact(PPE, 268.0, D(2025, 12, 31), D(2026, 2, 20), D(2025, 1, 1), "fy"),
        _fact(PRODUCTIVE, productive_fy, D(2025, 12, 31), D(2026, 2, 20), D(2025, 1, 1), "fy"),
        _fact(PPE, 70.0, D(2026, 3, 31), D(2026, 5, 1), D(2026, 1, 1), "q1b"),
        _fact(PPE, 140.0, D(2026, 6, 30), D(2026, 8, 1), D(2026, 1, 1), "q2b"),
        _fact(PRODUCTIVE, 360.0, D(2026, 6, 30), D(2026, 8, 1), D(2026, 4, 1), "q2b"),
    ]


def test_a_quarter_from_a_separate_line_ends_the_four_quarter_ttm():
    """It read 360 + (268 - 770) + 70 + 360 = 288; property alone is 268 + 140 - 130."""
    st = Statements(fis())
    assert st._ttm_quarters("capex", AS_OF) is None
    value, parts = st.ttm("capex", AS_OF)
    assert value == pytest.approx(278.0)
    assert parts[-1].period_end == D(2026, 6, 30)
    assert {p.concept for p in parts} == {PPE}


def test_a_concept_stating_the_same_fiscal_year_still_mixes():
    """Within 0.5% of the year's own figure it is the same line under another name."""
    value, parts = Statements(fis(productive_fy=269.0))._ttm_quarters("capex", AS_OF)
    assert value == pytest.approx(360.0 + (268.0 - 60.0 - 350.0 - 360.0) + 70.0 + 360.0)
    assert {p.concept for p in parts} == {PPE, PRODUCTIVE}


def test_a_concept_used_only_in_the_10qs_still_mixes():
    """With no fiscal year of its own it is another name for the line (APD, WEC)."""
    facts = [f for f in fis() if not (f.concept == PRODUCTIVE and f.accn == "fy")]
    value, parts = Statements(facts)._ttm_quarters("capex", AS_OF)
    assert value == pytest.approx(288.0)
    assert parts[-1].concept == PRODUCTIVE
