"""
modeling/statements.py on Apple's real company facts (served 2026-10-08) plus
synthetic cases. Each test pins one rule from the module docstring with a
known answer from Apple's filings.
"""

import gzip
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from core.fundamentals import Fact, parse_company_facts
from modeling.statements import Statements

FIX = Path(__file__).resolve().parents[1] / "fixtures"
SPLIT = [(date(2020, 8, 31), 4.0)]
NOW = date(2026, 10, 9)


@pytest.fixture(scope="module")
def aapl():
    doc = json.loads(gzip.open(FIX / "companyfacts_aapl_2026-10-08.json.gz").read())
    return Statements(parse_company_facts(doc, datetime(2026, 10, 8, tzinfo=timezone.utc)), SPLIT)


@pytest.fixture(scope="module")
def sivb():
    doc = json.loads(gzip.open(FIX / "companyfacts_sivb_2026-10-08.json.gz").read())
    return Statements(parse_company_facts(doc, datetime(2026, 10, 8, tzinfo=timezone.utc)))


def test_fy2025_revenue_known_answer(aapl):
    last = aapl.annual("revenue", NOW)[-1]
    assert (last.period_end, last.value) == (date(2025, 9, 27), 416_161_000_000)


def test_filed_strictly_before_as_of(aapl):
    # The FY2025 10-K was filed 2025-10-31 (accepted after the close).
    assert aapl.annual("revenue", date(2025, 10, 31))[-1].period_end == date(2024, 9, 28)
    assert aapl.annual("revenue", date(2025, 11, 1))[-1].period_end == date(2025, 9, 27)


def test_revenue_is_continuous_across_the_asc606_switch(aapl):
    rev = aapl.annual("revenue", NOW)
    assert len(rev) == 19  # FY2007..FY2025
    assert all((b.period_end - a.period_end).days <= 371 for a, b in zip(rev, rev[1:]))
    concepts = {v.period_end.year: v.concept for v in rev}
    assert concepts[2010] == "SalesRevenueNet"
    assert concepts[2025] == "RevenueFromContractWithCustomerExcludingAssessedTax"


def test_q4_is_fy_minus_three_quarters_to_the_dollar(aapl):
    q = [v for v in aapl.quarterly("revenue", NOW) if date(2024, 9, 28) < v.period_end <= date(2025, 9, 27)]
    assert [v.derived for v in q] == [False, False, False, True]
    assert q[-1].value == 102_466_000_000  # Apple's reported FY2025 Q4
    assert sum(v.value for v in q) == 416_161_000_000


def test_ttm_is_the_last_four_contiguous_quarters(aapl):
    total, qs = aapl.ttm("revenue", NOW)
    assert [v.period_end for v in qs] == [date(2025, 9, 27), date(2025, 12, 27), date(2026, 3, 28), date(2026, 6, 27)]
    assert total == sum(v.value for v in qs)


@pytest.mark.parametrize(
    "as_of, expected_millions, filed",
    [
        (date(2020, 6, 1), 4_648.913, date(2019, 10, 31)),   # pre-split, pre-split basis
        (date(2020, 10, 1), 18_595.652, date(2019, 10, 31)),  # same filing, rescaled x4 to today's basis
        (NOW, 18_595.651, date(2021, 10, 29)),                # restated by Apple itself: not rescaled again
    ],
)
def test_share_counts_follow_the_split_basis_of_their_filing(aapl, as_of, expected_millions, filed):
    v = aapl.at_period("diluted_shares", date(2019, 9, 28), as_of)
    assert v.filed == filed
    assert v.value / 1e6 == pytest.approx(expected_millions, abs=1e-3)


def test_a_restatement_is_invisible_until_the_day_after_it_was_filed(aapl):
    end = date(2025, 9, 27)
    assert aapl.at_period("long_term_debt", end, date(2026, 1, 30)).value == 90_678_000_000
    assert aapl.at_period("long_term_debt", end, date(2026, 1, 31)).value == 90_700_000_000


def test_derived_items(aapl):
    end, fcf = aapl.free_cash_flow_annual(NOW)[-1]
    cfo = aapl.at_period("cfo", end, NOW).value
    capex = aapl.at_period("capex", end, NOW).value
    assert end == date(2025, 9, 27) and fcf == cfo - capex
    assert 0.10 < aapl.effective_tax_rate(NOW) < 0.30


def test_missing_is_none_not_zero(sivb):
    # A bank files no operating income or cost of revenue.
    assert sivb.annual("operating_income", NOW) == []
    assert sivb.instant("commercial_paper", NOW) is None
    assert sivb.annual("revenue", NOW)  # but does file revenue (as RevenueFromContract...)


def _fact(concept, value, end, filed, start=None, accn="a"):
    return Fact(1, "us-gaap", concept, "USD", start, end, value, None, None, "10-K", filed, accn, None,
                datetime(2026, 1, 1, tzinfo=timezone.utc))


def test_a_lower_priority_concept_fills_only_periods_the_higher_one_lacks():
    s = Statements([
        _fact("Revenues", 10.0, date(2017, 12, 31), date(2018, 2, 1), date(2017, 1, 1)),
        _fact("Revenues", 11.0, date(2018, 12, 31), date(2019, 2, 1), date(2018, 1, 1)),
        _fact("RevenueFromContractWithCustomerExcludingAssessedTax", 12.0, date(2018, 12, 31), date(2019, 2, 1), date(2018, 1, 1)),
    ])
    assert [(v.value, v.concept[:8]) for v in s.annual("revenue", date(2020, 1, 1))] == [(10.0, "Revenues"), (12.0, "RevenueF")]


def test_balance_priority_first_concept_wins_on_the_same_date():
    # Cash: CashAndCashEquivalents... outranks the total including restricted cash.
    end, filed = date(2025, 9, 27), date(2025, 10, 31)
    s = Statements([
        _fact("CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", 40.0, end, filed, accn="b"),
        _fact("CashAndCashEquivalentsAtCarryingValue", 35.0, end, filed, accn="a"),
    ])
    assert s.instant("cash", NOW).value == 35.0
    assert s.instant("cash", NOW).concept == "CashAndCashEquivalentsAtCarryingValue"


def test_ttm_refuses_a_gap_in_the_quarters():
    q = lambda s_, e_, v: _fact("Revenues", v, e_, date(2026, 1, 1), s_)  # noqa: E731
    s = Statements([
        q(date(2024, 1, 1), date(2024, 3, 31), 1.0),
        q(date(2024, 4, 1), date(2024, 6, 30), 1.0),
        # Q3 2024 missing
        q(date(2024, 10, 1), date(2024, 12, 31), 1.0),
        q(date(2025, 1, 1), date(2025, 3, 31), 1.0),
    ])
    assert s.ttm("revenue", NOW) is None
