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
    assert all(
        (b.period_end - a.period_end).days <= 371 for a, b in zip(rev, rev[1:], strict=False)
    )
    concepts = {v.period_end.year: v.concept for v in rev}
    assert concepts[2010] == "SalesRevenueNet"
    assert concepts[2025] == "RevenueFromContractWithCustomerExcludingAssessedTax"


def test_q4_is_fy_minus_three_quarters_to_the_dollar(aapl):
    q = [
        v
        for v in aapl.quarterly("revenue", NOW)
        if date(2024, 9, 28) < v.period_end <= date(2025, 9, 27)
    ]
    assert [v.derived for v in q] == [False, False, False, True]
    assert q[-1].value == 102_466_000_000  # Apple's reported FY2025 Q4
    assert sum(v.value for v in q) == 416_161_000_000


def test_ttm_is_the_last_four_contiguous_quarters(aapl):
    total, qs = aapl.ttm("revenue", NOW)
    assert [v.period_end for v in qs] == [
        date(2025, 9, 27),
        date(2025, 12, 27),
        date(2026, 3, 28),
        date(2026, 6, 27),
    ]
    assert total == sum(v.value for v in qs)


@pytest.mark.parametrize(
    "as_of, expected_millions, filed",
    [
        (date(2020, 6, 1), 4_648.913, date(2019, 10, 31)),  # pre-split, pre-split basis
        (
            date(2020, 10, 1),
            18_595.652,
            date(2019, 10, 31),
        ),  # same filing, rescaled x4 to today's basis
        (NOW, 18_595.651, date(2021, 10, 29)),  # restated by Apple itself: not rescaled again
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
    return Fact(
        1,
        "us-gaap",
        concept,
        "USD",
        start,
        end,
        value,
        None,
        None,
        "10-K",
        filed,
        accn,
        None,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_a_lower_priority_concept_fills_only_periods_the_higher_one_lacks():
    s = Statements(
        [
            _fact("Revenues", 10.0, date(2017, 12, 31), date(2018, 2, 1), date(2017, 1, 1)),
            _fact("Revenues", 11.0, date(2018, 12, 31), date(2019, 2, 1), date(2018, 1, 1)),
            _fact(
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                12.0,
                date(2018, 12, 31),
                date(2019, 2, 1),
                date(2018, 1, 1),
            ),
        ]
    )
    assert [(v.value, v.concept[:8]) for v in s.annual("revenue", date(2020, 1, 1))] == [
        (10.0, "Revenues"),
        (12.0, "RevenueF"),
    ]


def test_balance_priority_first_concept_wins_on_the_same_date():
    # Cash: CashAndCashEquivalents... outranks the total including restricted cash.
    end, filed = date(2025, 9, 27), date(2025, 10, 31)
    s = Statements(
        [
            _fact(
                "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
                40.0,
                end,
                filed,
                accn="b",
            ),
            _fact("CashAndCashEquivalentsAtCarryingValue", 35.0, end, filed, accn="a"),
        ]
    )
    assert s.instant("cash", NOW).value == 35.0
    assert s.instant("cash", NOW).concept == "CashAndCashEquivalentsAtCarryingValue"


def test_ttm_refuses_a_gap_in_the_quarters():
    q = lambda s_, e_, v: _fact("Revenues", v, e_, date(2026, 1, 1), s_)  # noqa: E731
    s = Statements(
        [
            q(date(2024, 1, 1), date(2024, 3, 31), 1.0),
            q(date(2024, 4, 1), date(2024, 6, 30), 1.0),
            # Q3 2024 missing
            q(date(2024, 10, 1), date(2024, 12, 31), 1.0),
            q(date(2025, 1, 1), date(2025, 3, 31), 1.0),
        ]
    )
    assert s.ttm("revenue", NOW) is None


def test_cash_flow_has_no_hole_where_the_filer_switched_concept(aapl):
    # Apple filed CFO as ...ContinuingOperations from 2014-04 to 2017-02: with
    # only the standard concept its cash flow stopped at FY2013 for three years.
    for as_of in (date(2015, 1, 29), date(2016, 4, 28), date(2017, 2, 2)):
        latest = aapl.annual("cfo", as_of)[-1]
        assert (as_of - latest.period_end).days < 460
    concepts = {v.period_end.year: v.concept for v in aapl.annual("cfo", NOW)}
    assert (
        concepts[2015] == "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
        or concepts[2015] == "NetCashProvidedByUsedInOperatingActivities"
    )
    years = sorted(concepts)
    assert years == list(range(years[0], years[-1] + 1))  # no gap


def test_convertible_notes_are_noncurrent_plus_current():
    """
    AKAM tags only convertible notes, and uses ConvertibleLongTermNotesPayable
    for the NONCURRENT part (its label says "including the current portion"):
    in 2024-06 it fell from $3.540B to $2.394B as $1.148B turned current.
    Until 2026-10-12 neither concept was read and AKAM was refused.
    """
    end, filed = date(2024, 6, 30), date(2024, 8, 1)
    s = Statements(
        [
            _fact("ConvertibleLongTermNotesPayable", 2.394e9, end, filed),
            _fact("ConvertibleNotesPayableCurrent", 1.148e9, end, filed),
        ]
    )
    debt = s.debt(NOW)
    assert debt.value == pytest.approx(3.542e9)
    assert debt.concept == "ConvertibleLongTermNotesPayable + ConvertibleNotesPayableCurrent"


@pytest.mark.parametrize("concept", ["SeniorLongTermNotes", "UnsecuredLongTermDebt"])
def test_senior_and_unsecured_notes_are_totals(concept):
    """VRSN's SeniorLongTermNotes stayed $1.785B when $0.549B turned current."""
    s = Statements([_fact(concept, 1.785e9, date(2026, 6, 30), date(2026, 7, 30))])
    assert s.debt(NOW).value == 1.785e9


def test_a_new_debt_concept_never_outranks_an_existing_one_on_its_date():
    end, filed = date(2025, 12, 31), date(2026, 2, 1)
    s = Statements(
        [
            _fact("LongTermDebtNoncurrent", 5.0e9, end, filed),
            _fact("ConvertibleLongTermNotesPayable", 1.0e9, end, filed),
        ]
    )
    assert s.debt(NOW).value == 5.0e9


def _dal_capex():
    """DAL's capex facts, 2025-2026: two concepts, the main one gone after 2026 Q1."""
    main, other = "PaymentsToAcquireProductiveAssets", "PaymentsToAcquireOtherProductiveAssets"
    d = date
    return [
        _fact(main, 1.224e9, d(2025, 3, 31), d(2025, 4, 9), d(2025, 1, 1), "q1"),
        _fact(main, 2.433e9, d(2025, 6, 30), d(2025, 7, 10), d(2025, 1, 1), "q2"),
        _fact(main, 1.209e9, d(2025, 6, 30), d(2025, 7, 10), d(2025, 4, 1), "q2"),
        _fact(main, 3.592e9, d(2025, 9, 30), d(2025, 10, 9), d(2025, 1, 1), "q3"),
        _fact(main, 1.160e9, d(2025, 9, 30), d(2025, 10, 9), d(2025, 7, 1), "q3"),
        _fact(main, 4.499e9, d(2025, 12, 31), d(2026, 2, 11), d(2025, 1, 1), "fy"),
        _fact(main, 1.200e9, d(2026, 3, 31), d(2026, 4, 8), d(2026, 1, 1), "q1b"),
        _fact(other, 0.680e9, d(2025, 9, 30), d(2025, 10, 9), d(2025, 1, 1), "q3"),
        _fact(other, 0.978e9, d(2025, 12, 31), d(2026, 2, 11), d(2025, 1, 1), "fy"),
        _fact(other, 0.414e9, d(2026, 6, 30), d(2026, 7, 10), d(2026, 1, 1), "q2b"),
        _fact(other, 0.627e9, d(2026, 9, 30), d(2026, 10, 9), d(2026, 1, 1), "q3b"),
    ]


def test_ttm_never_mixes_concepts():
    """FY $4.499B + 9M $0.627B (the OTHER concept) - prior 9M $3.592B was $1.534B."""
    value, parts = Statements(_dal_capex()).ttm("capex", date(2026, 10, 12))
    assert value == pytest.approx(4.475e9)
    assert parts[-1].period_end == date(2026, 3, 31)
    assert {p.concept for p in parts} == {"PaymentsToAcquireProductiveAssets"}


def test_a_quarter_and_its_year_to_date_both_survive_in_any_order():
    """
    A 10-Q's quarter and year-to-date share concept, end and filing date.
    Only one per end used to survive, chosen by the order the database
    returned them: with the quarter kept, the TTM fell back to an older end.
    """
    cfo = "NetCashProvidedByUsedInOperatingActivities"
    d = date
    facts = [
        _fact(cfo, 100.0, d(2025, 12, 31), d(2026, 2, 1), d(2025, 1, 1), "fy"),
        _fact(cfo, 80.0, d(2025, 9, 30), d(2025, 10, 30), d(2025, 1, 1), "q3"),
        _fact(cfo, 30.0, d(2025, 9, 30), d(2025, 10, 30), d(2025, 7, 1), "q3"),
        _fact(cfo, 20.0, d(2026, 3, 31), d(2026, 4, 30), d(2026, 1, 1), "q1"),
        _fact(cfo, 25.0, d(2025, 3, 31), d(2025, 4, 30), d(2025, 1, 1), "q1p"),
        _fact(cfo, 90.0, d(2026, 9, 30), d(2026, 10, 30), d(2026, 1, 1), "q3b"),
        _fact(cfo, 35.0, d(2026, 9, 30), d(2026, 10, 30), d(2026, 7, 1), "q3b"),
    ]
    as_of = d(2026, 11, 2)
    for order in (facts, list(reversed(facts))):
        value, parts = Statements(order)._ttm_ytd("cfo", as_of)
        assert value == 100.0 + 90.0 - 80.0
        assert parts[-1].period_end == d(2026, 9, 30)


def test_a_line_tagged_differently_in_the_10k_still_forms_a_ttm():
    """
    APD files operating cash flow under the ContinuingOperations variant in
    its 10-K and the plain concept in its 10-Qs, never both for one period:
    that is one line under two names, and it mixes.
    """
    plain = "NetCashProvidedByUsedInOperatingActivities"
    cont = "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
    d = date
    s = Statements(
        [
            _fact(cont, 3.257e9, d(2025, 9, 30), d(2025, 11, 20), d(2024, 10, 1), "fy"),
            _fact(plain, 1.996e9, d(2025, 6, 30), d(2025, 7, 31), d(2024, 10, 1), "q3"),
            _fact(plain, 3.310e9, d(2026, 6, 30), d(2026, 7, 30), d(2025, 10, 1), "q3b"),
        ]
    )
    value, parts = s._ttm_ytd("cfo", d(2026, 10, 12))
    assert value == pytest.approx(3.257e9 + 3.310e9 - 1.996e9)
    assert parts[-1].period_end == d(2026, 6, 30)


def test_discrete_quarters_are_one_quarter_each_and_never_another_line():
    """
    Q2 = H1 - Q1, Q3 = 9M - H1, Q4 = FY - 9M from one concept. DAL's ground-
    equipment capex (a separate line: it reports the fiscal year too) never
    stands in for the aircraft line, not even in a year the aircraft line has
    no 10-Q at all.
    """
    other = "PaymentsToAcquireOtherProductiveAssets"
    facts = [
        f for f in _dal_capex() if not (f.period_end.year == 2026 and "Other" not in f.concept)
    ] + [_fact(other, 0.200e9, date(2026, 3, 31), date(2026, 4, 8), date(2026, 1, 1), "q1b")]
    quarters = Statements(facts).discrete_quarters("capex", date(2026, 10, 12))
    got = [(q.period_end, round(q.value / 1e9, 3), q.derived) for q in quarters]
    assert got == [
        (date(2025, 3, 31), 1.224, False),
        (date(2025, 6, 30), 1.209, True),
        (date(2025, 9, 30), 1.159, True),
        (date(2025, 12, 31), 0.907, True),
    ]


def test_a_missing_year_to_date_ends_the_chain_instead_of_a_six_month_quarter():
    """Without H1, 9M - Q1 is six months; it must not be shown as Q3."""
    cfo = "NetCashProvidedByUsedInOperatingActivities"
    d = date
    s = Statements(
        [
            _fact(cfo, 100.0, d(2025, 12, 31), d(2026, 2, 1), d(2025, 1, 1), "fy"),
            _fact(cfo, 20.0, d(2025, 3, 31), d(2025, 4, 30), d(2025, 1, 1), "q1"),
            _fact(cfo, 70.0, d(2025, 9, 30), d(2025, 10, 30), d(2025, 1, 1), "q3"),
        ]
    )
    got = [(q.period_end, q.value) for q in s.discrete_quarters("cfo", d(2026, 3, 1))]
    assert got == [(d(2025, 3, 31), 20.0)]
