"""
core/fundamentals.py on the AAPL company facts served 2026-10-08 (trimmed to
the allow-list, committed as a fixture): what is kept, as served, and the
insert-only rule against a fake repository.
"""

import gzip
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from core.fundamentals import Fact, ingest_company, parse_company_facts

FIX = Path(__file__).resolve().parents[1] / "fixtures"
WHEN = datetime(2026, 10, 8, 18, 0, tzinfo=timezone.utc)


def load(name):
    return json.loads(gzip.open(FIX / name).read())


@pytest.fixture(scope="module")
def aapl():
    return load("companyfacts_aapl_2026-10-08.json.gz")


class FakeFactRepo:
    def __init__(self):
        self.rows = {}

    async def insert(self, facts):
        new = 0
        for f in facts:
            if f.key() not in self.rows:
                self.rows[f.key()] = f
                new += 1
        return new

    async def facts(self, cik, concepts):
        return [f for f in self.rows.values() if f.cik == cik and f.concept in set(concepts)]


def test_parse_keeps_what_was_served(aapl):
    facts = parse_company_facts(aapl, WHEN)
    assert len(facts) == 4446
    fy25 = [f for f in facts if f.concept == "RevenueFromContractWithCustomerExcludingAssessedTax"
            and f.period_end == date(2025, 9, 27) and f.fp == "FY"]
    assert {f.value for f in fy25} == {416161000000.0}
    assert all(f.fetched_at == WHEN for f in facts)


def test_instants_have_no_period_start(aapl):
    facts = parse_company_facts(aapl, WHEN)
    cash = [f for f in facts if f.concept == "CashAndCashEquivalentsAtCarryingValue"]
    assert cash and all(f.period_start is None for f in cash)
    flows = [f for f in facts if f.concept == "NetCashProvidedByUsedInOperatingActivities"]
    assert flows and all(f.period_start is not None for f in flows)


def test_the_allow_list_is_enforced(aapl):
    doc = json.loads(json.dumps(aapl))
    doc["facts"]["us-gaap"]["SomethingElse"] = {"units": {"USD": [{"end": "2025-09-27", "val": 1, "form": "10-K", "filed": "2025-10-31", "accn": "x"}]}}
    concepts = {f.concept for f in parse_company_facts(doc, WHEN)}
    assert "SomethingElse" not in concepts
    assert len(parse_company_facts(aapl, WHEN, allow=[("us-gaap", "Assets")])) == len(
        [f for f in parse_company_facts(aapl, WHEN) if f.concept == "Assets"])


def test_the_same_period_in_a_later_filing_is_its_own_row(aapl):
    # FY2024 revenue appears in the FY2024 10-K and again in the FY2025 one.
    facts = [f for f in parse_company_facts(aapl, WHEN)
             if f.concept == "RevenueFromContractWithCustomerExcludingAssessedTax" and f.period_end == date(2024, 9, 28) and f.fp == "FY"]
    assert sorted(f.filed for f in facts) == [date(2024, 11, 1), date(2025, 10, 31)]
    assert len({f.key() for f in facts}) == 2


@pytest.mark.asyncio
async def test_ingest_is_insert_only(aapl):
    repo = FakeFactRepo()
    first = await ingest_company(repo, aapl, WHEN)
    again = await ingest_company(repo, aapl, WHEN)
    assert (first.served, first.inserted, again.inserted) == (4446, 4446, 0)
    assert first.entity_name == "Apple Inc." and first.cik == 320193
    # A restatement arrives under a new accession: a new row, the old one kept.
    old = next(f for f in repo.rows.values() if f.concept == "Assets")
    restated = Fact(**{**old.__dict__, "accn": "0000320193-99-000001", "value": old.value + 1, "filed": date(2026, 12, 1)})
    assert await repo.insert([restated, old]) == 1
    assert {f.value for f in repo.rows.values() if f.concept == "Assets" and f.period_end == old.period_end} >= {old.value, old.value + 1}
