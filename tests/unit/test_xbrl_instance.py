"""
Decision 59 (research/sec-companyfacts-gap-2026-10-23.md): a filing
companyfacts left out is read from its own XBRL instance, into exactly the
rows companyfacts serves for a filing it has. AAPL's Q3 FY2026 10-Q
(0000320193-26-000020, filed 2026-07-31) is in both committed fixtures: the
instance as served, and the companyfacts document of 2026-10-08.
"""

import gzip
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from core.fundamentals import Fact, ingest_company, ingest_instance, parse_company_facts
from core.xbrl_instance import instance_company_facts

FIX = Path(__file__).resolve().parents[1] / "fixtures"
WHEN = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)
ACCN = "0000320193-26-000020"


def rows(facts):
    return {f.key(): (f.value, f.form, f.filed, f.fy, f.fp) for f in facts}


def test_aapls_instance_gives_the_rows_companyfacts_served_for_it():
    served = json.loads(gzip.open(FIX / "companyfacts_aapl_2026-10-08.json.gz").read())
    allow = [(t, c) for t, concepts in served["facts"].items() for c in concepts]
    expected = [f for f in parse_company_facts(served, WHEN, allow) if f.accn == ACCN]
    xml = gzip.open(FIX / f"instance_aapl_{ACCN}.xml.gz").read()
    doc = instance_company_facts(xml, 320193, ACCN, "10-Q", "2026-07-31")
    got = parse_company_facts(doc, WHEN, allow, source="instance")
    assert len(expected) > 50
    assert rows(got) == rows(expected)
    assert {f.source for f in got} == {"instance"} and doc["entityName"] == "Apple Inc."


def instance(contexts, facts):
    return f"""<?xml version="1.0" encoding="utf-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:us-gaap="http://fasb.org/us-gaap/2025"
      xmlns:dei="http://xbrl.sec.gov/dei/2025"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <unit id="usd"><measure>iso4217:USD</measure></unit>
  <unit id="eps"><divide><unitNumerator><measure>iso4217:USD</measure></unitNumerator>
    <unitDenominator><measure>xbrli:shares</measure></unitDenominator></divide></unit>
  {contexts}
  <dei:DocumentFiscalYearFocus contextRef="y">2026</dei:DocumentFiscalYearFocus>
  <dei:DocumentFiscalPeriodFocus contextRef="y">Q2</dei:DocumentFiscalPeriodFocus>
  {facts}
</xbrl>""".encode()


def ctx(cid, start, end, member=False, cik=1):
    seg = (
        '<segment><xbrldi:explicitMember dimension="us-gaap:StatementBusinessSegmentsAxis">'
        "x:A</xbrldi:explicitMember></segment>"
        if member
        else ""
    )
    return (
        f'<context id="{cid}"><entity><identifier scheme="http://www.sec.gov/CIK">{cik}</identifier>'
        f"{seg}</entity><period><startDate>{start}</startDate><endDate>{end}</endDate></period></context>"
    )


def parse(xml):
    doc = instance_company_facts(xml, 1, "a-1", "10-Q", "2026-08-01")
    allow = [
        ("us-gaap", "Depreciation"),
        ("us-gaap", "GoodwillImpairmentLoss"),
        ("us-gaap", "EarningsPerShareDiluted"),
    ]
    return parse_company_facts(doc, WHEN, allow, source="instance")


def test_a_zero_length_duration_is_an_instant():
    # MSFT's FY2026 10-K: GoodwillImpairmentLoss on a 2026-05-01 to 2026-05-01 context.
    xml = instance(
        ctx("y", "2026-01-01", "2026-06-30") + ctx("d", "2026-05-01", "2026-05-01"),
        '<us-gaap:GoodwillImpairmentLoss contextRef="d" unitRef="usd" decimals="-6">0</us-gaap:GoodwillImpairmentLoss>',
    )
    (fact,) = parse(xml)
    assert (fact.period_start, fact.period_end) == (None, date(2026, 5, 1))


def test_a_fact_tagged_twice_keeps_the_more_precise():
    # AMZN's FY2024 10-K: Depreciation 30.2B in the text, 30,225M in the table.
    xml = instance(
        ctx("y", "2026-01-01", "2026-06-30"),
        '<us-gaap:Depreciation contextRef="y" unitRef="usd" decimals="-8">30200000000</us-gaap:Depreciation>'
        '<us-gaap:Depreciation contextRef="y" unitRef="usd" decimals="-6">30225000000</us-gaap:Depreciation>'
        '<us-gaap:Depreciation contextRef="y" unitRef="usd" decimals="-8">30200000000</us-gaap:Depreciation>',
    )
    (fact,) = parse(xml)
    assert fact.value == 30225e6


def test_dimensions_and_nils_are_left_out_and_units_are_named_as_companyfacts_names_them():
    xml = instance(
        ctx("y", "2026-01-01", "2026-06-30") + ctx("seg", "2026-01-01", "2026-06-30", member=True),
        '<us-gaap:Depreciation contextRef="seg" unitRef="usd" decimals="-6">5000000</us-gaap:Depreciation>'
        '<us-gaap:GoodwillImpairmentLoss contextRef="y" unitRef="usd" xsi:nil="true"/>'
        '<us-gaap:EarningsPerShareDiluted contextRef="y" unitRef="eps" decimals="2">1.57</us-gaap:EarningsPerShareDiluted>',
    )
    (fact,) = parse(xml)
    assert (fact.concept, fact.unit, fact.value) == ("EarningsPerShareDiluted", "USD/shares", 1.57)
    assert (fact.fy, fact.fp, fact.form, fact.filed) == (2026, "Q2", "10-Q", date(2026, 8, 1))


def test_another_filers_contexts_are_not_this_filers():
    # A successor's contexts in a filing its predecessor's CIK also lists (XOM 2026-08-03).
    xml = instance(
        ctx("y", "2026-01-01", "2026-06-30") + ctx("o", "2026-01-01", "2026-06-30", cik=2115436),
        '<us-gaap:Depreciation contextRef="o" unitRef="usd" decimals="-6">7000000</us-gaap:Depreciation>'
        '<us-gaap:EarningsPerShareDiluted contextRef="y" unitRef="eps" decimals="2">1.57</us-gaap:EarningsPerShareDiluted>',
    )
    assert [f.concept for f in parse(xml)] == ["EarningsPerShareDiluted"]


class FakeFactRepo:
    def __init__(self):
        self.rows = {}

    async def insert(self, facts):
        new = [f for f in facts if f.key() not in self.rows]
        self.rows.update({f.key(): f for f in new})
        return len(new)

    async def instance_values(self, facts):
        return {
            f.key(): self.rows[f.key()].value
            for f in facts
            if f.key() in self.rows and self.rows[f.key()].source == "instance"
        }

    async def stored_accessions(self, accns):
        return {f.accn for f in self.rows.values()} & set(accns)


@pytest.mark.asyncio
async def test_companyfacts_serving_a_different_value_later_is_a_disagreement_not_a_write():
    xml = gzip.open(FIX / f"instance_aapl_{ACCN}.xml.gz").read()
    repo = FakeFactRepo()
    loaded = await ingest_instance(repo, xml, 320193, ACCN, "10-Q", date(2026, 7, 31), WHEN)
    assert loaded.served == loaded.inserted > 100
    served = json.loads(gzip.open(FIX / "companyfacts_aapl_2026-10-08.json.gz").read())
    # As served: every key of that filing is already stored, and agrees.
    report = await ingest_company(repo, served, WHEN)
    assert report.disagreements == []
    assert all(f.source == "instance" for f in repo.rows.values() if f.accn == ACCN)
    # SEC later serving another value under the same key is named; the row stays.
    cash = next(
        r
        for r in served["facts"]["us-gaap"]["CashAndCashEquivalentsAtCarryingValue"]["units"]["USD"]
        if r["accn"] == ACCN
    )
    stored = cash["val"]
    cash["val"] = stored + 1
    report = await ingest_company(repo, served, WHEN)
    ((fact, kept),) = report.disagreements
    assert (fact.concept, fact.value, kept) == (
        "CashAndCashEquivalentsAtCarryingValue",
        stored + 1,
        stored,
    )
    assert repo.rows[fact.key()].value == stored and repo.rows[fact.key()].source == "instance"


def test_a_fact_defaults_to_companyfacts():
    f = Fact(1, "us-gaap", "Assets", "USD", None, date(2026, 6, 30), 1.0, None, None, "10-Q",
             date(2026, 8, 1), "a", None, WHEN)  # fmt: skip
    assert f.source == "companyfacts"
