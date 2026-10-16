"""
core.insiders.refresh_issuer against the real database, with a fake SEC.

    poetry run pytest -m integration tests/test_insiders_integration.py
"""

from datetime import date
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text

from core.insiders import refresh_issuer
from db.session import get_session

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "form4"
CIK = 999_999_001  # not a real filer


class FakeSEC:
    def __init__(self):
        self.documents = []

    def submissions(self, cik):
        return {
            "filings": {
                "recent": {
                    "form": ["4", "4", "10-Q"],
                    "accessionNumber": ["9999990001-26-000001", "9999990001-26-000002", "x"],
                    "filingDate": ["2026-10-15", "2026-09-03", "2026-08-01"],
                    "primaryDocument": ["xslF345X06/form4.xml", "xslF345X06/edge.xml", "q.htm"],
                }
            }
        }

    def get_text(self, url):
        self.documents.append(url)
        name = "edge_cases.xml" if url.endswith("edge.xml") else "msft_0000789019-26-000228.xml"
        return (FIXTURES / name).read_text()


@pytest_asyncio.fixture(loop_scope="session")
async def session():
    async with get_session() as s:
        await s.execute(text("delete from insider_transactions where issuer_cik = :c"), {"c": CIK})
        await s.commit()
        try:
            yield s
        finally:
            await s.execute(
                text("delete from insider_transactions where issuer_cik = :c"), {"c": CIK}
            )
            await s.commit()


async def test_new_filings_are_stored_under_the_issuer_asked_about(session):
    sec = FakeSEC()
    r = await refresh_issuer(sec, session, CIK, date(2026, 1, 1))
    assert (r.filings_seen, r.filings_fetched, r.rows_inserted) == (2, 2, 3)
    rows = (
        await session.execute(
            text(
                "select accn, row_no, code, price from insider_transactions where issuer_cik = :c order by accn, row_no"
            ),
            {"c": CIK},
        )
    ).all()
    assert [(a, n, c) for a, n, c, _ in rows] == [
        ("9999990001-26-000001", 0, "F"),
        ("9999990001-26-000002", 0, "S"),
        ("9999990001-26-000002", 1, "M"),
    ]
    assert rows[1][3] is None, "a footnote-only price is stored as NULL"


async def test_a_refresh_fetches_only_what_is_not_stored(session):
    await refresh_issuer(FakeSEC(), session, CIK, date(2026, 1, 1))
    sec = FakeSEC()
    r = await refresh_issuer(sec, session, CIK, date(2026, 1, 1))
    assert r.filings_fetched == 0 and sec.documents == []


async def test_since_bounds_what_is_fetched(session):
    sec = FakeSEC()
    r = await refresh_issuer(sec, session, CIK, date(2026, 10, 1))
    assert r.filings_seen == 1 and len(sec.documents) == 1
