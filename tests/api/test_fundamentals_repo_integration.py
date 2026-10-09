"""
db/repositories/fundamentals.py against the real TimescaleDB: insert-only,
and NULLS NOT DISTINCT on period_start (an instant fact stored twice is one
row). Uses a CIK no filer has and removes it afterwards.
"""

from datetime import date, datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete

from core.fundamentals import Fact
from db.models import FundamentalFactORM
from db.repositories.fundamentals import TimescaleFactRepo
from db.session import get_session

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

CIK = 2_000_000_001
WHEN = datetime(2026, 10, 9, tzinfo=timezone.utc)


def fact(concept, value, start=None, accn="A1", filed=date(2025, 10, 31)):
    return Fact(CIK, "us-gaap", concept, "USD", start, date(2025, 9, 27), value, 2025, "FY", "10-K", filed, accn, None, WHEN)


@pytest_asyncio.fixture(loop_scope="session")
async def repo():
    async with get_session() as session:
        await session.execute(delete(FundamentalFactORM).where(FundamentalFactORM.cik == CIK))
        await session.commit()
        try:
            yield TimescaleFactRepo(session)
        finally:
            await session.execute(delete(FundamentalFactORM).where(FundamentalFactORM.cik == CIK))
            await session.commit()


async def test_instant_facts_dedupe_despite_null_start(repo):
    assert await repo.insert([fact("Assets", 1.0)]) == 1
    assert await repo.insert([fact("Assets", 999.0)]) == 0  # same key, NULL start: not a second row
    rows = await repo.facts(CIK, ["Assets"])
    assert [r.value for r in rows] == [1.0]


async def test_restatement_is_a_new_row_and_nothing_is_overwritten(repo):
    flow = dict(start=date(2024, 9, 29))
    assert await repo.insert([fact("NetIncomeLoss", 10.0, **flow)]) == 1
    assert await repo.insert([fact("NetIncomeLoss", 11.0, accn="A2", filed=date(2026, 10, 30), **flow)]) == 1
    rows = await repo.facts(CIK, ["NetIncomeLoss"])
    assert [(r.accn, r.value) for r in rows] == [("A1", 10.0), ("A2", 11.0)]
