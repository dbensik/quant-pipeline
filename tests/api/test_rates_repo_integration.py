"""
db/repositories/rates.py against a REAL TimescaleDB: insert-only on
(series, obs_date), and the on-or-before read. The unit tests in
tests/unit/test_rates.py cover the same rules on a fake; only SQL can show the
fake and the table agree.

    poetry run pytest -m integration tests/api/test_rates_repo_integration.py
"""

from datetime import date, datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete

from core.rates import RateObservation
from db.models import RateObservationORM
from db.repositories.rates import TimescaleRateRepo
from db.session import get_session

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

SERIES = "PYTEST-RATE"
FETCHED = datetime(2026, 10, 8, tzinfo=timezone.utc)


def obs(day, value):
    return RateObservation(SERIES, day, value, "test", FETCHED)


@pytest_asyncio.fixture(loop_scope="session")
async def repo():
    async with get_session() as session:
        await _purge(session)
        try:
            yield TimescaleRateRepo(session)
        finally:
            await _purge(session)


async def _purge(session):
    await session.execute(delete(RateObservationORM).where(RateObservationORM.series == SERIES))
    await session.commit()


async def test_insert_is_insert_only(repo):
    assert await repo.insert([obs(date(2026, 10, 6), 4.037), obs(date(2026, 10, 7), 4.037)]) == 2
    assert await repo.insert([obs(date(2026, 10, 7), 9.99), obs(date(2026, 10, 9), 4.02)]) == 1
    stored = await repo.latest_on_or_before(SERIES, date(2026, 10, 7))
    assert stored.value == 4.037
    assert await repo.newest_date(SERIES) == date(2026, 10, 9)


async def test_on_or_before_skips_a_holiday_and_never_looks_ahead(repo):
    await repo.insert([obs(date(2026, 10, 9), 4.02), obs(date(2026, 10, 13), 4.10)])
    holiday = await repo.latest_on_or_before(SERIES, date(2026, 10, 12))
    assert holiday.obs_date == date(2026, 10, 9)
    assert await repo.latest_on_or_before(SERIES, date(2026, 10, 8)) is None
