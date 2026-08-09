"""
TimescaleUniverseRepo — point-in-time index membership, against real Postgres.

The rule these exist to protect: a query for a date before the first snapshot
must NOT fall back to today's membership. That fallback is survivorship bias,
and it is invisible in the output — the numbers still look reasonable, they
are just flattered by the exclusion of everything that did badly enough to be
dropped.

    docker-compose up -d timescaledb
    poetry run alembic upgrade head
    poetry run pytest -m integration

Phase 5 — point-in-time universe
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete

from db.models import UniverseMembershipORM, UniverseSnapshotORM
from db.repositories.universe import TimescaleUniverseRepo
from db.session import get_session

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

INDEX = "pytest-index"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T1 = T0 + timedelta(days=30)
T2 = T0 + timedelta(days=60)


@pytest_asyncio.fixture(loop_scope="session")
async def repo():
    async with get_session() as session:
        instance = TimescaleUniverseRepo(session)
        await _purge(session)
        try:
            yield instance
        finally:
            await _purge(session)


async def _purge(session):
    await session.execute(
        delete(UniverseMembershipORM).where(UniverseMembershipORM.index_name == INDEX)
    )
    await session.execute(
        delete(UniverseSnapshotORM).where(UniverseSnapshotORM.index_name == INDEX)
    )
    await session.commit()


async def test_a_snapshot_records_its_members(repo):
    snapshot = await repo.record_snapshot(INDEX, ["AAPL", "MSFT"], taken_at=T0)
    assert snapshot.member_count == 2

    members = await repo.members_as_of(INDEX, T0)
    assert members.observed is True
    assert members.symbols == ["AAPL", "MSFT"]


async def test_a_date_before_the_first_snapshot_is_NOT_observed(repo):
    """
    THE rule. Returning today's membership here is survivorship bias, and it
    would be invisible: the screen still returns names, just the wrong ones.
    """
    await repo.record_snapshot(INDEX, ["AAPL", "MSFT"], taken_at=T1)

    members = await repo.members_as_of(INDEX, T0)
    assert members.observed is False
    assert members.symbols == []
    assert members.first_observed == T1


async def test_a_symbol_dropped_between_snapshots_stays_queryable_in_its_window(repo):
    """
    The whole point: a name that left the index must still appear in a screen
    of the window it was in. Deleting it is what creates the bias.
    """
    await repo.record_snapshot(INDEX, ["AAPL", "ENRON"], taken_at=T0)
    await repo.record_snapshot(INDEX, ["AAPL"], taken_at=T2)

    then = await repo.members_as_of(INDEX, T0)
    now = await repo.members_as_of(INDEX, T2)

    assert "ENRON" in then.symbols
    assert "ENRON" not in now.symbols


async def test_a_dropped_symbol_is_reported_as_removed(repo):
    await repo.record_snapshot(INDEX, ["AAPL", "ENRON"], taken_at=T0)
    snapshot = await repo.record_snapshot(INDEX, ["AAPL", "NVDA"], taken_at=T2)

    assert snapshot.removed == ["ENRON"]
    assert snapshot.added == ["NVDA"]


async def test_first_seen_is_not_rewritten_by_later_snapshots(repo):
    """
    A long-standing member must keep its original observation date, or every
    snapshot would make the index look brand new.
    """
    await repo.record_snapshot(INDEX, ["AAPL"], taken_at=T0)
    await repo.record_snapshot(INDEX, ["AAPL"], taken_at=T2)

    members = await repo.members_as_of(INDEX, T1)
    assert members.symbols == ["AAPL"]
    assert members.first_observed == T0


async def test_membership_between_snapshots_uses_the_surrounding_window(repo):
    await repo.record_snapshot(INDEX, ["AAPL", "MSFT"], taken_at=T0)
    await repo.record_snapshot(INDEX, ["AAPL", "MSFT"], taken_at=T2)

    members = await repo.members_as_of(INDEX, T1)
    assert members.symbols == ["AAPL", "MSFT"]


async def test_symbols_are_upper_cased_and_deduplicated(repo):
    snapshot = await repo.record_snapshot(INDEX, ["aapl", "AAPL", " msft "], taken_at=T0)
    assert snapshot.member_count == 2
    assert (await repo.members_as_of(INDEX, T0)).symbols == ["AAPL", "MSFT"]


async def test_an_index_never_snapshotted_reports_so(repo):
    members = await repo.members_as_of("never-seen-index", T0)
    assert members.observed is False
    assert members.first_observed is None


async def test_the_index_appears_in_the_listing_once_snapshotted(repo):
    await repo.record_snapshot(INDEX, ["AAPL"], taken_at=T0)
    assert INDEX in await repo.indexes()


async def test_snapshot_history_is_newest_first(repo):
    await repo.record_snapshot(INDEX, ["AAPL"], taken_at=T0)
    await repo.record_snapshot(INDEX, ["AAPL", "MSFT"], taken_at=T2)

    history = await repo.snapshots(INDEX)
    assert [s.taken_at for s in history] == [T2, T0]
    assert [s.member_count for s in history] == [2, 1]
