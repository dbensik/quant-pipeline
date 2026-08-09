"""
TimescaleWatchlistRepo — integration tests against a REAL TimescaleDB.

The router tests substitute FakeWatchlistRepo, so they cover the HTTP contract
but never touch SQL. What lives only in the database: the unique constraint on
(watchlist_id, symbol), ON DELETE CASCADE, `position` ordering surviving a
round trip, and the indexed reverse lookup.

Replacement semantics matter most here. save_watchlist clears the child rows
and re-inserts, which under delete-orphan is the difference between a working
save and a UNIQUE violation on the second write of the same ticker.

    docker-compose up -d timescaledb
    poetry run alembic upgrade head
    poetry run pytest -m integration

Phase 5 — decommissioning Streamlit
"""

import pytest
import pytest_asyncio
from sqlalchemy import text

from db.repositories.watchlists import TimescaleWatchlistRepo
from db.session import get_session

# See test_portfolios_repo_integration.py: one module-level engine means the
# pooled connections bind to the first event loop.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

NAME = "pytest-watchlist-integration"


@pytest_asyncio.fixture(loop_scope="session")
async def repo():
    async with get_session() as session:
        instance = TimescaleWatchlistRepo(session)
        await instance.delete_watchlist(NAME)
        try:
            yield instance
        finally:
            await instance.delete_watchlist(NAME)


async def test_save_and_read_back(repo):
    saved = await repo.save_watchlist(NAME, ["AAPL", "MSFT"])
    assert saved.symbols == ["AAPL", "MSFT"]

    found = await repo.get_watchlist(NAME)
    assert found.symbols == ["AAPL", "MSFT"]
    assert found.created_at is not None


async def test_order_survives_the_round_trip(repo):
    """
    Rows come back in whatever order the database feels like without an
    explicit sort, so `position` is what preserves the user's arrangement.
    """
    symbols = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN"]
    await repo.save_watchlist(NAME, symbols)
    assert (await repo.get_watchlist(NAME)).symbols == symbols


async def test_resaving_replaces_without_violating_the_unique_constraint(repo):
    """
    THE integration-only case. save_watchlist clears the child collection and
    re-inserts; if the delete is not flushed before the insert, re-saving a
    list that keeps any ticker raises UNIQUE violation on
    (watchlist_id, symbol). A fake cannot exhibit that.
    """
    await repo.save_watchlist(NAME, ["AAPL", "MSFT", "NVDA"])
    await repo.save_watchlist(NAME, ["AAPL", "TSLA"])
    assert (await repo.get_watchlist(NAME)).symbols == ["AAPL", "TSLA"]


async def test_replacement_leaves_no_orphan_rows(repo):
    await repo.save_watchlist(NAME, ["AAPL", "MSFT", "NVDA"])
    await repo.save_watchlist(NAME, ["AAPL"])

    result = await repo.session.execute(
        text(
            "SELECT count(*) FROM watchlist_symbols s "
            "JOIN watchlists w ON w.id = s.watchlist_id WHERE w.name = :name"
        ),
        {"name": NAME},
    )
    assert result.scalar_one() == 1


async def test_duplicates_are_collapsed_before_insert(repo):
    saved = await repo.save_watchlist(NAME, ["AAPL", "aapl", "MSFT"])
    assert saved.symbols == ["AAPL", "MSFT"]


async def test_deleting_cascades_to_symbols(repo):
    await repo.save_watchlist(NAME, ["AAPL", "MSFT"])
    assert await repo.delete_watchlist(NAME) is True

    orphans = await repo.session.execute(
        text(
            "SELECT count(*) FROM watchlist_symbols WHERE watchlist_id NOT IN "
            "(SELECT id FROM watchlists)"
        )
    )
    assert orphans.scalar_one() == 0


async def test_deleting_an_absent_watchlist_returns_false(repo):
    assert await repo.delete_watchlist("no-such-watchlist-xyz") is False


async def test_reverse_lookup_finds_the_watchlist(repo):
    await repo.save_watchlist(NAME, ["AAPL", "MSFT"])
    assert NAME in await repo.watchlists_containing("AAPL")
    assert NAME not in await repo.watchlists_containing("ZZZZ")


async def test_reverse_lookup_is_case_insensitive(repo):
    await repo.save_watchlist(NAME, ["AAPL"])
    assert NAME in await repo.watchlists_containing("aapl")


async def test_listing_does_not_duplicate_a_row_per_symbol(repo):
    """selectin loading returns one row per child without .unique()."""
    await repo.save_watchlist(NAME, ["AAPL", "MSFT", "NVDA"])
    listed = [w for w in await repo.list_watchlists() if w.name == NAME]
    assert len(listed) == 1
    assert len(listed[0].symbols) == 3


async def test_an_empty_list_is_a_valid_state(repo):
    await repo.save_watchlist(NAME, ["AAPL"])
    saved = await repo.save_watchlist(NAME, [])
    assert saved.symbols == []
    assert await repo.get_watchlist(NAME) is not None
