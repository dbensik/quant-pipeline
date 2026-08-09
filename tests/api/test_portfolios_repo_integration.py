"""
TimescalePortfolioRepo — integration tests against a REAL TimescaleDB.

WHY THESE EXIST ALONGSIDE test_portfolios_router.py
    The router tests substitute FakePortfolioRepo through the Protocol, so
    they verify the HTTP contract but never touch SQL. The behaviours below
    live entirely in the database and a fake can only assert that the fake
    implements them: ON DELETE CASCADE, the unique constraint on name, trade
    ids being scoped to their portfolio, and reading the log in timestamp
    order (average cost is order-dependent).

RUNNING THEM
    docker-compose up -d timescaledb
    poetry run alembic upgrade head
    poetry run pytest -m integration

    `pytest tests/` skips this file, so the default suite still needs no
    Docker.

Each test creates and removes its own uniquely-named portfolio, so a failed
run leaves nothing behind that would break the next one.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text

from core.portfolio import Trade, derive_state
from db.repositories.portfolios import TimescalePortfolioRepo
from db.session import get_session

# loop_scope="session", NOT the default per-test loop. db/session.py builds ONE
# module-level async engine, and its pooled asyncpg connections bind to the
# event loop that first used them. With a fresh loop per test, the second test
# to reuse a pooled connection dies in teardown with "Event loop is closed".
pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

START = datetime(2024, 1, 1, tzinfo=timezone.utc)
NAME = "pytest-portfolio-integration"


def trade(ticker, action, quantity, price, day=0):
    return Trade(
        ticker=ticker,
        action=action,
        quantity=quantity,
        price=price,
        ts=START + timedelta(days=day),
    )


@pytest_asyncio.fixture(loop_scope="session")
async def repo():
    """A repository bound to a real session, with the test portfolio removed
    before and after so a previous failure cannot poison the run."""
    async with get_session() as session:
        instance = TimescalePortfolioRepo(session)
        await instance.delete_portfolio(NAME)
        try:
            yield instance
        finally:
            await instance.delete_portfolio(NAME)


async def test_create_and_read_back(repo):
    await repo.create_portfolio(NAME, 50_000.0, {"note": "integration"})
    found = await repo.get_portfolio(NAME)
    assert found is not None
    assert found.initial_cash == 50_000.0
    assert found.metadata == {"note": "integration"}
    assert found.created_at is not None


async def test_duplicate_name_raises(repo):
    await repo.create_portfolio(NAME, 50_000.0)
    with pytest.raises(ValueError):
        await repo.create_portfolio(NAME, 10_000.0)


async def test_unknown_portfolio_is_none(repo):
    assert await repo.get_portfolio("no-such-portfolio-xyz") is None


async def test_trades_come_back_in_timestamp_order(repo):
    """
    Average cost is order-dependent, so the log must read chronologically
    regardless of insertion order.
    """
    await repo.create_portfolio(NAME, 100_000.0)
    await repo.add_trade(NAME, trade("AAPL", "BUY", 10, 300.0, day=2))
    await repo.add_trade(NAME, trade("AAPL", "BUY", 10, 100.0, day=0))
    await repo.add_trade(NAME, trade("AAPL", "BUY", 10, 200.0, day=1))

    found = await repo.get_portfolio(NAME)
    assert [t.price for t in found.trades] == [100.0, 200.0, 300.0]
    assert derive_state(found.trades, found.initial_cash).positions[0].average_price == 200.0


async def test_listing_omits_trade_logs(repo):
    await repo.create_portfolio(NAME, 100_000.0)
    await repo.add_trade(NAME, trade("AAPL", "BUY", 10, 100.0))

    listed = [p for p in await repo.list_portfolios() if p.name == NAME]
    assert len(listed) == 1, "selectin loading must not duplicate the row per trade"
    assert listed[0].trades == []


async def test_deleting_a_portfolio_cascades_to_its_trades(repo):
    """
    ON DELETE CASCADE, asserted against the table rather than the ORM — an
    orphaned trade row would otherwise sit invisible behind the relationship.
    """
    await repo.create_portfolio(NAME, 100_000.0)
    await repo.add_trade(NAME, trade("AAPL", "BUY", 10, 100.0))

    result = await repo.session.execute(
        text(
            "SELECT count(*) FROM portfolio_trades t "
            "JOIN portfolios p ON p.id = t.portfolio_id WHERE p.name = :name"
        ),
        {"name": NAME},
    )
    assert result.scalar_one() == 1

    assert await repo.delete_portfolio(NAME) is True

    orphans = await repo.session.execute(
        text(
            "SELECT count(*) FROM portfolio_trades WHERE portfolio_id NOT IN "
            "(SELECT id FROM portfolios)"
        )
    )
    assert orphans.scalar_one() == 0


async def test_deleting_an_absent_portfolio_returns_false(repo):
    assert await repo.delete_portfolio("no-such-portfolio-xyz") is False


async def test_trade_deletion_is_scoped_to_its_portfolio(repo):
    """
    A trade id from one portfolio must not be deletable through another, or a
    caller could reach into a log they did not name.
    """
    other = f"{NAME}-other"
    await repo.delete_portfolio(other)
    await repo.create_portfolio(NAME, 100_000.0)
    await repo.create_portfolio(other, 100_000.0)
    try:
        stored = await repo.add_trade(NAME, trade("AAPL", "BUY", 10, 100.0))

        assert await repo.delete_trade(other, stored.id) is False
        assert len((await repo.get_portfolio(NAME)).trades) == 1

        assert await repo.delete_trade(NAME, stored.id) is True
        assert (await repo.get_portfolio(NAME)).trades == []
    finally:
        await repo.delete_portfolio(other)


async def test_non_numeric_trade_id_is_rejected_not_raised(repo):
    await repo.create_portfolio(NAME, 100_000.0)
    assert await repo.delete_trade(NAME, "not-an-id") is False


async def test_adding_a_trade_to_an_absent_portfolio_returns_none(repo):
    assert await repo.add_trade("no-such-portfolio-xyz", trade("AAPL", "BUY", 1, 1.0)) is None
