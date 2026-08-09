"""
Ingestion against a REAL TimescaleDB.

The router tests substitute FakeRepo and pass explicit symbols, so they never
touch SQL. These cover what only the database can answer: that bars actually
land in `market_data`, that the upsert makes a re-run idempotent, that
registering a ticker works, and that omitting `symbols` falls back to the
asset registry.

    docker-compose up -d timescaledb
    poetry run alembic upgrade head
    poetry run pytest -m integration

The fetcher is still a stub — these test the WRITE path, not Yahoo.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text

from core.ingest import ingest_symbols
from core.models import OHLCV, Asset, MarketDataRecord, Timestamp
from db.models import AssetORM, MarketDataORM
from db.repositories.market_data import TimescaleMarketDataRepo
from db.session import get_session

# See test_portfolios_repo_integration.py — one module-level engine.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

SYMBOL = "PYTEST-INGEST"
START = datetime(2024, 6, 1, tzinfo=timezone.utc)


def stub_fetcher(count=3, symbol=SYMBOL):
    def fetch(symbols, start_date, end_date):
        begin = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return [
            MarketDataRecord(
                # Deliberately mislabelled 'equity', as yfinance_adapter does.
                asset=Asset(symbol=symbols[0], asset_class="equity", source="yfinance"),
                ohlcv=OHLCV(
                    open=10.0 + i, high=11.0 + i, low=9.0 + i, close=10.5 + i,
                    volume=100.0, timestamp=Timestamp(utc=begin + timedelta(days=i)),
                ),
            )
            for i in range(count)
        ]

    return fetch


@pytest_asyncio.fixture(loop_scope="session")
async def repo():
    """A repo with a throwaway asset registered, removed afterwards."""
    async with get_session() as session:
        instance = TimescaleMarketDataRepo(session)
        await _purge(session)

        session.add(
            AssetORM(symbol=SYMBOL, asset_class="crypto", source="yfinance")
        )
        await session.commit()
        try:
            yield instance
        finally:
            await _purge(session)


async def _purge(session):
    rows = await session.execute(select(AssetORM.id).where(AssetORM.symbol == SYMBOL))
    ids = list(rows.scalars().all())
    if ids:
        await session.execute(
            delete(MarketDataORM).where(MarketDataORM.asset_id.in_(ids))
        )
        await session.execute(delete(AssetORM).where(AssetORM.id.in_(ids)))
        await session.commit()


async def _bar_count(session) -> int:
    result = await session.execute(
        text(
            "SELECT count(*) FROM market_data m JOIN assets a ON a.id = m.asset_id "
            "WHERE a.symbol = :s"
        ),
        {"s": SYMBOL},
    )
    return result.scalar_one()


async def test_bars_actually_land_in_market_data(repo):
    """
    THE point of step 7d. The Streamlit button wrote to SQLite, so nothing it
    ingested ever reached the table the API reads.
    """
    report = await ingest_symbols(
        repo, [SYMBOL], start=START, fetcher=stub_fetcher(3)
    )
    assert report.written == 3
    assert await _bar_count(repo.session) == 3


async def test_rerunning_is_idempotent(repo):
    """
    write() upserts with ON CONFLICT DO NOTHING on (time, asset_id), so a
    re-run of the same window must not duplicate rows.
    """
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(3))
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(3))
    assert await _bar_count(repo.session) == 3


async def test_the_asset_class_on_disk_is_not_overwritten(repo):
    """
    The fixture registers PYTEST-INGEST as crypto while the stub fetcher
    labels its records 'equity', as yfinance_adapter does. Writing them
    unretagged would create a SECOND asset row for the same symbol.
    """
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(2))

    rows = await repo.session.execute(
        select(AssetORM.asset_class).where(AssetORM.symbol == SYMBOL)
    )
    classes = list(rows.scalars().all())
    assert classes == ["crypto"], "a duplicate asset row was created"


async def test_a_second_run_resumes_rather_than_refetching(repo):
    calls = []

    def recording(symbols, start_date, end_date):
        calls.append(start_date)
        return stub_fetcher(3)(symbols, start_date, end_date)

    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=recording)
    await ingest_symbols(repo, [SYMBOL], fetcher=recording)

    # First run wrote 2024-06-01..03; the resume must begin on the 4th.
    assert calls[1] == (START + timedelta(days=3)).strftime("%Y-%m-%d")


async def test_ingested_bars_are_readable_through_the_repository(repo):
    """Round trip: what ingestion wrote is what fetch_range returns."""
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(3))

    records = await repo.fetch_range(
        symbol=SYMBOL,
        asset_class=None,
        start=START - timedelta(days=1),
        end=START + timedelta(days=10),
    )
    assert len(records) == 3
    assert records[0].ohlcv.close == pytest.approx(10.5)


async def test_the_registry_fallback_finds_symbols(repo):
    """
    Omitting `symbols` ingests everything registered. Covered here rather than
    in the router tests, where the equivalent test passed only by silently
    reaching the running container.
    """
    from api.routers.ingest import all_registered_symbols

    symbols = await all_registered_symbols(repo.session)
    assert SYMBOL in symbols
    assert len(symbols) > 1


async def test_empty_bars_never_reach_the_table(repo):
    def all_null(symbols, start_date, end_date):
        begin = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return [
            MarketDataRecord(
                asset=Asset(symbol=symbols[0], asset_class="equity", source="yfinance"),
                ohlcv=OHLCV(
                    open=None, high=None, low=None, close=None, volume=None,
                    timestamp=Timestamp(utc=begin),
                ),
            )
        ]

    report = await ingest_symbols(repo, [SYMBOL], start=START, fetcher=all_null)
    assert report.outcomes[0].skipped_empty == 1
    assert await _bar_count(repo.session) == 0


# ---------------------------------------------------------------------------
# Re-adjustment (corporate actions)
# ---------------------------------------------------------------------------

async def test_a_plain_rerun_does_not_change_stored_prices(repo):
    """
    ON CONFLICT DO NOTHING: an incremental re-run is idempotent and cheap.
    """
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(2))

    def different_prices(symbols, start_date, end_date):
        records = stub_fetcher(2)(symbols, start_date, end_date)
        for r in records:
            r.ohlcv.close = 999.0
        return records

    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=different_prices)

    rows = await repo.session.execute(
        text(
            "SELECT m.close FROM market_data m JOIN assets a ON a.id=m.asset_id "
            "WHERE a.symbol = :s ORDER BY m.time"
        ),
        {"s": SYMBOL},
    )
    assert 999.0 not in [r[0] for r in rows]


async def test_full_backfill_restates_existing_bars(repo):
    """
    THE fix for split drift, asserted against real SQL.

    yfinance re-adjusts a whole series for splits as of the fetch date, so the
    only way to remove a discontinuity is to overwrite the stored bars. With
    ON CONFLICT DO NOTHING that was impossible: the corrected bars collided and
    were discarded, and the run still reported thousands of bars written.
    """
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(2))

    def readjusted(symbols, start_date, end_date):
        records = stub_fetcher(2)(symbols, start_date, end_date)
        for r in records:
            r.ohlcv.close = r.ohlcv.close / 10.0  # as a 10:1 split would
        return records

    await ingest_symbols(
        repo, [SYMBOL], start=START, full_backfill=True, fetcher=readjusted
    )

    rows = await repo.session.execute(
        text(
            "SELECT m.close FROM market_data m JOIN assets a ON a.id=m.asset_id "
            "WHERE a.symbol = :s ORDER BY m.time"
        ),
        {"s": SYMBOL},
    )
    closes = [r[0] for r in rows]
    assert closes == pytest.approx([1.05, 1.15])


async def test_full_backfill_does_not_duplicate_rows(repo):
    """Overwriting must update in place, not append a second bar per date."""
    await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(3))
    await ingest_symbols(
        repo, [SYMBOL], start=START, full_backfill=True, fetcher=stub_fetcher(3)
    )
    assert await _bar_count(repo.session) == 3


async def test_written_reports_rows_the_database_accepted(repo):
    """
    A second incremental run persists nothing, and must say so. This used to
    report the submitted count, so a no-op refresh looked like a success.
    """
    first = await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(3))
    assert first.written == 3

    second = await ingest_symbols(repo, [SYMBOL], start=START, fetcher=stub_fetcher(3))
    assert second.written == 0
