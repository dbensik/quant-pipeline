"""
scripts/register_new_members.py against real Postgres (decision 44).

Drives the WRITE path the 06:00 job takes when an index gains an equity: a
throwaway index and ticker, SEC's map and the price fetch faked, everything
it wrote removed afterwards.

    poetry run pytest -m integration tests/api/test_register_new_members_integration.py
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from db.models import AssetORM, UniverseMembershipORM, UniverseSnapshotORM
from db.repositories.universe import TimescaleUniverseRepo
from db.session import get_session
from scripts import register_new_members as script

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
]

INDEX = "pytest-joiners"
JOINER, RENAMED, HOLDER = "ZZJOIN", "ZZRENAMED", "ZZHOLDER"


async def _purge(session):
    await session.execute(
        delete(UniverseMembershipORM).where(UniverseMembershipORM.index_name == INDEX)
    )
    await session.execute(
        delete(UniverseSnapshotORM).where(UniverseSnapshotORM.index_name == INDEX)
    )
    await session.execute(delete(AssetORM).where(AssetORM.symbol.in_([JOINER, RENAMED, HOLDER])))
    await session.commit()


@pytest_asyncio.fixture(loop_scope="session")
async def joiners(monkeypatch):
    async with get_session() as session:
        await _purge(session)
        # HOLDER is registered under CIK 2 already; RENAMED joins under it.
        session.add(
            AssetORM(symbol=HOLDER, asset_class="equity", source="yfinance", metadata_={"cik": 2})
        )
        await session.commit()
        await TimescaleUniverseRepo(session).record_snapshot(
            INDEX, [JOINER, RENAMED], datetime.now(timezone.utc) - timedelta(minutes=1)
        )

        class FakeSEC:
            def company_tickers(self):
                return {
                    "0": {"cik_str": 1, "ticker": JOINER, "title": "Joiner Inc"},
                    "1": {"cik_str": 2, "ticker": RENAMED, "title": "Holder Renamed Inc"},
                }

        fetched = []

        async def fake_ingest(repo, symbols, start, skip_delisted):
            fetched.extend(symbols)

        monkeypatch.setattr(script.settings, "NEW_MEMBER_INDEXES", {INDEX: ("equity", "Test")})
        monkeypatch.setattr("core.sec.SECGateway", FakeSEC)
        monkeypatch.setattr(script, "ingest_symbols", fake_ingest)
        try:
            yield fetched
        finally:
            await _purge(session)


async def test_a_joiner_is_registered_and_a_rename_is_flagged_not_registered(joiners, capsys):
    code = await script.run(dry_run=False)
    out = capsys.readouterr().out

    async with get_session() as session:
        rows = {
            r.symbol: r
            for r in (
                await session.execute(
                    select(AssetORM).where(AssetORM.symbol.in_([JOINER, RENAMED]))
                )
            ).scalars()
        }
    assert JOINER in rows and RENAMED not in rows
    assert rows[JOINER].asset_class == "equity"
    assert rows[JOINER].metadata_["cik"] == 1
    assert rows[JOINER].metadata_["index"] == "Test"
    assert joiners == [JOINER]
    assert f"RENAME? {RENAMED}: CIK 2 is already registered as {HOLDER}" in out
    # The fake fetch wrote no bars, so the joiner is reported empty, and a
    # rename candidate needs a human either way.
    assert f"EMPTY {JOINER}" in out
    assert code == 1
