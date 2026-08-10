"""
db/repositories/market_data.py

MarketDataRepository Protocol (structural interface) + TimescaleDB implementation.

Design notes
------------
* The Protocol keeps the domain layer decoupled from any specific storage engine.
  Swap TimescaleMarketDataRepo for an in-memory stub in unit tests by satisfying
  the same Protocol — no monkey-patching or mocking needed.

* _get_or_create_asset uses SELECT-then-INSERT with a conflict guard so concurrent
  writes on the same asset row don't race. PostgreSQL 15+ handles ON CONFLICT DO
  NOTHING reliably under READ COMMITTED.

* write() flushes inside a single transaction per call. If any row fails, the
  entire batch rolls back. Callers can retry or split the batch.

Phase 2 — TimescaleDB Schema & Repository Layer
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Asset, OHLCV, MarketDataRecord, Timestamp
from db.models import AssetORM, MarketDataORM


# ---------------------------------------------------------------------------
# Protocol — the interface every storage backend must satisfy
# ---------------------------------------------------------------------------

class MarketDataRepository:
    """
    Structural Protocol.  Any class implementing these two async methods is a
    valid MarketDataRepository — no explicit inheritance required.
    """

    async def write(
        self, records: List[MarketDataRecord], replace: bool = False
    ) -> int:
        """
        Persist a batch of MarketDataRecord objects. Returns rows persisted.

        `replace=True` overwrites bars that already exist. Required for a
        re-adjustment pass — see the implementation for why.
        """
        ...

    async def fetch_range(
        self,
        symbol: str,
        asset_class: Optional[str],
        start: datetime,
        end: datetime,
        source: Optional[str] = None,
    ) -> List[MarketDataRecord]:
        """
        Return OHLCV records for *symbol* within [start, end].

        Pass asset_class=None to resolve by symbol alone — API consumers know a
        symbol, not its asset class, and deriving one in the API layer would
        duplicate the migration's '-USD' heuristic (a decision about legacy
        data, not an API contract). Symbol is not unique by the
        uq_asset_identity constraint in principle, so callers that genuinely
        need disambiguation should still pass asset_class.

        Optionally filter by data *source* (e.g. 'yfinance').
        Results are ordered by time ascending.
        """
        ...

    async def find_asset(
        self, symbol: str, asset_class: Optional[str] = None
    ) -> Optional[Asset]:
        """Return the registered Asset for *symbol*, or None if unknown."""
        ...


# ---------------------------------------------------------------------------
# TimescaleDB implementation
# ---------------------------------------------------------------------------

class TimescaleMarketDataRepo:
    """
    Concrete implementation backed by TimescaleDB (PostgreSQL + TimescaleDB extension).

    Injected with an AsyncSession from db/session.py so the caller controls
    transaction boundaries and session lifetime.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def write(
        self, records: List[MarketDataRecord], replace: bool = False
    ) -> int:
        """
        Upsert a batch of MarketDataRecord objects. Returns rows persisted.

        DEFAULT (replace=False) is ON CONFLICT DO NOTHING on the (time,
        asset_id) primary key, so re-running an incremental batch is
        idempotent and cheap.

        replace=True is ON CONFLICT DO UPDATE, and exists because DO NOTHING
        made `full_backfill` a silent no-op. yfinance's auto_adjust restates
        the WHOLE series for splits as of the fetch date, so a symbol that
        split after its bars were stored has two segments adjusted to
        different as-of dates. Measured 2026-08-09: NFLX closed 1260.27 on
        2025-07-15 and 125.03 on 2025-07-16 — a 10:1 split in November 2025
        applied to the newer segment only, which every strategy reads as a
        -90% day. Re-fetching could not fix it because every corrected bar
        collided with an existing row and was discarded.

        The return value matters for the same reason: the ingest report used
        to count rows SUBMITTED, so a run that persisted nothing still
        reported 39,707 bars written.
        """
        persisted = 0
        for record in records:
            asset_id = await self._get_or_create_asset(record.asset)
            stmt = (
                pg_insert(MarketDataORM)
                .values(
                    time=record.ohlcv.timestamp.utc,
                    asset_id=asset_id,
                    open=record.ohlcv.open,
                    high=record.ohlcv.high,
                    low=record.ohlcv.low,
                    close=record.ohlcv.close,
                    volume=record.ohlcv.volume,
                    source=record.asset.source,
                )
            )
            if replace:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["time", "asset_id"],
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                        "source": stmt.excluded.source,
                    },
                )
            else:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["time", "asset_id"]
                )
            result = await self.session.execute(stmt)
            # rowcount is 0 for a skipped conflict, 1 for an insert or update.
            persisted += result.rowcount or 0

        await self.session.commit()
        return persisted

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def fetch_range(
        self,
        symbol: str,
        asset_class: Optional[str],
        start: datetime,
        end: datetime,
        source: Optional[str] = None,
    ) -> List[MarketDataRecord]:
        """
        Time-range query.  Joins market_data → assets so no separate lookup
        is needed. TimescaleDB's chunk exclusion makes this very fast for
        bounded date ranges.

        asset_class=None resolves by symbol alone — see the Protocol docstring.
        """
        stmt = (
            select(MarketDataORM, AssetORM)
            .join(AssetORM, MarketDataORM.asset_id == AssetORM.id)
            .where(
                AssetORM.symbol == symbol,
                MarketDataORM.time >= start,
                MarketDataORM.time <= end,
            )
            .order_by(MarketDataORM.time.asc())
        )
        if asset_class:
            stmt = stmt.where(AssetORM.asset_class == asset_class)
        if source:
            stmt = stmt.where(MarketDataORM.source == source)

        result = await self.session.execute(stmt)
        rows = result.all()

        return [self._orm_to_domain(md_row, asset_row) for md_row, asset_row in rows]

    async def find_asset(
        self, symbol: str, asset_class: Optional[str] = None
    ) -> Optional[Asset]:
        """
        Look up a registered asset by symbol.

        Lets callers distinguish "unknown symbol" (404) from "known symbol, no
        bars in this date range" (200 with an empty list) — fetch_range alone
        returns [] for both.
        """
        stmt = select(AssetORM).where(AssetORM.symbol == symbol)
        if asset_class:
            stmt = stmt.where(AssetORM.asset_class == asset_class)

        row = (await self.session.execute(stmt.limit(1))).scalar_one_or_none()
        if row is None:
            return None
        return Asset(
            symbol=row.symbol,
            asset_class=row.asset_class,
            source=row.source,
            metadata=row.metadata_ or {},
            delisted_at=row.delisted_at,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _get_or_create_asset(self, asset: Asset) -> int:
        """
        Return the PK of an existing AssetORM row, or insert and return
        the new PK.  Uses INSERT … ON CONFLICT DO NOTHING so concurrent
        callers converge on the same row safely.
        """
        # Try SELECT first (hot path — asset already exists)
        stmt = select(AssetORM.id).where(
            AssetORM.symbol == asset.symbol,
            AssetORM.asset_class == asset.asset_class,
            AssetORM.source == asset.source,
        )
        existing = await self.session.scalar(stmt)
        if existing is not None:
            return existing

        # INSERT with conflict guard
        insert_stmt = (
            pg_insert(AssetORM)
            .values(
                symbol=asset.symbol,
                asset_class=asset.asset_class,
                source=asset.source,
                # NOTE: must be metadata_, not metadata. The DB column is named
                # "metadata" but the mapped attribute is metadata_ — plain
                # `metadata=` resolves to the declarative Base.metadata MetaData
                # object and raises AttributeError on compile.
                metadata_=asset.metadata,
            )
            .on_conflict_do_nothing(constraint="uq_asset_identity")
            .returning(AssetORM.id)
        )
        result = await self.session.execute(insert_stmt)
        new_id = result.scalar_one_or_none()

        if new_id is not None:
            return new_id

        # Race: another writer inserted between our SELECT and INSERT.
        # Re-run the SELECT — the row is guaranteed to exist now.
        return await self.session.scalar(stmt)  # type: ignore[return-value]

    @staticmethod
    def _orm_to_domain(md: MarketDataORM, asset: AssetORM) -> MarketDataRecord:
        """Convert ORM rows back to canonical domain objects."""
        return MarketDataRecord(
            asset=Asset(
                symbol=asset.symbol,
                asset_class=asset.asset_class,
                source=asset.source,
                metadata=asset.metadata_ or {},
            ),
            ohlcv=OHLCV(
                open=md.open,
                high=md.high,
                low=md.low,
                close=md.close,
                volume=md.volume,
                timestamp=Timestamp(utc=md.time),
            ),
        )

    # -- corporate actions ---------------------------------------------------

    async def mark_full_refresh(self, symbol: str, when: datetime) -> None:
        """
        Record that `symbol`'s whole series has been restated.

        A split newer than this timestamp means the stored bars are adjusted to
        a stale as-of date — see core/corporate_actions.py.
        """
        await self.session.execute(
            AssetORM.__table__.update()
            .where(AssetORM.symbol == symbol)
            .values(last_full_refresh_at=when, delisted_at=None)
        )
        await self.session.commit()

    async def mark_delisted(self, symbol: str, when: datetime) -> None:
        """Record that `symbol` appears to have stopped trading."""
        await self.session.execute(
            AssetORM.__table__.update()
            .where(AssetORM.symbol == symbol)
            .values(delisted_at=when)
        )
        await self.session.commit()

    async def refresh_state(self, symbol: str) -> tuple:
        """(last_full_refresh_at, delisted_at) for `symbol`."""
        result = await self.session.execute(
            select(AssetORM.last_full_refresh_at, AssetORM.delisted_at).where(
                AssetORM.symbol == symbol
            )
        )
        row = result.first()
        return (row[0], row[1]) if row else (None, None)
