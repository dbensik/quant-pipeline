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

    async def write(self, records: List[MarketDataRecord]) -> None:
        """Persist a batch of MarketDataRecord objects."""
        ...

    async def fetch_range(
        self,
        symbol: str,
        asset_class: str,
        start: datetime,
        end: datetime,
        source: Optional[str] = None,
    ) -> List[MarketDataRecord]:
        """
        Return OHLCV records for *symbol* / *asset_class* within [start, end].
        Optionally filter by data *source* (e.g. 'yfinance').
        Results are ordered by time ascending.
        """
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

    async def write(self, records: List[MarketDataRecord]) -> None:
        """
        Upsert a batch of MarketDataRecord objects.

        Uses ON CONFLICT DO NOTHING on the (time, asset_id) composite PK so
        re-running the same batch is idempotent — safe for back-fill jobs.
        """
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
                .on_conflict_do_nothing(index_elements=["time", "asset_id"])
            )
            await self.session.execute(stmt)

        await self.session.commit()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def fetch_range(
        self,
        symbol: str,
        asset_class: str,
        start: datetime,
        end: datetime,
        source: Optional[str] = None,
    ) -> List[MarketDataRecord]:
        """
        Time-range query.  Joins market_data → assets so no separate lookup
        is needed. TimescaleDB's chunk exclusion makes this very fast for
        bounded date ranges.
        """
        stmt = (
            select(MarketDataORM, AssetORM)
            .join(AssetORM, MarketDataORM.asset_id == AssetORM.id)
            .where(
                AssetORM.symbol == symbol,
                AssetORM.asset_class == asset_class,
                MarketDataORM.time >= start,
                MarketDataORM.time <= end,
            )
            .order_by(MarketDataORM.time.asc())
        )
        if source:
            stmt = stmt.where(MarketDataORM.source == source)

        result = await self.session.execute(stmt)
        rows = result.all()

        return [self._orm_to_domain(md_row, asset_row) for md_row, asset_row in rows]

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
                metadata=asset.metadata,
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
