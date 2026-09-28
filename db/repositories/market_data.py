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

import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from core.models import Asset, OHLCV, MarketDataRecord, Timestamp
from core.price_adjustment import MODES as ADJUST_MODES
from core.price_adjustment import Action, ServedBar
from core.price_adjustment import adjust as adjust_prices
from db.models import AssetORM, CorporateActionORM, MarketDataORM

logger = logging.getLogger(__name__)


def _finite(value: Optional[float]) -> bool:
    return value is not None and not math.isnan(value)


def _num(value: Optional[float]) -> Optional[float]:
    """NaN becomes NULL: a served NaN is 'no value', not a number to store."""
    return value if _finite(value) else None


def _served_columns(record: MarketDataRecord) -> dict:
    """served_* and fetched_at for a new row, or nothing if not supplied."""
    if record.served is None or record.fetched_at is None:
        return {}
    return {
        "served_open": _num(record.served.open),
        "served_high": _num(record.served.high),
        "served_low": _num(record.served.low),
        "served_close": _num(record.served.close),
        "served_volume": _num(record.served.volume),
        "fetched_at": record.fetched_at,
    }


# ---------------------------------------------------------------------------
# Protocol — the interface every storage backend must satisfy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AssetLastBar:
    """
    The newest stored bar for one registered asset — the unit of the
    freshness surface (GET /api/v1/ingest/freshness).

    `last_bar` is None for an asset that is registered but has no bars at all,
    which is a distinct condition from "stale": the former never ingested, the
    latter stopped. `delisted_at` travels with it so the caller can exclude
    names that are legitimately frozen rather than counting them as stale.
    """

    symbol: str
    asset_class: str
    last_bar: Optional[datetime]
    delisted_at: Optional[datetime] = None


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
        adjust: str = "total",
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

        `adjust`: 'total' (default — total return, what every backtest and
        screener reads), 'split' (splits and spinoffs only — Yahoo's quoted
        Close, for charts), or 'none' (what actually traded). Honoured for
        assets whose price_basis is 'served'; others return stored values.
        """
        ...

    async def find_asset(
        self, symbol: str, asset_class: Optional[str] = None
    ) -> Optional[Asset]:
        """Return the registered Asset for *symbol*, or None if unknown."""
        ...

    async def latest_bars(self) -> List[AssetLastBar]:
        """
        Newest stored bar for EVERY registered asset, in one round trip.

        The freshness surface needs the whole registry at once; asking
        find_asset + fetch_range per symbol would be ~600 queries for a badge
        that renders on every page load.
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
                    # Served values ride along on a NEW row. An existing row
                    # gets them only through fill_served, never from here:
                    # replace=True below rewrites the adjusted columns alone.
                    **_served_columns(record),
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

    async def _asset_id(self, asset: Asset) -> Optional[int]:
        """The registered id for `asset`, without creating one."""
        return await self.session.scalar(
            select(AssetORM.id).where(
                AssetORM.symbol == asset.symbol,
                AssetORM.asset_class == asset.asset_class,
                AssetORM.source == asset.source,
            )
        )

    async def fill_served(self, records: List[MarketDataRecord]) -> int:
        """
        Set served_* and fetched_at on EXISTING rows that have none yet.
        Returns rows filled.

        Only where served_close IS NULL: a served value is written once and
        never replaced. That is what lets a stored bar stay correct when a
        later split arrives — core/price_adjustment.py derives raw from the
        value AND the time it was fetched, so replacing either would silently
        re-date it. (The 14-day ingest overlap re-serves every recent bar each
        morning; without the NULL guard it would re-stamp them all.)

        One set-based UPDATE per asset, so the phase-4 backfill of ~1M rows
        is not a million round trips.
        """
        by_asset = defaultdict(list)
        for r in records:
            if r.served is not None and r.fetched_at is not None and _finite(r.served.close):
                by_asset[(r.asset.symbol, r.asset.asset_class, r.asset.source)].append(r)

        filled = 0
        for (symbol, asset_class, source), rows in by_asset.items():
            asset_id = await self._asset_id(Asset(symbol, asset_class, source))
            if asset_id is None:
                continue
            result = await self.session.execute(
                text(
                    """
                    UPDATE market_data AS m SET
                        served_open = v.o, served_high = v.h, served_low = v.l,
                        served_close = v.c, served_volume = v.vol, fetched_at = v.f
                    FROM unnest(
                        CAST(:t AS timestamptz[]), CAST(:o AS float8[]),
                        CAST(:h AS float8[]), CAST(:l AS float8[]),
                        CAST(:c AS float8[]), CAST(:vol AS float8[]),
                        CAST(:f AS timestamptz[])
                    ) AS v(t, o, h, l, c, vol, f)
                    WHERE m.asset_id = :asset_id
                      AND m.time = v.t
                      AND m.served_close IS NULL
                    """
                ),
                {
                    "asset_id": asset_id,
                    "t": [r.ohlcv.timestamp.utc for r in rows],
                    "o": [_num(r.served.open) for r in rows],
                    "h": [_num(r.served.high) for r in rows],
                    "l": [_num(r.served.low) for r in rows],
                    "c": [_num(r.served.close) for r in rows],
                    "vol": [_num(r.served.volume) for r in rows],
                    "f": [r.fetched_at for r in rows],
                },
            )
            filled += result.rowcount or 0

        await self.session.commit()
        return filled

    async def write_actions(self, records: List[MarketDataRecord]) -> int:
        """
        Record the splits and dividends the provider reported on these bars.
        Returns actions newly recorded.

        The FIRST record of an event is kept and never updated. Yahoo serves a
        dividend in the split basis of the day it is fetched, so NVDA's 2024-03
        dividend is 0.04 fetched before its split and 0.004 after — same key,
        different value. The value is only meaningful with the fetched_at it
        came with, so the two are stored together once and left alone.
        """
        recorded = 0
        for r in records:
            events = [("dividend", r.dividend), ("split", r.split_ratio)]
            events = [(k, v) for k, v in events if v is not None and _finite(v) and v > 0]
            if not events or r.fetched_at is None:
                continue
            asset_id = await self._asset_id(r.asset)
            if asset_id is None:
                continue
            for kind, value in events:
                result = await self.session.execute(
                    text(
                        """
                        INSERT INTO corporate_actions
                            (asset_id, ex_date, kind, value, source, fetched_at)
                        VALUES (:asset_id, :ex_date, :kind, :value, :source, :fetched_at)
                        ON CONFLICT ON CONSTRAINT uq_corporate_action DO NOTHING
                        """
                    ),
                    {
                        "asset_id": asset_id,
                        "ex_date": r.ohlcv.timestamp.utc.date(),
                        "kind": kind,
                        "value": value,
                        "source": r.asset.source,
                        "fetched_at": r.fetched_at,
                    },
                )
                recorded += result.rowcount or 0

        await self.session.commit()
        return recorded

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
        adjust: str = "total",
    ) -> List[MarketDataRecord]:
        """
        Time-range query.  Joins market_data → assets so no separate lookup
        is needed. TimescaleDB's chunk exclusion makes this very fast for
        bounded date ranges.

        asset_class=None resolves by symbol alone — see the Protocol docstring.

        `adjust` applies only when config.settings.SERVED_PRICES_ENABLED is
        on, and only to assets with price_basis = 'served' (phase 5
        of research/dividend-drift-plan-2026-09-27.md): their prices are
        derived at read time from the served values and corporate_actions,
        so every bar is adjusted to the same as-of date. Every other asset
        (legacy, crypto) returns its stored columns whatever `adjust` says —
        there is nothing to derive a different basis from.
        """
        if adjust not in ADJUST_MODES:
            raise ValueError(f"adjust must be one of {ADJUST_MODES}, not {adjust!r}")

        asset_query = select(AssetORM).where(AssetORM.symbol == symbol)
        if asset_class:
            asset_query = asset_query.where(AssetORM.asset_class == asset_class)
        assets = list((await self.session.execute(asset_query)).scalars())
        if (
            settings.SERVED_PRICES_ENABLED
            and len(assets) == 1
            and assets[0].price_basis == "served"
        ):
            served = await self._fetch_served(assets[0], start, end, source, adjust)
            if served is not None:
                return served

        return await self._fetch_stored(symbol, asset_class, start, end, source)

    async def _fetch_stored(
        self,
        symbol: str,
        asset_class: Optional[str],
        start: datetime,
        end: datetime,
        source: Optional[str],
    ) -> List[MarketDataRecord]:
        """The stored (auto_adjust as of each bar's fetch) columns, as always."""
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

    async def _fetch_served(
        self,
        asset: AssetORM,
        start: datetime,
        end: datetime,
        source: Optional[str],
        adjust: str,
    ) -> Optional[List[MarketDataRecord]]:
        """
        Bars in [start, end] derived from served values, or None to fall back.

        Reads PAST `end` to the newest bar: a bar's adjustment is every event
        after it, and a dividend's factor needs the close before its ex-date.
        Nothing before `start` is needed: a dividend adjusts only bars before
        its ex-date, so if the window starts on or after the ex-date no window
        bar is affected, and if it starts earlier the bar before the ex-date
        is already in the window.

        Falls back — for the WHOLE call, with a warning — if any bar it needs
        has no served values. Never bar by bar: mixing the two bases is the
        very drift this replaces. price_basis = 'served' is only set on full
        coverage (scripts/gate_served.py), so this should not happen.
        """
        stmt = (
            select(MarketDataORM)
            .where(
                MarketDataORM.asset_id == asset.id,
                MarketDataORM.time >= start,
            )
            .order_by(MarketDataORM.time.asc())
        )
        if source:
            stmt = stmt.where(MarketDataORM.source == source)
        rows = list((await self.session.execute(stmt)).scalars())
        if not any(start <= r.time <= end for r in rows):
            return []
        if any(r.served_close is None or r.fetched_at is None for r in rows):
            logger.warning(
                "%s: price_basis is 'served' but %d bar(s) from %s lack served "
                "values; returning stored prices for this read.",
                asset.symbol,
                sum(r.served_close is None for r in rows),
                start.date(),
            )
            return None

        actions = [
            Action(a.ex_date, a.kind, a.value, a.fetched_at)
            for a in (
                await self.session.execute(
                    select(CorporateActionORM).where(
                        CorporateActionORM.asset_id == asset.id
                    )
                )
            ).scalars()
        ]
        # Bar dates are New York trading dates; stored at 00:00 UTC of that date.
        bars = [
            ServedBar(
                r.time.date(), r.served_open, r.served_high, r.served_low,
                r.served_close, r.served_volume, r.fetched_at,
            )
            for r in rows
        ]
        derived = {b.day: b for b in adjust_prices(bars, actions, adjust)}
        domain_asset = Asset(
            symbol=asset.symbol,
            asset_class=asset.asset_class,
            source=asset.source,
            metadata=asset.metadata_ or {},
        )
        out = []
        for r in rows:
            if not (start <= r.time <= end):
                continue
            b = derived[r.time.date()]
            out.append(
                MarketDataRecord(
                    asset=domain_asset,
                    ohlcv=OHLCV(
                        open=b.open, high=b.high, low=b.low, close=b.close,
                        volume=b.volume, timestamp=Timestamp(utc=r.time),
                    ),
                )
            )
        return out

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

    async def latest_bars(self) -> List[AssetLastBar]:
        """
        One aggregate over market_data, outer-joined so an asset with no bars
        still appears (last_bar=None). The whole store at ~900k rows answers
        in well under a second; measured 2026-09-14 against the live database
        before this was wired to a badge that every page renders.
        """
        stmt = (
            select(
                AssetORM.symbol,
                AssetORM.asset_class,
                AssetORM.delisted_at,
                func.max(MarketDataORM.time),
            )
            .select_from(AssetORM)
            .outerjoin(MarketDataORM, MarketDataORM.asset_id == AssetORM.id)
            .group_by(AssetORM.id, AssetORM.symbol, AssetORM.asset_class, AssetORM.delisted_at)
            .order_by(AssetORM.symbol)
        )
        rows = (await self.session.execute(stmt)).all()
        return [
            AssetLastBar(
                symbol=symbol,
                asset_class=asset_class,
                last_bar=last_bar,
                delisted_at=delisted_at,
            )
            for symbol, asset_class, delisted_at, last_bar in rows
        ]

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
