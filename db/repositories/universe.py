"""
db/repositories/universe.py

Point-in-time index membership.

The one rule this module exists to enforce: a query for a date BEFORE the
first snapshot must not silently fall back to today's membership. That
fallback is survivorship bias, and it is invisible in the output — which is
precisely why it needs a structural answer rather than a caveat in a docstring.

Phase 5 — point-in-time universe
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import UniverseMembershipORM, UniverseSnapshotORM


@dataclass
class Snapshot:
    index_name: str
    taken_at: datetime
    member_count: int
    added: List[str]
    removed: List[str]


@dataclass
class Membership:
    index_name: str
    as_of: datetime
    symbols: List[str]
    #: When the index was first and last observed. A caller can tell how much
    #: of its requested window is actually covered.
    first_observed: Optional[datetime]
    last_observed: Optional[datetime]
    observed: bool


class UniverseRepository:
    """Structural Protocol — no explicit inheritance required."""

    async def record_snapshot(
        self, index_name: str, symbols: List[str], taken_at: Optional[datetime] = None
    ) -> Snapshot:
        ...

    async def members_as_of(self, index_name: str, as_of: datetime) -> Membership:
        ...

    async def indexes(self) -> List[str]:
        ...


class TimescaleUniverseRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def record_snapshot(
        self, index_name: str, symbols: List[str], taken_at: Optional[datetime] = None
    ) -> Snapshot:
        """
        Record that `index_name` contained exactly `symbols` at `taken_at`.

        Symbols already known keep their `first_seen` and have `last_seen`
        advanced. Symbols absent from this snapshot are left alone: their stale
        `last_seen` IS the record that they have gone. Nothing is deleted, so
        a name that left the index remains queryable for the window it was in
        — which is the entire point.
        """
        taken_at = taken_at or datetime.now(timezone.utc)
        cleaned = sorted({s.upper().strip() for s in symbols if s and s.strip()})

        previous = await self._current_members(index_name, taken_at)

        for symbol in cleaned:
            statement = (
                pg_insert(UniverseMembershipORM)
                .values(
                    index_name=index_name,
                    symbol=symbol,
                    first_seen=taken_at,
                    last_seen=taken_at,
                )
                .on_conflict_do_update(
                    index_elements=["index_name", "symbol"],
                    # first_seen is NOT touched: a symbol that leaves and
                    # rejoins keeps its original observation date, and the gap
                    # is visible from the snapshot history rather than by
                    # rewriting when we first saw it.
                    set_={"last_seen": taken_at},
                )
            )
            await self.session.execute(statement)

        self.session.add(
            UniverseSnapshotORM(
                index_name=index_name,
                taken_at=taken_at,
                member_count=len(cleaned),
            )
        )
        await self.session.commit()

        current = set(cleaned)
        return Snapshot(
            index_name=index_name,
            taken_at=taken_at,
            member_count=len(cleaned),
            added=sorted(current - previous),
            removed=sorted(previous - current),
        )

    async def _current_members(self, index_name: str, before: datetime) -> set:
        result = await self.session.execute(
            select(UniverseMembershipORM.symbol).where(
                UniverseMembershipORM.index_name == index_name,
                UniverseMembershipORM.last_seen < before,
            )
        )
        rows = set(result.scalars().all())
        if not rows:
            return set()
        # Only those still present at the most recent prior snapshot count as
        # "current"; anything older than that has already left.
        latest = await self.session.execute(
            select(func.max(UniverseSnapshotORM.taken_at)).where(
                UniverseSnapshotORM.index_name == index_name,
                UniverseSnapshotORM.taken_at < before,
            )
        )
        previous_snapshot = latest.scalar_one_or_none()
        if previous_snapshot is None:
            return set()

        result = await self.session.execute(
            select(UniverseMembershipORM.symbol).where(
                UniverseMembershipORM.index_name == index_name,
                UniverseMembershipORM.last_seen >= previous_snapshot,
                UniverseMembershipORM.last_seen < before,
            )
        )
        return set(result.scalars().all())

    async def members_as_of(self, index_name: str, as_of: datetime) -> Membership:
        """
        Members observed to be in the index at `as_of`.

        Returns `observed=False` when `as_of` predates the first snapshot. The
        caller must NOT treat that as an empty index, and must not substitute
        today's membership — that substitution is the survivorship bias this
        whole table exists to prevent.
        """
        bounds = await self.session.execute(
            select(
                func.min(UniverseSnapshotORM.taken_at),
                func.max(UniverseSnapshotORM.taken_at),
            ).where(UniverseSnapshotORM.index_name == index_name)
        )
        first_observed, last_observed = bounds.first() or (None, None)

        # Compared at DAY granularity. A snapshot taken at 17:00 covers a
        # query dated 00:00 the same day — otherwise "no snapshot at or before
        # 2026-08-09; the earliest is 2026-08-09" is a self-contradicting
        # message, which is exactly what it produced.
        before_first = first_observed is not None and (
            as_of.date() < first_observed.date()
        )
        if first_observed is None or before_first:
            return Membership(
                index_name=index_name,
                as_of=as_of,
                symbols=[],
                first_observed=first_observed,
                last_observed=last_observed,
                observed=False,
            )

        # Membership is judged against the most recent snapshot AT OR BEFORE
        # `as_of`, not against `as_of` itself.
        #
        # A symbol seen on 1 June and absent on 1 December left at some unknown
        # point between them. Requiring `last_seen >= as_of` would drop it from
        # every date after 1 June, which understates the universe for five
        # months — the same survivorship error in miniature. Anchoring on the
        # last actual observation says instead: "when we last looked before
        # this date, it was there."
        # end-of-day, for the same reason: a query dated 00:00 should anchor
        # on a snapshot taken later that day.
        day_end = datetime.combine(
            as_of.date(), datetime.max.time(), tzinfo=timezone.utc
        )
        anchor = await self.session.execute(
            select(func.max(UniverseSnapshotORM.taken_at)).where(
                UniverseSnapshotORM.index_name == index_name,
                UniverseSnapshotORM.taken_at <= day_end,
            )
        )
        anchored_at = anchor.scalar_one_or_none() or as_of

        result = await self.session.execute(
            select(UniverseMembershipORM.symbol)
            .where(
                UniverseMembershipORM.index_name == index_name,
                UniverseMembershipORM.first_seen <= anchored_at,
                UniverseMembershipORM.last_seen >= anchored_at,
            )
            .order_by(UniverseMembershipORM.symbol)
        )
        return Membership(
            index_name=index_name,
            as_of=as_of,
            symbols=list(result.scalars().all()),
            first_observed=first_observed,
            last_observed=last_observed,
            observed=True,
        )

    async def indexes(self) -> List[str]:
        result = await self.session.execute(
            select(UniverseSnapshotORM.index_name)
            .distinct()
            .order_by(UniverseSnapshotORM.index_name)
        )
        return list(result.scalars().all())

    async def snapshots(self, index_name: str) -> List[Snapshot]:
        result = await self.session.execute(
            select(UniverseSnapshotORM)
            .where(UniverseSnapshotORM.index_name == index_name)
            .order_by(UniverseSnapshotORM.taken_at.desc())
        )
        return [
            Snapshot(
                index_name=row.index_name,
                taken_at=row.taken_at,
                member_count=row.member_count,
                added=[],
                removed=[],
            )
            for row in result.scalars().all()
        ]
