"""
db/repositories/fundamentals.py

TimescaleDB implementation of core.fundamentals.FactRepository. Insert-only:
ON CONFLICT ON CONSTRAINT uq_fundamental_fact DO NOTHING, so a re-served or
restated fact never replaces a stored one (a restatement arrives under a new
accession number and is its own row).
"""

from __future__ import annotations

from typing import Iterable, List

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.fundamentals import Fact
from db.models import FundamentalFactORM

_COLUMNS = ("cik", "taxonomy", "concept", "unit", "period_start", "period_end", "value",
            "fy", "fp", "form", "filed", "accn", "frame", "fetched_at")
_BATCH = 2000


class TimescaleFactRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert(self, facts: List[Fact]) -> int:
        inserted = 0
        for i in range(0, len(facts), _BATCH):
            chunk = facts[i : i + _BATCH]
            stmt = (
                pg_insert(FundamentalFactORM)
                .values([{c: getattr(f, c) for c in _COLUMNS} for f in chunk])
                .on_conflict_do_nothing(constraint="uq_fundamental_fact")
                .returning(FundamentalFactORM.id)
            )
            inserted += len((await self.session.execute(stmt)).fetchall())
        await self.session.commit()
        return inserted

    async def facts(self, cik: int, concepts: Iterable[str]) -> List[Fact]:
        result = await self.session.execute(
            select(FundamentalFactORM)
            .where(FundamentalFactORM.cik == cik, FundamentalFactORM.concept.in_(list(concepts)))
            .order_by(FundamentalFactORM.concept, FundamentalFactORM.period_end, FundamentalFactORM.filed)
        )
        return [Fact(**{c: getattr(r, c) for c in _COLUMNS}) for r in result.scalars()]
