"""
db/repositories/fundamentals.py

TimescaleDB implementation of core.fundamentals.FactRepository. Insert-only:
ON CONFLICT ON CONSTRAINT uq_fundamental_fact DO NOTHING, so a re-served or
restated fact never replaces a stored one (a restatement arrives under a new
accession number and is its own row).
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.fundamentals import Fact
from db.models import FundamentalFactORM

_COLUMNS = (
    "cik",
    "taxonomy",
    "concept",
    "unit",
    "period_start",
    "period_end",
    "value",
    "fy",
    "fp",
    "form",
    "filed",
    "accn",
    "frame",
    "fetched_at",
    "source",
)
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
            # A TOTAL order: a quarter and a year-to-date figure share concept,
            # end and filing date, and the statements must not depend on which
            # the database happens to return first.
            .order_by(
                FundamentalFactORM.concept,
                FundamentalFactORM.period_end,
                FundamentalFactORM.filed,
                FundamentalFactORM.period_start.nulls_first(),
                FundamentalFactORM.accn,
                FundamentalFactORM.id,
            )
        )
        return [Fact(**{c: getattr(r, c) for c in _COLUMNS}) for r in result.scalars()]

    async def instance_values(self, facts: List[Fact]) -> Dict[Tuple, float]:
        accns = {(f.cik, f.accn) for f in facts}
        if not accns:
            return {}
        wanted = {f.key() for f in facts}
        F = FundamentalFactORM
        rows = await self.session.execute(
            select(F).where(F.source == "instance", tuple_(F.cik, F.accn).in_(list(accns)))
        )
        out = {}
        for r in rows.scalars():
            key = (r.cik, r.taxonomy, r.concept, r.unit, r.period_start, r.period_end, r.accn)
            if key in wanted:
                out[key] = r.value
        return out

    async def stored_accessions(
        self, accns: Iterable[str], taxonomies: Optional[Iterable[str]] = None
    ) -> Set[str]:
        F = FundamentalFactORM
        query = select(F.accn).where(F.accn.in_(list(accns)))
        if taxonomies is not None:
            query = query.where(F.taxonomy.in_(list(taxonomies)))
        rows = await self.session.execute(query.distinct())
        return set(rows.scalars())
