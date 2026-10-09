"""
db/repositories/valuation.py

What a valuation reads besides prices and rates: an equity's CIK and sector
from the registry, its stored facts, its splits, the dates its 10-K/10-Qs
were filed, and coverage counts. One Protocol (core/valuation_service.py)
so tests/api/ runs on a fake.
"""

from __future__ import annotations

from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.fundamentals import Fact
from db.models import AssetORM, CorporateActionORM, FundamentalFactORM
from db.repositories.fundamentals import TimescaleFactRepo


class TimescaleValuationRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self._facts = TimescaleFactRepo(session)

    async def equity(self, symbol: str) -> Optional[Tuple[Optional[int], Optional[str]]]:
        """(cik, sector) for a registered equity, None if not one."""
        row = (await self.session.execute(
            select(AssetORM.metadata_).where(AssetORM.symbol == symbol.upper(), AssetORM.asset_class == "equity")
        )).first()
        if row is None:
            return None
        meta = row[0] or {}
        cik = meta.get("cik")
        return (int(cik) if cik is not None else None, meta.get("sector"))

    async def facts(self, cik: int, concepts: Iterable[str]) -> List[Fact]:
        return await self._facts.facts(cik, concepts)

    async def splits(self, symbol: str) -> List[Tuple[date, float]]:
        rows = await self.session.execute(
            select(CorporateActionORM.ex_date, CorporateActionORM.value)
            .join(AssetORM, AssetORM.id == CorporateActionORM.asset_id)
            .where(AssetORM.symbol == symbol.upper(), CorporateActionORM.kind == "split")
            .order_by(CorporateActionORM.ex_date)
        )
        return [(r.ex_date, float(r.value)) for r in rows]

    async def filing_dates(self, cik: int) -> List[date]:
        rows = await self.session.execute(
            select(FundamentalFactORM.filed).distinct()
            .where(FundamentalFactORM.cik == cik, FundamentalFactORM.form.in_(["10-K", "10-Q"]))
            .order_by(FundamentalFactORM.filed)
        )
        return [r.filed for r in rows]

    async def coverage(self) -> Dict[str, object]:
        equities = (await self.session.execute(text(
            "SELECT count(*), count(*) FILTER (WHERE metadata ? 'cik') FROM assets WHERE asset_class = 'equity'"
        ))).one()
        facts = (await self.session.execute(
            select(func.count(), func.count(func.distinct(FundamentalFactORM.cik)), func.max(FundamentalFactORM.filed))
        )).one()
        return {
            "equities": int(equities[0]),
            "with_cik": int(equities[1]),
            "facts": int(facts[0]),
            "ciks_with_facts": int(facts[1]),
            "newest_filed": facts[2],
        }
