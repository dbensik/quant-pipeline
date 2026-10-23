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

    async def equity(self, symbol: str) -> Optional[Tuple[List[int], Optional[str]]]:
        """
        ([cik, *predecessors], sector) for a registered equity, None if not
        one. The list is empty when no CIK is recorded.
        """
        row = (
            await self.session.execute(
                select(AssetORM.metadata_).where(
                    AssetORM.symbol == symbol.upper(), AssetORM.asset_class == "equity"
                )
            )
        ).first()
        if row is None:
            return None
        meta = row[0] or {}
        cik = meta.get("cik")
        if cik is None:
            return ([], meta.get("sector"))
        return (
            [int(cik), *[int(p) for p in meta.get("cik_predecessors") or []]],
            meta.get("sector"),
        )

    async def share_classes(self, symbols: Iterable[str]) -> Dict[str, List[str]]:
        """
        Each of `symbols` that is a registered equity -> the other registered
        equities with the same own CIK (predecessor CIKs are not read: they
        name an earlier filer, not a second class). Measured 2026-10-22: the
        registry's only shared CIKs are GOOG/GOOGL, FOX/FOXA and NWS/NWSA.
        """
        wanted = {s.upper() for s in symbols}
        rows = (
            await self.session.execute(
                select(AssetORM.symbol, AssetORM.metadata_).where(AssetORM.asset_class == "equity")
            )
        ).all()
        by_cik: Dict[int, List[str]] = {}
        own: Dict[str, Optional[int]] = {}
        for symbol, meta in rows:
            cik = (meta or {}).get("cik")
            own[symbol] = int(cik) if cik is not None else None
            if cik is not None:
                by_cik.setdefault(int(cik), []).append(symbol)
        return {
            s: sorted(o for o in by_cik.get(own[s], []) if o != s) if own[s] is not None else []
            for s in wanted
            if s in own
        }

    async def news_identities(self, symbols: Iterable[str]) -> Dict[str, Tuple[str, Optional[str]]]:
        """
        Each of `symbols` registered as an equity, ETF or coin -> (asset
        class, metadata.verified_name): what the Research page needs to
        search it by the archive's terms (core.news_archive.search_terms).
        """
        wanted = sorted({s.upper() for s in symbols})
        if not wanted:
            return {}
        rows = (
            await self.session.execute(
                select(
                    AssetORM.symbol,
                    AssetORM.asset_class,
                    AssetORM.metadata_["verified_name"].astext,
                ).where(
                    AssetORM.symbol.in_(wanted),
                    AssetORM.asset_class.in_(["equity", "etf", "crypto"]),
                )
            )
        ).all()
        return {symbol: (asset_class, name) for symbol, asset_class, name in rows}

    async def facts(self, ciks: Iterable[int], concepts: Iterable[str]) -> List[Fact]:
        concepts = list(concepts)
        out: List[Fact] = []
        for cik in ciks:
            out.extend(await self._facts.facts(cik, concepts))
        return out

    async def splits(self, symbol: str) -> List[Tuple[date, float]]:
        rows = await self.session.execute(
            select(CorporateActionORM.ex_date, CorporateActionORM.value)
            .join(AssetORM, AssetORM.id == CorporateActionORM.asset_id)
            .where(AssetORM.symbol == symbol.upper(), CorporateActionORM.kind == "split")
            .order_by(CorporateActionORM.ex_date)
        )
        return [(r.ex_date, float(r.value)) for r in rows]

    async def filing_dates(self, ciks: Iterable[int]) -> List[date]:
        rows = await self.session.execute(
            select(FundamentalFactORM.filed)
            .distinct()
            .where(
                FundamentalFactORM.cik.in_(list(ciks)),
                FundamentalFactORM.form.in_(["10-K", "10-Q"]),
            )
            .order_by(FundamentalFactORM.filed)
        )
        return [r.filed for r in rows]

    async def coverage(self) -> Dict[str, object]:
        equities = (
            await self.session.execute(
                text(
                    "SELECT count(*), count(*) FILTER (WHERE metadata ? 'cik') FROM assets WHERE asset_class = 'equity'"
                )
            )
        ).one()
        facts = (
            await self.session.execute(
                select(
                    func.count(),
                    func.count(func.distinct(FundamentalFactORM.cik)),
                    func.max(FundamentalFactORM.filed),
                )
            )
        ).one()
        return {
            "equities": int(equities[0]),
            "with_cik": int(equities[1]),
            "facts": int(facts[0]),
            "ciks_with_facts": int(facts[1]),
            "newest_filed": facts[2],
        }
