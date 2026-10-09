"""
core/fundamentals.py

THE write path for SEC fundamentals: company facts -> allow-listed raw rows
-> `fundamental_facts`, insert-only. Tier 3 phase 1 of
research/dcf-plan-2026-10-08.md. Mirrors core/ingest.py for prices: the
fetch is injected (a gateway, or a saved file), nothing is ever rewritten.

WHAT IS STORED
    Every row of every concept in settings.FUNDAMENTAL_CONCEPTS, as the SEC
    served it: value, unit, period (start is NULL for an instant), fiscal
    labels, form, filed date, accession, frame. Nothing is standardised here
    — concept priority, Q4 derivation, TTM and split-rescaled shares happen
    at read time (modeling/statements.py), so a better mapping later never
    needs a rewrite of history.

IDENTITY CHECK
    The CIK on an asset is a claim (core/cik_resolution.py). Before rows are
    stored, the filer's own `entityName` is returned to the caller, who
    records it; scripts decide what to do with a mismatch. Nothing here
    guesses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

from config.settings import FUNDAMENTAL_CONCEPTS


@dataclass(frozen=True)
class Fact:
    cik: int
    taxonomy: str
    concept: str
    unit: str
    period_start: Optional[date]
    period_end: date
    value: float
    fy: Optional[int]
    fp: Optional[str]
    form: str
    filed: date
    accn: str
    frame: Optional[str]
    fetched_at: datetime

    def key(self) -> Tuple:
        return (self.cik, self.taxonomy, self.concept, self.unit, self.period_start, self.period_end, self.accn)


def parse_company_facts(
    doc: Dict[str, Any],
    fetched_at: datetime,
    allow: Sequence[Tuple[str, str]] = FUNDAMENTAL_CONCEPTS,
) -> List[Fact]:
    """The allow-listed facts in one companyfacts document, as served."""
    cik = int(doc["cik"])
    allowed = set(allow)
    out: List[Fact] = []
    for taxonomy, concepts in (doc.get("facts") or {}).items():
        for concept, body in concepts.items():
            if (taxonomy, concept) not in allowed:
                continue
            for unit, rows in (body.get("units") or {}).items():
                for r in rows:
                    if r.get("val") is None:
                        continue
                    out.append(
                        Fact(
                            cik=cik,
                            taxonomy=taxonomy,
                            concept=concept,
                            unit=unit,
                            period_start=date.fromisoformat(r["start"]) if r.get("start") else None,
                            period_end=date.fromisoformat(r["end"]),
                            value=float(r["val"]),
                            fy=r.get("fy"),
                            fp=r.get("fp"),
                            form=r["form"],
                            filed=date.fromisoformat(r["filed"]),
                            accn=r["accn"],
                            frame=r.get("frame"),
                            fetched_at=fetched_at,
                        )
                    )
    return out


class FactRepository(Protocol):
    async def insert(self, facts: List[Fact]) -> int:
        """Insert facts not already stored; return how many were new."""
        ...

    async def facts(self, cik: int, concepts: Iterable[str]) -> List[Fact]:
        ...


@dataclass
class FactIngestReport:
    cik: int
    entity_name: str
    served: int
    inserted: int
    concepts: int


async def ingest_company(
    repo: FactRepository,
    doc: Dict[str, Any],
    fetched_at: Optional[datetime] = None,
) -> FactIngestReport:
    """Store one companyfacts document's allow-listed facts. Insert-only."""
    facts = parse_company_facts(doc, fetched_at or datetime.now(timezone.utc))
    inserted = await repo.insert(facts)
    return FactIngestReport(
        cik=int(doc["cik"]),
        entity_name=str(doc.get("entityName", "")),
        served=len(facts),
        inserted=inserted,
        concepts=len({f.concept for f in facts}),
    )
