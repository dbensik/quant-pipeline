"""
core/fundamentals.py

THE write path for SEC fundamentals: company facts -> allow-listed raw rows
-> `fundamental_facts`, insert-only. Tier 3 phase 1 of
research/dcf-plan-2026-10-08.md. Mirrors core/ingest.py for prices: the
fetch is injected (a gateway, or a saved file), nothing is ever rewritten.

WHAT IS STORED
    Every row of every concept in settings.STORED_CONCEPTS (the DCF's
    FUNDAMENTAL_CONCEPTS and the desk's RESEARCH_CONCEPTS), as the SEC
    served it: value, unit, period (start is NULL for an instant), fiscal
    labels, form, filed date, accession, frame, and where it came from
    (`source`). Nothing is standardised here
    — concept priority, Q4 derivation, TTM and split-rescaled shares happen
    at read time (modeling/statements.py), so a better mapping later never
    needs a rewrite of history.

TWO SOURCES (decision 59, research/sec-companyfacts-gap-2026-10-23.md)
    companyfacts, SEC's per-company document, is the source. It leaves out
    whole filings (63 10-Qs of Jul-Aug 2026), so a filing it lacks is read
    from the filing's own XBRL instance (core/xbrl_instance.py) into the same
    shape, stored with source 'instance'. The key has no value in it, so when
    companyfacts later serves that filing its rows are not inserted: each one
    whose value differs from the instance's is returned as a disagreement for
    the caller to flag, never written over.

IDENTITY CHECK
    The CIK on an asset is a claim (core/cik_resolution.py). Before rows are
    stored, the filer's own `entityName` is returned to the caller, who
    records it; scripts decide what to do with a mismatch. Nothing here
    guesses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Set, Tuple

from config.settings import STORED_CONCEPTS


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
    source: str = "companyfacts"

    def key(self) -> Tuple:
        return (
            self.cik,
            self.taxonomy,
            self.concept,
            self.unit,
            self.period_start,
            self.period_end,
            self.accn,
        )


def parse_company_facts(
    doc: Dict[str, Any],
    fetched_at: datetime,
    allow: Sequence[Tuple[str, str]] = STORED_CONCEPTS,
    source: str = "companyfacts",
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
                            source=source,
                        )
                    )
    return out


class FactRepository(Protocol):
    async def insert(self, facts: List[Fact]) -> int:
        """Insert facts not already stored; return how many were new."""
        ...

    async def facts(self, cik: int, concepts: Iterable[str]) -> List[Fact]: ...

    async def instance_values(self, facts: List[Fact]) -> Dict[Tuple, float]:
        """Stored values, by key, of the rows among `facts`' keys that came from an instance."""
        ...

    async def stored_accessions(self, accns: Iterable[str]) -> Set[str]:
        """Those of `accns` with a stored row under ANY CIK: a filing a successor
        and its predecessor both list (XOM's 2026-08-03 10-Q) is stored once."""
        ...


@dataclass
class FactIngestReport:
    cik: int
    entity_name: str
    served: int
    inserted: int
    concepts: int
    #: companyfacts rows whose value differs from the instance row stored
    #: under the same key: (the served fact, the stored value).
    disagreements: List[Tuple[Fact, float]] = field(default_factory=list)


async def ingest_company(
    repo: FactRepository,
    doc: Dict[str, Any],
    fetched_at: Optional[datetime] = None,
) -> FactIngestReport:
    """Store one companyfacts document's allow-listed facts. Insert-only."""
    facts = parse_company_facts(doc, fetched_at or datetime.now(timezone.utc))
    stored = await repo.instance_values(facts)
    # Exact: both are the filing's own decimal text, and matched exactly on 78 filings.
    disagreements = [(f, stored[f.key()]) for f in facts if stored.get(f.key(), f.value) != f.value]
    inserted = await repo.insert(facts)
    return FactIngestReport(
        cik=int(doc["cik"]),
        entity_name=str(doc.get("entityName", "")),
        served=len(facts),
        inserted=inserted,
        concepts=len({f.concept for f in facts}),
        disagreements=disagreements,
    )


async def ingest_instance(
    repo: FactRepository,
    xml: bytes,
    cik: int,
    accn: str,
    form: str,
    filed: date,
    fetched_at: Optional[datetime] = None,
) -> FactIngestReport:
    """Store one filing's allow-listed facts from its XBRL instance. Insert-only."""
    from core.xbrl_instance import instance_company_facts

    doc = instance_company_facts(xml, cik, accn, form, filed.isoformat())
    facts = parse_company_facts(doc, fetched_at or datetime.now(timezone.utc), source="instance")
    return FactIngestReport(
        cik=int(cik),
        entity_name=str(doc.get("entityName", "")),
        served=len(facts),
        inserted=await repo.insert(facts),
        concepts=len({f.concept for f in facts}),
    )
