"""
api/routers/universe.py
Point-in-time index membership.

Screeners resolve their universe from `assets` — whatever is registered today.
Screening a 2024 window against the 2026 S&P 500 is survivorship bias: the
names dropped from the index are disproportionately the ones that did badly,
so excluding them flatters every result, silently.

This router records what an index contained WHEN IT WAS OBSERVED, and refuses
to guess about anything earlier. `POST /snapshot` is the only way history
accumulates, and it cannot be backdated — which is the argument for taking the
first one now.

Phase 5 — point-in-time universe
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from db.repositories.universe import TimescaleUniverseRepo

from api.dependencies import get_universe_repo
from api.routers.ingest import UNIVERSE_SOURCES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/universe", tags=["universe"])


class SnapshotResult(BaseModel):
    index_name: str
    taken_at: datetime
    member_count: int
    added: List[str] = Field(description="Not seen in the previous snapshot")
    removed: List[str] = Field(
        description=(
            "Present previously and absent now. They are NOT deleted — they "
            "stay queryable for the window they were in, which is the point."
        )
    )


class MembershipResult(BaseModel):
    index_name: str
    as_of: datetime
    symbols: List[str]
    observed: bool = Field(
        description=(
            "False when `as_of` predates the first snapshot. The symbol list "
            "is then empty and must NOT be read as an empty index — "
            "substituting today's membership is the bias this exists to stop."
        )
    )
    first_observed: Optional[datetime] = None
    last_observed: Optional[datetime] = None
    detail: Optional[str] = None


class SnapshotSummary(BaseModel):
    taken_at: datetime
    member_count: int


@router.get("", response_model=List[str], summary="Indexes with recorded history")
async def list_indexes(
    repo: TimescaleUniverseRepo = Depends(get_universe_repo),
) -> List[str]:
    return await repo.indexes()


@router.post(
    "/{index_name}/snapshot",
    response_model=SnapshotResult,
    status_code=201,
    summary="Record what an index contains right now",
    responses={
        422: {"description": "Unknown index"},
        503: {"description": "Could not read the constituent list"},
    },
)
async def take_snapshot(
    index_name: str,
    repo: TimescaleUniverseRepo = Depends(get_universe_repo),
) -> SnapshotResult:
    """
    Fetches the index's current members and records them as of now.

    Deliberately NOT backdatable. A snapshot asserts "this system observed
    these members at this time"; accepting a date would let a caller
    manufacture history the data cannot support.
    """
    if index_name not in UNIVERSE_SOURCES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown index {index_name!r}. "
                f"Choose one of: {', '.join(UNIVERSE_SOURCES)}."
            ),
        )

    from data_pipeline.dynamic_universe import DynamicUniverse

    try:
        symbols = await run_in_threadpool(
            DynamicUniverse().get_tickers, index_name
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=503, detail=f"Could not read {index_name}: {exc}"
        ) from None

    if not symbols:
        # Empty means the scrape failed; DynamicUniverse returns [] for both.
        # Recording it would write a snapshot claiming the index emptied.
        raise HTTPException(
            status_code=503,
            detail=(
                f"Could not read the {index_name} constituent list — the "
                "upstream page returned nothing usable. Refusing to record a "
                "snapshot that would claim the index is empty."
            ),
        )

    snapshot = await repo.record_snapshot(index_name, symbols)
    return SnapshotResult(
        index_name=snapshot.index_name,
        taken_at=snapshot.taken_at,
        member_count=snapshot.member_count,
        added=snapshot.added,
        removed=snapshot.removed,
    )


@router.get(
    "/{index_name}/members",
    response_model=MembershipResult,
    summary="Members of an index at a point in time",
)
async def members(
    index_name: str,
    as_of: Optional[datetime] = Query(
        default=None, description="Defaults to now."
    ),
    repo: TimescaleUniverseRepo = Depends(get_universe_repo),
) -> MembershipResult:
    when = as_of or datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)

    result = await repo.members_as_of(index_name, when)

    detail = None
    if not result.observed:
        detail = (
            f"No snapshot of {index_name!r} exists at or before "
            f"{when.date().isoformat()}"
            + (
                f"; the earliest is {result.first_observed.date().isoformat()}."
                if result.first_observed
                else " — this index has never been snapshotted."
            )
            + " Membership before then is unknown, and today's list is NOT a"
            " substitute for it."
        )

    return MembershipResult(
        index_name=result.index_name,
        as_of=result.as_of,
        symbols=result.symbols,
        observed=result.observed,
        first_observed=result.first_observed,
        last_observed=result.last_observed,
        detail=detail,
    )


@router.get(
    "/{index_name}/snapshots",
    response_model=List[SnapshotSummary],
    summary="When an index was observed, and how many members it had",
)
async def snapshots(
    index_name: str,
    repo: TimescaleUniverseRepo = Depends(get_universe_repo),
) -> List[SnapshotSummary]:
    return [
        SnapshotSummary(taken_at=s.taken_at, member_count=s.member_count)
        for s in await repo.snapshots(index_name)
    ]
