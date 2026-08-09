"""
api/routers/watchlists.py
Named lists of tickers.

Ported from dashboard_app/watchlist_manager.py, which stored them in
`watchlists.json`. Moved to the database for the same reason portfolios were
in 0003: mutable state with more than one writer, held in memory and rewritten
whole on every save.

Saving a watchlist REPLACES its symbols rather than merging them. That is what
the Streamlit form did — it submitted the complete multiselect — and a merge
would make removing a ticker impossible through the same control that adds one.

Phase 5 — decommissioning Streamlit
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from db.repositories.watchlists import TimescaleWatchlistRepo

from api.dependencies import get_watchlist_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/watchlists", tags=["watchlists"])

MAX_SYMBOLS = 500


class WatchlistOut(BaseModel):
    name: str
    symbols: List[str]
    created_at: Optional[datetime] = None


class SaveWatchlistRequest(BaseModel):
    symbols: List[str] = Field(
        description=(
            "The complete list. Symbols are upper-cased and de-duplicated, "
            "and their order is preserved."
        )
    )


@router.get("", response_model=List[WatchlistOut], summary="List watchlists")
async def list_watchlists(
    symbol: Optional[str] = Query(
        default=None,
        description="Return only watchlists containing this ticker.",
    ),
    repo: TimescaleWatchlistRepo = Depends(get_watchlist_repo),
) -> List[WatchlistOut]:
    watchlists = await repo.list_watchlists()
    if symbol:
        names = set(await repo.watchlists_containing(symbol))
        watchlists = [w for w in watchlists if w.name in names]
    return [
        WatchlistOut(name=w.name, symbols=w.symbols, created_at=w.created_at)
        for w in watchlists
    ]


@router.get(
    "/{name}",
    response_model=WatchlistOut,
    summary="One watchlist",
    responses={404: {"description": "No such watchlist"}},
)
async def get_watchlist(
    name: str,
    repo: TimescaleWatchlistRepo = Depends(get_watchlist_repo),
) -> WatchlistOut:
    found = await repo.get_watchlist(name)
    if found is None:
        raise HTTPException(status_code=404, detail=f"No watchlist named {name!r}.")
    return WatchlistOut(
        name=found.name, symbols=found.symbols, created_at=found.created_at
    )


@router.put(
    "/{name}",
    response_model=WatchlistOut,
    summary="Create or replace a watchlist",
    responses={422: {"description": "Too many symbols"}},
)
async def save_watchlist(
    name: str,
    request: SaveWatchlistRequest,
    repo: TimescaleWatchlistRepo = Depends(get_watchlist_repo),
) -> WatchlistOut:
    """
    PUT, not POST: this is idempotent and replaces the whole list, so the same
    call creates a new watchlist or overwrites an existing one.
    """
    if not name.strip():
        raise HTTPException(status_code=422, detail="A watchlist needs a name.")
    if len(request.symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{len(request.symbols)} symbols given; the limit is {MAX_SYMBOLS}."
            ),
        )

    saved = await repo.save_watchlist(name, request.symbols)
    return WatchlistOut(
        name=saved.name, symbols=saved.symbols, created_at=saved.created_at
    )


@router.delete(
    "/{name}",
    status_code=204,
    summary="Delete a watchlist",
    responses={404: {"description": "No such watchlist"}},
)
async def delete_watchlist(
    name: str,
    repo: TimescaleWatchlistRepo = Depends(get_watchlist_repo),
) -> None:
    if not await repo.delete_watchlist(name):
        raise HTTPException(status_code=404, detail=f"No watchlist named {name!r}.")
