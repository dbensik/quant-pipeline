"""
db/repositories/watchlists.py

WatchlistRepository Protocol + TimescaleDB implementation.

Same seam as market_data and portfolios: the router depends on the Protocol,
so tests/api/ can substitute an in-memory stub and the router suite keeps
running with no Docker.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import WatchlistORM, WatchlistSymbolORM


@dataclass
class Watchlist:
    name: str
    symbols: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class WatchlistRepository:
    """Structural Protocol — no explicit inheritance required."""

    async def list_watchlists(self) -> List[Watchlist]:
        ...

    async def get_watchlist(self, name: str) -> Optional[Watchlist]:
        ...

    async def save_watchlist(self, name: str, symbols: List[str]) -> Watchlist:
        """Create or replace a watchlist's symbols wholesale."""
        ...

    async def delete_watchlist(self, name: str) -> bool:
        ...

    async def watchlists_containing(self, symbol: str) -> List[str]:
        """Names of every watchlist holding `symbol`."""
        ...


# ---------------------------------------------------------------------------
# TimescaleDB implementation
# ---------------------------------------------------------------------------

def _to_watchlist(row: WatchlistORM) -> Watchlist:
    return Watchlist(
        name=row.name,
        symbols=[s.symbol for s in sorted(row.symbols, key=lambda s: s.position)],
        created_at=row.created_at,
    )


class TimescaleWatchlistRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_watchlists(self) -> List[Watchlist]:
        result = await self.session.execute(
            select(WatchlistORM).order_by(WatchlistORM.name)
        )
        # .unique() because `symbols` uses lazy="selectin"; without it a list
        # with N symbols would appear N times.
        return [_to_watchlist(row) for row in result.unique().scalars().all()]

    async def get_watchlist(self, name: str) -> Optional[Watchlist]:
        row = await self._find(name)
        return _to_watchlist(row) if row else None

    async def save_watchlist(self, name: str, symbols: List[str]) -> Watchlist:
        # De-duplicate while preserving order: the unique constraint would
        # reject a repeated ticker, but silently keeping the first occurrence
        # is what the UI means by a list of tickers.
        seen: set[str] = set()
        ordered: List[str] = []
        for symbol in symbols:
            upper = symbol.upper().strip()
            if upper and upper not in seen:
                seen.add(upper)
                ordered.append(upper)

        children = [
            WatchlistSymbolORM(symbol=symbol, position=position)
            for position, symbol in enumerate(ordered)
        ]

        row = await self._find(name)
        if row is None:
            # Children passed to the CONSTRUCTOR. Creating the row, flushing,
            # then touching row.symbols would lazy-load the (now persistent)
            # collection outside greenlet context — MissingGreenlet.
            row = WatchlistORM(name=name, symbols=children)
            self.session.add(row)
        else:
            # Replace wholesale — a PUT, not a merge. delete-orphan removes
            # the rows that drop out, and the flush must land BEFORE the
            # re-insert or keeping any ticker across a save violates the
            # (watchlist_id, symbol) unique constraint.
            #
            # _find eager-loads `symbols` via lazy="selectin", so mutating the
            # collection here needs no further IO.
            row.symbols.clear()
            await self.session.flush()
            row.symbols.extend(children)

        await self.session.commit()

        # AsyncSessionLocal sets expire_on_commit=False, so `row` keeps
        # whatever it held in memory and the identity map hands the same
        # instance back to _find. Expiring forces the re-read — without it a
        # save returned the pre-save collection, which is what
        # test_resaving_replaces_without_violating_the_unique_constraint
        # caught and a fake repository never could.
        self.session.expire(row)
        refreshed = await self._find(name)
        return _to_watchlist(refreshed)

    async def delete_watchlist(self, name: str) -> bool:
        row = await self._find(name)
        if row is None:
            return False
        await self.session.delete(row)
        await self.session.commit()
        return True

    async def watchlists_containing(self, symbol: str) -> List[str]:
        result = await self.session.execute(
            select(WatchlistORM.name)
            .join(WatchlistSymbolORM, WatchlistSymbolORM.watchlist_id == WatchlistORM.id)
            .where(WatchlistSymbolORM.symbol == symbol.upper())
            .order_by(WatchlistORM.name)
        )
        return list(result.scalars().all())

    async def _find(self, name: str) -> Optional[WatchlistORM]:
        result = await self.session.execute(
            select(WatchlistORM).where(WatchlistORM.name == name)
        )
        return result.unique().scalars().first()
