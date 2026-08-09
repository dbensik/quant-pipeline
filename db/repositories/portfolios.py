"""
db/repositories/portfolios.py

PortfolioRepository Protocol (structural interface) + TimescaleDB implementation.

Same seam as db/repositories/market_data.py, and for the same reason: the API
router depends on the Protocol, so `tests/api/` can substitute an in-memory
stub and the whole router suite keeps running with no Docker and no database.

Only the trade log is stored. Cash, positions and P&L are derived by
core/portfolio.derive_state, so this layer never persists a computed value.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.portfolio import Portfolio, Trade
from db.models import PortfolioORM, PortfolioTradeORM


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class PortfolioRepository:
    """
    Structural Protocol. Any class implementing these async methods is a valid
    PortfolioRepository — no explicit inheritance required.
    """

    async def list_portfolios(self) -> List[Portfolio]:
        """Every portfolio, WITHOUT its trades (listing must not load logs)."""
        ...

    async def get_portfolio(self, name: str) -> Optional[Portfolio]:
        """One portfolio with its full trade log, or None if it does not exist."""
        ...

    async def create_portfolio(
        self,
        name: str,
        initial_cash: float,
        metadata: Optional[dict] = None,
    ) -> Portfolio:
        """Create an empty portfolio. Raises ValueError if the name is taken."""
        ...

    async def delete_portfolio(self, name: str) -> bool:
        """Delete a portfolio and its trades. False if it did not exist."""
        ...

    async def add_trade(self, name: str, trade: Trade) -> Optional[Trade]:
        """Append one trade. None if the portfolio does not exist."""
        ...

    async def delete_trade(self, name: str, trade_id: str) -> bool:
        """Remove one trade. False if the portfolio or trade does not exist."""
        ...


# ---------------------------------------------------------------------------
# TimescaleDB implementation
# ---------------------------------------------------------------------------

def _to_trade(row: PortfolioTradeORM) -> Trade:
    return Trade(
        id=str(row.id),
        ticker=row.ticker,
        action=row.action,
        quantity=row.quantity,
        price=row.price,
        ts=row.time,
        costs=row.costs or 0.0,
        broker=row.broker,
        notes=row.notes,
    )


def _to_portfolio(row: PortfolioORM, trades: Optional[List[Trade]] = None) -> Portfolio:
    return Portfolio(
        name=row.name,
        initial_cash=row.initial_cash,
        trades=trades if trades is not None else [],
        created_at=row.created_at,
        metadata=row.metadata_,
    )


class TimescalePortfolioRepo:
    """PortfolioRepository backed by the shared async engine."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_portfolios(self) -> List[Portfolio]:
        result = await self.session.execute(
            select(PortfolioORM).order_by(PortfolioORM.name)
        )
        # .unique() because PortfolioORM.trades uses lazy="selectin"; without
        # it a portfolio with N trades would appear N times.
        return [_to_portfolio(row) for row in result.unique().scalars().all()]

    async def get_portfolio(self, name: str) -> Optional[Portfolio]:
        row = await self._find(name)
        if row is None:
            return None

        trades = await self.session.execute(
            select(PortfolioTradeORM)
            .where(PortfolioTradeORM.portfolio_id == row.id)
            # Average cost is order-dependent. derive_state sorts defensively,
            # but ordering here means the sort is a no-op rather than a fix.
            .order_by(PortfolioTradeORM.time, PortfolioTradeORM.id)
        )
        return _to_portfolio(row, [_to_trade(t) for t in trades.scalars().all()])

    async def create_portfolio(
        self,
        name: str,
        initial_cash: float,
        metadata: Optional[dict] = None,
    ) -> Portfolio:
        if await self._find(name) is not None:
            raise ValueError(f"A portfolio named {name!r} already exists.")

        row = PortfolioORM(
            name=name, initial_cash=initial_cash, metadata_=metadata
        )
        self.session.add(row)
        await self.session.commit()
        await self.session.refresh(row)
        return _to_portfolio(row, [])

    async def delete_portfolio(self, name: str) -> bool:
        row = await self._find(name)
        if row is None:
            return False
        # Trades go with it via ON DELETE CASCADE.
        await self.session.delete(row)
        await self.session.commit()
        return True

    async def add_trade(self, name: str, trade: Trade) -> Optional[Trade]:
        row = await self._find(name)
        if row is None:
            return None

        orm = PortfolioTradeORM(
            portfolio_id=row.id,
            time=trade.ts,
            ticker=trade.ticker,
            action=trade.action,
            quantity=trade.quantity,
            price=trade.price,
            costs=trade.costs,
            broker=trade.broker,
            notes=trade.notes,
        )
        self.session.add(orm)
        await self.session.commit()
        await self.session.refresh(orm)
        return _to_trade(orm)

    async def delete_trade(self, name: str, trade_id: str) -> bool:
        row = await self._find(name)
        if row is None:
            return False
        try:
            identifier = int(trade_id)
        except (TypeError, ValueError):
            return False

        result = await self.session.execute(
            delete(PortfolioTradeORM).where(
                PortfolioTradeORM.id == identifier,
                # Scoped to the portfolio, so a trade id from another
                # portfolio cannot be deleted through this one.
                PortfolioTradeORM.portfolio_id == row.id,
            )
        )
        await self.session.commit()
        return bool(result.rowcount)

    async def _find(self, name: str) -> Optional[PortfolioORM]:
        result = await self.session.execute(
            select(PortfolioORM).where(PortfolioORM.name == name)
        )
        return result.unique().scalars().first()
