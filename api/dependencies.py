"""
api/dependencies.py
FastAPI dependency-injection wiring.

Phase 3 — FastAPI routers for the React UI
"""

from typing import AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.repositories.market_data import TimescaleMarketDataRepo
from db.repositories.portfolios import TimescalePortfolioRepo
from db.repositories.watchlists import TimescaleWatchlistRepo
from db.session import get_session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yield a request-scoped AsyncSession.

    NOTE: db.session.get_session is an @asynccontextmanager, which Depends()
    cannot consume directly — FastAPI needs a plain async generator. This thin
    wrapper is that adapter; don't "simplify" it by passing get_session to
    Depends().
    """
    async with get_session() as session:
        yield session


async def get_market_data_repo(
    session: AsyncSession = Depends(get_db),
) -> TimescaleMarketDataRepo:
    """
    Yield a repository bound to the request's session.

    Routers depend on this rather than constructing a repo themselves, so the
    storage backend can be swapped (the repository is a structural Protocol)
    without touching route handlers.
    """
    return TimescaleMarketDataRepo(session)


async def get_portfolio_repo(
    session: AsyncSession = Depends(get_db),
) -> TimescalePortfolioRepo:
    """Portfolio repository bound to the request's session."""
    return TimescalePortfolioRepo(session)


async def get_watchlist_repo(
    session: AsyncSession = Depends(get_db),
) -> TimescaleWatchlistRepo:
    """Watchlist repository bound to the request's session."""
    return TimescaleWatchlistRepo(session)


def get_fetcher():
    """
    The price-bar fetcher used by ingestion.

    A dependency purely so tests can substitute one — `pytest tests/` must
    need no network, and ingestion is otherwise the one write path that would
    reach Yahoo.
    """
    from core.ingest import default_fetcher

    return default_fetcher
