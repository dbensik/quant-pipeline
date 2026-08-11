"""
db/session.py
Async SQLAlchemy engine and session factory.

Used by:
  - FastAPI dependency injection (api/dependencies.py)
  - Repository integration tests (use get_session() directly)

Phase 2 — TimescaleDB Schema & Repository Layer
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Defaults match the local docker-compose timescaledb service, so importing
    # this module without a .env works out of the box for local development.
    # Override via .env (see .env.example) or real environment variables in prod.
    #
    # PORT 15432, not 5432: docker-compose publishes the container's 5432 on the
    # host's 15432 so this project does not fight every other Postgres on the
    # machine. Inside the compose network the host port is irrelevant and
    # services address `timescaledb:5432` instead.
    DATABASE_URL: str = (
        "postgresql+asyncpg://quant:quant@localhost:15432/quant_pipeline"
    )
    SYNC_DATABASE_URL: str = (
        "postgresql://quant:quant@localhost:15432/quant_pipeline"  # Alembic only (sync driver)
    )

    class Config:
        env_file = ".env"
        extra = "ignore"        # tolerate API_HOST, API_PORT, ENVIRONMENT etc. in the same .env


settings = Settings()

# echo=False in production; set echo=True locally for SQL query logging
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,         # reconnect silently after idle timeout
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,     # keep ORM objects usable after commit without re-querying
    autoflush=False,
    autocommit=False,
)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context-manager that yields a single AsyncSession and guarantees
    rollback on exception and close on exit.

    Usage (repositories / scripts):
        async with get_session() as session:
            repo = TimescaleMarketDataRepo(session)
            await repo.write(records)

    Usage (FastAPI — see api/dependencies.py for the Depends() wrapper):
        async with get_session() as session:
            yield session
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
