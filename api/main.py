"""
Main entry point for the Quant Pipeline API.

This module exposes the REST API endpoints for interacting with the
quant-pipeline backend, allowing access to data pipelines, alpha models,
portfolio optimization, and execution services.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from fastapi.responses import JSONResponse

from app.core.config import settings
from db.session import engine as db_engine

from api.routers import (
    assets,
    backtest,
    ohlcv,
    portfolio_backtest,
    screeners,
    signals,
    statistics,
    strategies,
    ws,
)


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown hooks
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifespan events.

    Startup: initialise DB connection pool and any warm caches.
    Shutdown: dispose pool and release resources cleanly.

    Uses db/session.py's engine rather than constructing its own — one engine
    per process against one database. The previous second engine was built from
    app.core.config.database_url, which defaulted to SQLite, so the API would
    have served an empty database while the migrated data sat in TimescaleDB.
    """
    app.state.db_engine = db_engine
    yield
    await db_engine.dispose()


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Quant Pipeline API",
    description="API for quantitative finance data pipelines and strategy execution.",
    version="0.1.0",
    lifespan=lifespan,
    # Scope OpenAPI docs to the versioned prefix
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    # Driven from config so this becomes a production domain without code change.
    # Never combine allow_origins=["*"] with allow_credentials=True — browsers
    # reject credentialed requests to a wildcard origin per the CORS spec.
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(assets.router)
app.include_router(ohlcv.router)
app.include_router(strategies.router)
app.include_router(screeners.router)
app.include_router(statistics.router)
app.include_router(backtest.router)
app.include_router(portfolio_backtest.router)
app.include_router(signals.router)
app.include_router(ws.router)


# ---------------------------------------------------------------------------
# Global exception handler — prevents stack trace leakage in response bodies
# ---------------------------------------------------------------------------


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all handler to suppress internal detail from API consumers."""
    # TODO: wire into structured logger (e.g. structlog) before production
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# ---------------------------------------------------------------------------
# System endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/health/live", tags=["system"])
async def liveness() -> dict:
    """
    Liveness probe.

    Confirms the process is running. Container orchestrators (Docker Compose,
    Kubernetes) use this to decide whether to restart the container.
    """
    return {"status": "online"}


@app.get("/api/v1/health/ready", tags=["system"])
async def readiness(request: Request) -> dict:
    """
    Readiness probe.

    Verifies downstream dependencies (TimescaleDB) are reachable before the
    container is marked healthy. Returns 503 if the DB is unavailable so the
    orchestrator withholds traffic until the pool is ready.
    """
    db_healthy = True
    try:
        async with request.app.state.db_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        db_healthy = False

    if not db_healthy:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "detail": "Database unreachable"},
        )

    return {"status": "ready"}


# ---------------------------------------------------------------------------
# Service routers
# TODO: implement each router module and uncomment the includes below.
#
# Suggested layout:
#   app/api/v1/routers/
#       pipelines.py
#       alpha_models.py
#       portfolio.py
#       execution.py
#
# from app.api.v1.routers import pipelines, alpha_models, portfolio, execution
#
# app.include_router(pipelines.router,    prefix="/api/v1/pipelines",             tags=["pipelines"])
# app.include_router(alpha_models.router, prefix="/api/v1/alpha-models",          tags=["alpha-models"])
# app.include_router(portfolio.router,    prefix="/api/v1/portfolio-optimization", tags=["portfolio"])
# app.include_router(execution.router,    prefix="/api/v1/execution",             tags=["execution"])
# ---------------------------------------------------------------------------
