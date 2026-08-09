"""
api/routers/ingest.py
Fetching new price bars into TimescaleDB, and managing the universe.

THE STREAMLIT BUTTON WAS WRITING TO THE WRONG DATABASE. It shelled out to
`cli/run_pipeline.py`, which at the time drove PipelineOrchestrator over a
`sqlite3.Connection`; nothing in that path touched TimescaleDB. Every ingest
run since the cutover filled a database the API does not read, while
reporting success — the newest bar in TimescaleDB was 2025-07-15.

Nothing here shells out. `core/ingest.py` runs in-process, which also avoids
inheriting the hardcoded conda interpreter path the button used
(/opt/anaconda3/envs/quant-pipeline-env/bin/python) — an environment
deprecated on 2026-07-31 when Poetry became authoritative.

`cli/run_pipeline.py` was rewritten on 2026-08-09 to call core/ingest.py too,
so the CLI and this endpoint cannot diverge; the SQLite orchestrator and its
DatabaseManager were deleted.

Ingestion is single-flight: a second start while one is running is a 409.
Progress streams over WS /api/v1/ws/ingest.

Phase 5 — decommissioning Streamlit
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.ingest import IngestReport, ingest_symbols, job
from db.models import AssetORM
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_db, get_fetcher, get_market_data_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])

MAX_SYMBOLS = 1_000
UNIVERSE_SOURCES = ("sp500", "dow_jones", "nasdaq100", "top_100_crypto")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class IngestRequest(BaseModel):
    symbols: Optional[List[str]] = Field(
        default=None,
        description="Tickers to refresh. Omit for every asset in the registry.",
    )
    start: Optional[datetime] = Field(
        default=None,
        description=(
            "Window start. Omitted means resume from the day after each "
            "symbol's newest stored bar."
        ),
    )
    end: Optional[datetime] = None
    full_backfill: bool = Field(
        default=False, description="Ignore stored history and refetch from 2015."
    )


class SymbolResult(BaseModel):
    symbol: str
    fetched: int
    written: int
    skipped_empty: int = Field(
        description="Bars with no prices at all — dropped, per the Phase 2 cutover"
    )
    error: Optional[str] = None
    first_bar: Optional[datetime] = None
    last_bar: Optional[datetime] = None


class IngestResponse(BaseModel):
    symbols: List[str]
    written: int
    failed: List[str]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    results: List[SymbolResult]


class IngestStatus(BaseModel):
    running: bool
    started_at: Optional[datetime] = None
    completed: int = 0
    total: int = 0
    current_symbol: Optional[str] = None


class AddAssetRequest(BaseModel):
    symbol: str = Field(min_length=1)
    asset_class: str = Field(default="equity", description="equity | crypto")
    source: str = Field(default="yfinance")


class AssetOut(BaseModel):
    symbol: str
    asset_class: str
    source: str
    created: bool = Field(description="False when it already existed")


class DriftedSymbol(BaseModel):
    symbol: str
    last_full_refresh_at: Optional[datetime] = None
    splits: List[Dict[str, Any]] = Field(
        description="Splits newer than the last full refresh"
    )
    detail: str


class DataHealthResponse(BaseModel):
    checked: int
    drifted: List[DriftedSymbol] = Field(
        description=(
            "Stored bars are adjusted to a stale as-of date. Fix with a full "
            "backfill of these symbols."
        )
    )
    delisted: List[str] = Field(
        description="Marked as no longer trading by a previous ingest"
    )
    unrefreshed: List[str] = Field(
        description=(
            "Never restated by a full backfill, so their adjustment date is "
            "unknown. Not necessarily wrong."
        )
    )


class UniverseResponse(BaseModel):
    source: str
    symbols: List[str]
    count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_response(report: IngestReport) -> IngestResponse:
    return IngestResponse(
        symbols=report.symbols,
        written=report.written,
        failed=report.failed,
        started_at=report.started_at,
        finished_at=report.finished_at,
        results=[
            SymbolResult(
                symbol=o.symbol,
                fetched=o.fetched,
                written=o.written,
                skipped_empty=o.skipped_empty,
                error=o.error,
                first_bar=o.first_bar,
                last_bar=o.last_bar,
            )
            for o in report.outcomes
        ],
    )


async def all_registered_symbols(session: AsyncSession) -> List[str]:
    result = await session.execute(select(AssetORM.symbol).order_by(AssetORM.symbol))
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/status", response_model=IngestStatus, summary="Current ingest state")
async def ingest_status() -> IngestStatus:
    return IngestStatus(**job.status())


@router.post(
    "",
    response_model=IngestResponse,
    summary="Fetch new bars into TimescaleDB",
    responses={
        409: {"description": "An ingest is already running"},
        422: {"description": "No symbols, or too many"},
    },
)
async def run_ingest(
    request: IngestRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
    session: AsyncSession = Depends(get_db),
    fetcher=Depends(get_fetcher),
) -> IngestResponse:
    """
    Runs to completion before responding. For a large universe use the
    websocket instead — this can take minutes.
    """
    symbols = request.symbols or await all_registered_symbols(session)
    symbols = [s.upper().strip() for s in symbols if s and s.strip()]
    if not symbols:
        raise HTTPException(
            status_code=422, detail="No symbols to ingest — the registry is empty."
        )
    if len(symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"{len(symbols)} symbols requested; the limit is {MAX_SYMBOLS}.",
        )

    if not job.try_start(len(symbols)):
        raise HTTPException(
            status_code=409,
            detail=(
                "An ingest is already running. Two runs would fetch the same "
                "window twice for no benefit — poll GET /api/v1/ingest/status."
            ),
        )

    report: Optional[IngestReport] = None
    try:
        report = await ingest_symbols(
            repo=repo,
            symbols=symbols,
            start=request.start,
            end=request.end,
            full_backfill=request.full_backfill,
            fetcher=fetcher,
            progress=job.note,
            run_in_thread=run_in_threadpool,
        )
    finally:
        # In a finally block so a failure mid-run cannot leave the job stuck
        # "running", which would 409 every later request until a restart.
        job.finish(report)

    return to_response(report)


@router.post(
    "/assets",
    response_model=AssetOut,
    status_code=201,
    summary="Add a ticker to the universe",
)
async def add_asset(
    request: AddAssetRequest,
    session: AsyncSession = Depends(get_db),
) -> AssetOut:
    """
    Registers a symbol so ingestion will fetch it.

    Idempotent: adding an existing ticker returns it with created=false rather
    than 409. The Streamlit form was a fire-and-forget "Add Ticker & Run
    Pipeline" button, and making a repeat press an error would be unhelpful.
    """
    symbol = request.symbol.upper().strip()
    if request.asset_class not in ("equity", "crypto"):
        raise HTTPException(
            status_code=422,
            detail=f"asset_class must be 'equity' or 'crypto'; got {request.asset_class!r}.",
        )

    existing = await session.execute(
        select(AssetORM).where(
            AssetORM.symbol == symbol,
            AssetORM.asset_class == request.asset_class,
            AssetORM.source == request.source,
        )
    )
    found = existing.unique().scalars().first()
    if found is not None:
        return AssetOut(
            symbol=found.symbol,
            asset_class=found.asset_class,
            source=found.source,
            created=False,
        )

    row = AssetORM(
        symbol=symbol, asset_class=request.asset_class, source=request.source
    )
    session.add(row)
    await session.commit()
    return AssetOut(
        symbol=symbol,
        asset_class=request.asset_class,
        source=request.source,
        created=True,
    )


@router.get(
    "/health",
    response_model=DataHealthResponse,
    summary="Which stored series have drifted against corporate actions",
    responses={422: {"description": "Too many symbols"}},
)
async def data_health(
    symbols: Optional[List[str]] = Query(
        default=None,
        description="Tickers to check. Omit for every registered asset.",
    ),
    session: AsyncSession = Depends(get_db),
) -> DataHealthResponse:
    """
    Reports drift; it does not fix it.

    The fix is `POST /api/v1/ingest {"symbols": [...], "full_backfill": true}`,
    which restates the series — a write, and therefore the caller's decision.

    One network call per symbol (split history), so checking the whole registry
    takes minutes. Pass `symbols` to check a few.
    """
    from core.corporate_actions import detect_drift, yfinance_splits

    result = await session.execute(
        select(
            AssetORM.symbol, AssetORM.last_full_refresh_at, AssetORM.delisted_at
        ).order_by(AssetORM.symbol)
    )
    rows = list(result.all())
    if symbols:
        wanted = {s.upper() for s in symbols}
        rows = [r for r in rows if r[0] in wanted]

    if len(rows) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"{len(rows)} symbols to check; the limit is {MAX_SYMBOLS}.",
        )

    drifted: List[DriftedSymbol] = []
    delisted: List[str] = []
    unrefreshed: List[str] = []

    for symbol, last_refresh, delisted_at in rows:
        if delisted_at is not None:
            delisted.append(symbol)
            continue
        if last_refresh is None:
            unrefreshed.append(symbol)

        report = await run_in_threadpool(
            detect_drift, symbol, last_refresh, yfinance_splits
        )
        if report.needs_refresh and last_refresh is not None:
            drifted.append(
                DriftedSymbol(
                    symbol=symbol,
                    last_full_refresh_at=last_refresh,
                    splits=[
                        {"date": s.date.date().isoformat(), "ratio": s.ratio}
                        for s in report.splits
                    ],
                    detail=report.describe(),
                )
            )

    return DataHealthResponse(
        checked=len(rows),
        drifted=drifted,
        delisted=delisted,
        unrefreshed=unrefreshed,
    )


@router.get(
    "/universe",
    response_model=UniverseResponse,
    summary="Constituents of a known index",
    responses={
        422: {"description": "Unknown source"},
        503: {"description": "Could not reach the constituent list"},
    },
)
async def get_universe(
    source: str = Query(description=f"One of: {', '.join(UNIVERSE_SOURCES)}"),
) -> UniverseResponse:
    """
    Lists an index's members WITHOUT registering them — the caller decides
    what to add. DynamicUniverse scrapes public pages, so this is the second
    network-touching path in the API; it is read-only and explicit.
    """
    if source not in UNIVERSE_SOURCES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown source {source!r}. Choose one of: {', '.join(UNIVERSE_SOURCES)}.",
        )

    from data_pipeline.dynamic_universe import DynamicUniverse

    try:
        symbols = await run_in_threadpool(DynamicUniverse().get_tickers, source)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=503, detail=f"Could not fetch the {source} universe: {exc}"
        ) from None

    if not symbols:
        raise HTTPException(
            status_code=503,
            detail=f"The {source} constituent list came back empty.",
        )

    return UniverseResponse(source=source, symbols=symbols, count=len(symbols))
