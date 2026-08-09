"""
api/routers/screeners.py
Filter a universe of symbols down to those passing one or more screeners.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from db.repositories.market_data import TimescaleMarketDataRepo
from screeners import registry
from screeners.base_screener import BaseScreener

from api.dependencies import get_market_data_repo
from api.frames import records_to_frame

router = APIRouter(prefix="/api/v1/screeners", tags=["screeners"])

# Screening loads full OHLCV for every candidate. Unbounded, a request for the
# whole 616-symbol universe over five years would pull ~800k rows into one
# response cycle; this keeps a single request bounded and makes the limit
# explicit to the caller rather than discovering it as a timeout.
MAX_SYMBOLS = 200


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ParamSchema(BaseModel):
    name: str
    type: Literal["int", "float", "str"]
    default: Any
    label: str
    description: str = ""
    minimum: Optional[float] = None
    maximum: Optional[float] = None


class ScreenerSchema(BaseModel):
    id: str
    display_name: str
    description: str
    params: List[ParamSchema]
    caveat: Optional[str] = None


class ScreenerListResponse(BaseModel):
    count: int
    screeners: List[ScreenerSchema]


class ScreenerStep(BaseModel):
    screener_id: str = Field(description="Registry id, e.g. 'momentum'")
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Omitted parameters use registry defaults.",
    )


class ScreenRequest(BaseModel):
    symbols: List[str] = Field(description="Universe to filter")
    start: datetime
    end: datetime
    screeners: List[ScreenerStep] = Field(
        description=(
            "Applied in order, each narrowing the output of the last — the "
            "same composition ScreenerPipeline performs."
        )
    )


class ScreenerStepResult(BaseModel):
    screener_id: str
    display_name: str
    params: Dict[str, Any]
    passed: int = Field(description="Symbols surviving this step")


class ScreenResponse(BaseModel):
    requested: int
    with_data: int = Field(
        description=(
            "Symbols that had bars in the range. A symbol with no data cannot "
            "be screened and is reported here rather than silently failing the "
            "filter — five registered crypto tickers hold zero bars."
        )
    )
    passed: List[str]
    steps: List[ScreenerStepResult] = Field(
        description="Per-step counts, so a screen returning nothing shows where it emptied."
    )


def _to_schema(spec: registry.ScreenerSpec) -> ScreenerSchema:
    return ScreenerSchema(
        id=spec.id,
        display_name=spec.display_name,
        description=spec.description,
        params=[
            ParamSchema(
                name=p.name, type=p.type, default=p.default, label=p.label,
                description=p.description, minimum=p.minimum, maximum=p.maximum,
            )
            for p in spec.params
        ],
        caveat=spec.caveat,
    )


def _screen_sync(
    steps: List[tuple[registry.ScreenerSpec, BaseScreener, Dict[str, Any]]],
    tickers: List[str],
    data: Dict[str, Any],
) -> tuple[List[str], List[ScreenerStepResult]]:
    """
    Apply each screener in turn — CPU-bound, so kept off the event loop.

    Composed rather than run independently: each step filters the survivors of
    the previous one, which is what ScreenerPipeline does and what the Streamlit
    sidebar's stacked checkboxes meant.
    """
    surviving = list(tickers)
    results: List[ScreenerStepResult] = []

    for spec, screener, params in steps:
        surviving = screener.screen(surviving, data)
        results.append(
            ScreenerStepResult(
                screener_id=spec.id,
                display_name=spec.display_name,
                params=params,
                passed=len(surviving),
            )
        )

    return surviving, results


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=ScreenerListResponse, summary="List available screeners")
async def list_screeners() -> ScreenerListResponse:
    """
    Every registered screener and its parameter schema.

    Same contract as /api/v1/strategies: a UI builds its whole screener panel,
    including per-screener controls with defaults and bounds, from this alone.
    """
    specs = registry.all_screeners()
    return ScreenerListResponse(
        count=len(specs), screeners=[_to_schema(s) for s in specs]
    )


@router.get(
    "/{screener_id}",
    response_model=ScreenerSchema,
    summary="One screener's schema",
    responses={404: {"description": "No such screener"}},
)
async def get_screener(
    screener_id: str = Path(description="Registry id, e.g. 'low_volatility'"),
) -> ScreenerSchema:
    try:
        return _to_schema(registry.get(screener_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


@router.post(
    "/run",
    response_model=ScreenResponse,
    summary="Filter a universe through one or more screeners",
    responses={
        404: {"description": "Unknown screener"},
        422: {"description": "Invalid parameters, bad range, or too many symbols"},
    },
)
async def run_screen(
    request: ScreenRequest,
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> ScreenResponse:
    """
    Fetch history for each symbol, then apply the screeners in order.

    Symbols with no bars in the range are excluded before screening and counted
    in `with_data`, so an empty result distinguishes "nothing passed the filter"
    from "nothing had data".
    """
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    if not request.symbols:
        raise HTTPException(status_code=422, detail="`symbols` must not be empty.")

    if len(request.symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{len(request.symbols)} symbols requested; the limit is "
                f"{MAX_SYMBOLS}. Screening loads full OHLCV per symbol."
            ),
        )

    if not request.screeners:
        raise HTTPException(
            status_code=422, detail="`screeners` must contain at least one step."
        )

    # Build every screener before fetching anything, so a bad parameter fails
    # immediately instead of after a long data load.
    steps = []
    for step in request.screeners:
        try:
            spec = registry.get(step.screener_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        try:
            screener = spec.build(step.params)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        effective = {p.name: step.params.get(p.name, p.default) for p in spec.params}
        steps.append((spec, screener, effective))

    start = (
        request.start.replace(tzinfo=timezone.utc)
        if request.start.tzinfo is None else request.start
    )
    end = (
        request.end.replace(tzinfo=timezone.utc)
        if request.end.tzinfo is None else request.end
    )

    data: Dict[str, Any] = {}
    for symbol in request.symbols:
        records = await repo.fetch_range(
            symbol=symbol, asset_class=None, start=start, end=end
        )
        frame = records_to_frame(records)
        if not frame.empty:
            data[symbol] = frame

    if not data:
        raise HTTPException(
            status_code=422,
            detail=(
                f"None of the {len(request.symbols)} symbols has bars between "
                f"{start.date()} and {end.date()}."
            ),
        )

    try:
        passed, results = await run_in_threadpool(
            _screen_sync, steps, list(data), data
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    return ScreenResponse(
        requested=len(request.symbols),
        with_data=len(data),
        passed=passed,
        steps=results,
    )
