"""
api/routers/screeners.py
Filter a universe of symbols down to those passing one or more screeners.

Phase 5 — decommissioning Streamlit
"""

import csv
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from api.dependencies import get_market_data_repo, get_universe_repo
from api.frames import records_to_frame
from config import settings
from core.crypto_class import META_CRYPTO_CLASS, PEGGED
from core.membership_reconstruction import renames_from_rows, todays_symbol
from db.repositories.market_data import TimescaleMarketDataRepo
from db.repositories.universe import TimescaleUniverseRepo
from screeners import registry
from screeners.base_screener import BaseScreener

router = APIRouter(prefix="/api/v1/screeners", tags=["screeners"])

# Screening loads full OHLCV for every candidate. Unbounded, a request for the
# whole 616-symbol universe over five years would pull ~800k rows into one
# response cycle; this keeps a single request bounded and makes the limit
# explicit to the caller rather than discovering it as a timeout.
MAX_SYMBOLS = 200
#: An index resolves its own universe, so the cap is the index, not the
#: caller's list: the S&P 500 is ~503 securities, and with the 200 cap above
#: an S&P 500 screen was refused every time (found 2026-10-19, decision 42).
MAX_INDEX_SYMBOLS = 600
#: Index whose membership before the first daily snapshot can be read from
#: the reconstructed month-end lists (universe_membership_reconstructed).
RECONSTRUCTED_INDEXES = {"sp500"}


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
    symbols: List[str] = Field(
        default_factory=list,
        description=(
            "Universe to filter. Omit when using `index`, which resolves the "
            "universe as it stood at `start`."
        ),
    )
    index: Optional[str] = Field(
        default=None,
        description=(
            "Resolve the universe from this index's membership AS OF `start`, "
            "rather than from whatever is registered today. Requires a "
            "snapshot at or before `start` — see POST /api/v1/universe/"
            "{index}/snapshot — or, for sp500 before the first snapshot, the "
            "reconstructed month-end list on or before `start`. Without this, "
            "screening a past window uses today's membership, which excludes "
            "everything since dropped and flatters the result."
        ),
    )
    start: datetime
    end: datetime
    screeners: List[ScreenerStep] = Field(
        description=(
            "Applied in order, each narrowing the output of the last — the "
            "same composition ScreenerPipeline performs."
        )
    )
    include_pegged: bool = Field(
        default=False,
        description=(
            "With a crypto `index`: keep stablecoins and tokenized assets, which are "
            "left out by default (decision 46) and listed in `excluded_pegged`."
        ),
    )


class ScreenerStepResult(BaseModel):
    screener_id: str
    display_name: str
    params: Dict[str, Any]
    passed: int = Field(description="Symbols surviving this step")


class ScreenResponse(BaseModel):
    universe: List[str] = Field(
        default_factory=list,
        description=(
            "The symbols actually screened. Worth returning when `index` "
            "resolved them: a caller cannot otherwise tell WHICH universe was "
            "used, which is the thing point-in-time membership exists to make "
            "explicit."
        ),
    )
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
    membership_source: Optional[Literal["observed", "reconstructed"]] = Field(
        default=None,
        description=(
            "With `index`: `observed` (the daily snapshots) or `reconstructed` "
            "(a month-end list read from a past revision of the source page, not "
            "an observation). Never a blend of the two."
        ),
    )
    membership_date: Optional[datetime] = Field(
        default=None,
        description="With `index`: the date of the member list used, on or before `start`",
    )
    membership_note: Optional[str] = None
    renamed: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Listed tickers mapped to the registry's: a reconstructed ticker of its "
            "day to today's (FB -> META), and a share class to Yahoo's form "
            "(BRK.B -> BRK-B). `universe` carries the mapped ones"
        ),
    )
    excluded_pegged: List[str] = Field(
        default_factory=list,
        description=(
            "Index members left out because they are pegged: stablecoins and "
            "tokenized off-chain assets, by CoinGecko's categories (decision 46). "
            "A low-volatility screen would otherwise rank every dollar first."
        ),
    )
    without_data: List[str] = Field(
        default_factory=list,
        description=(
            "Members with no bars in the window, by name. For a past list these are "
            "mostly names that later left the index, which is the survivorship gap: "
            "read `with_data` against `requested` before trusting a screen."
        ),
    )


def _to_schema(spec: registry.ScreenerSpec) -> ScreenerSchema:
    return ScreenerSchema(
        id=spec.id,
        display_name=spec.display_name,
        description=spec.description,
        params=[
            ParamSchema(
                name=p.name,
                type=p.type,
                default=p.default,
                label=p.label,
                description=p.description,
                minimum=p.minimum,
                maximum=p.maximum,
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


def _registry_form(symbol: str) -> str:
    """
    An index list's share-class ticker in the registry's (Yahoo's) form.
    The source page writes BRK.B and BF.B; the bars are stored as BRK-B and
    BF-B, so read as listed both counted as members without prices.
    """
    return symbol.replace(".", "-")


def _is_crypto_index(index: str) -> bool:
    return settings.NEW_MEMBER_INDEXES.get(index, (None, None))[0] == "crypto"


async def _is_pegged(repo: TimescaleMarketDataRepo, symbol: str) -> bool:
    """
    A stablecoin or tokenized asset: by the class recorded on its asset row
    (scripts/classify_crypto.py), or, for a member deliberately left
    unregistered, by its reason in CRYPTO_MEMBERS_NOT_REGISTERED.
    """
    if settings.CRYPTO_MEMBERS_NOT_REGISTERED.get(symbol) in PEGGED:
        return True
    asset = await repo.find_asset(symbol, "crypto")
    return asset is not None and (asset.metadata or {}).get(META_CRYPTO_CLASS) in PEGGED


@lru_cache(maxsize=1)
def _renames() -> Dict[str, Any]:
    with open(settings.SP500_TICKER_RENAMES_CSV, newline="") as fh:
        return renames_from_rows(csv.DictReader(fh))


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
    return ScreenerListResponse(count=len(specs), screeners=[_to_schema(s) for s in specs])


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
    universe: TimescaleUniverseRepo = Depends(get_universe_repo),
) -> ScreenResponse:
    """
    Fetch history for each symbol, then apply the screeners in order.

    Symbols with no bars in the range are excluded before screening and counted
    in `with_data`, so an empty result distinguishes "nothing passed the filter"
    from "nothing had data".
    """
    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    # Resolve the universe as it stood, when asked to.
    provenance: Dict[str, Any] = {}
    if request.index:
        if request.symbols:
            raise HTTPException(
                status_code=422,
                detail="Pass `symbols` or `index`, not both.",
            )
        membership = await universe.members_as_of(request.index, request.start)
        if membership.observed:
            provenance = {
                "membership_source": "observed",
                "membership_date": request.start,
                "membership_note": (
                    f"Observed by the daily snapshot (first taken "
                    f"{membership.first_observed.date().isoformat()})."
                ),
            }
            # An observed ticker can be renamed later too (PSKY -> SKYD,
            # 2026-10-06): its bars are stored under today's key.
            renames = _renames()
            mapped = {
                s: _registry_form(todays_symbol(s, renames, request.start.date()))
                for s in membership.symbols
            }
            symbols = list(dict.fromkeys(mapped.values()))
            provenance["renamed"] = {old: new for old, new in mapped.items() if old != new}
        else:
            reconstructed = (
                await universe.reconstructed_members_as_of(request.index, request.start)
                if request.index in RECONSTRUCTED_INDEXES
                else None
            )
            if reconstructed is None:
                # Falling back to today's membership here is exactly the
                # survivorship bias the index option exists to avoid, so this
                # is an error rather than a silent substitution.
                earliest = (
                    membership.first_observed.date().isoformat()
                    if membership.first_observed
                    else None
                )
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"No snapshot of {request.index!r} at or before "
                        f"{request.start.date().isoformat()}"
                        + (
                            f"; the earliest is {earliest}."
                            if earliest
                            else " — this index has never been snapshotted."
                        )
                        + " Membership before then is unknown; today's list is not"
                        " a substitute. Take a snapshot, or pass `symbols`."
                    ),
                )
            # Tickers of the day -> today's registry tickers, so a renamed
            # member is priced under the key its bars are stored under.
            renames = _renames()
            on = reconstructed.list_date.date()
            mapped = {
                s: _registry_form(todays_symbol(s, renames, on)) for s in reconstructed.symbols
            }
            symbols = list(dict.fromkeys(mapped.values()))
            provenance = {
                "membership_source": "reconstructed",
                "membership_date": reconstructed.list_date,
                "membership_note": (
                    f"Reconstructed, not observed: the month-end list of "
                    f"{reconstructed.list_date.date().isoformat()}, read from "
                    f"{reconstructed.source} {reconstructed.source_revision} "
                    f"({reconstructed.revision_at.date().isoformat()}). Members that "
                    "left the index since are mostly unpriced: see without_data."
                ),
                "renamed": {old: new for old, new in mapped.items() if old != new},
            }
        if _is_crypto_index(request.index) and not request.include_pegged:
            pegged = [s for s in symbols if await _is_pegged(repo, s)]
            symbols = [s for s in symbols if s not in pegged]
            provenance["excluded_pegged"] = pegged
        if len(symbols) > MAX_INDEX_SYMBOLS:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"{request.index!r} resolved to {len(symbols)} members; the limit "
                    f"is {MAX_INDEX_SYMBOLS}."
                ),
            )
        request = request.model_copy(update={"symbols": symbols})
    elif len(request.symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{len(request.symbols)} symbols requested; the limit is "
                f"{MAX_SYMBOLS}. Screening loads full OHLCV per symbol."
            ),
        )

    if not request.symbols:
        raise HTTPException(status_code=422, detail="`symbols` must not be empty.")

    if not request.screeners:
        raise HTTPException(status_code=422, detail="`screeners` must contain at least one step.")

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
        if request.start.tzinfo is None
        else request.start
    )
    end = request.end.replace(tzinfo=timezone.utc) if request.end.tzinfo is None else request.end

    data: Dict[str, Any] = {}
    for symbol in request.symbols:
        records = await repo.fetch_range(symbol=symbol, asset_class=None, start=start, end=end)
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
        passed, results = await run_in_threadpool(_screen_sync, steps, list(data), data)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    return ScreenResponse(
        universe=list(request.symbols),
        requested=len(request.symbols),
        with_data=len(data),
        passed=passed,
        steps=results,
        without_data=[symbol for symbol in request.symbols if symbol not in data],
        **provenance,
    )
