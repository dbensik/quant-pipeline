"""
api/routers/statistics.py
Statistical tests and PCA over stored price history.

Phase 5 — decommissioning Streamlit
"""

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from analysis import registry
from db.repositories.market_data import TimescaleMarketDataRepo

from api.dependencies import get_market_data_repo
from api.frames import records_to_frame

router = APIRouter(prefix="/api/v1/statistics", tags=["statistics"])

# PCA and Johansen are O(n^2)+ in the number of series and the UI never asks
# for more than a handful. Bounded explicitly rather than discovered as a
# timeout.
MAX_SYMBOLS = 50


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


class TestSchema(BaseModel):
    id: str
    display_name: str
    description: str
    arity: Literal["single", "pair", "multi"] = Field(
        description="'single' takes one symbol, 'pair' exactly two, 'multi' two or more."
    )
    input_kind: Literal["price", "returns"] = Field(
        description=(
            "Whether the test consumes price levels or returns. The API "
            "performs the conversion — callers always send symbols."
        )
    )
    min_symbols: int
    params: List[ParamSchema]
    caveat: Optional[str] = None


class TestListResponse(BaseModel):
    count: int
    tests: List[TestSchema]


class StatisticsRequest(BaseModel):
    symbols: List[str]
    start: datetime
    end: datetime
    params: Dict[str, Any] = Field(default_factory=dict)


class StatisticsResponse(BaseModel):
    test_id: str
    display_name: str
    symbols: List[str] = Field(description="Symbols actually used, in order")
    input_kind: Literal["price", "returns"]
    observations: int = Field(description="Rows fed to the test after alignment")
    params: Dict[str, Any]
    result: Dict[str, Any]


def _to_schema(spec: registry.TestSpec) -> TestSchema:
    return TestSchema(
        id=spec.id,
        display_name=spec.display_name,
        description=spec.description,
        arity=spec.arity,
        input_kind=spec.input_kind,
        min_symbols=registry.MIN_SYMBOLS[spec.arity],
        params=[
            ParamSchema(
                name=p.name, type=p.type, default=p.default, label=p.label,
                description=p.description, minimum=p.minimum, maximum=p.maximum,
            )
            for p in spec.params
        ],
        caveat=spec.caveat,
    )


def _json_safe(value: Any) -> Any:
    """
    Make numpy/pandas results JSON-encodable.

    statsmodels returns numpy scalars and arrays, and NaN/Inf are not valid
    JSON — a p-value of NaN on a degenerate series would otherwise emit a body
    a strict client rejects.
    """
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, pd.DataFrame):
        return {
            "index": [str(i) for i in value.index],
            "columns": [str(c) for c in value.columns],
            "data": _json_safe(value.to_numpy().tolist()),
        }
    if isinstance(value, pd.Series):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if hasattr(value, "tolist"):  # numpy array
        return _json_safe(value.tolist())
    if hasattr(value, "item"):  # numpy scalar
        try:
            value = value.item()
        except (ValueError, AttributeError):
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _run_test_sync(
    spec: registry.TestSpec, frame: pd.DataFrame, params: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Dispatch to the right analyzer. CPU-bound, so kept off the event loop.

    `frame` is already prices or returns per spec.input_kind, and its columns
    are in the caller's symbol order — which matters for OLS, where the first
    symbol is the asset and the second the benchmark.
    """
    # Imported here rather than at module scope: statsmodels and pykalman are
    # heavy, and only this endpoint needs them.
    from analysis.principal_component_analyzer import PrincipalComponentAnalyzer
    from analysis.statistical_analyzer import StatisticalAnalyzer

    analyzer = StatisticalAnalyzer()
    columns = list(frame.columns)

    if spec.id == "adf":
        return analyzer.run_adf_test(frame[columns[0]])
    if spec.id == "kalman":
        return {"smoothed": analyzer.run_kalman_filter_smoother(frame[columns[0]])}
    if spec.id == "ols":
        return analyzer.run_ols_regression(frame[columns[0]], frame[columns[1]])
    if spec.id == "engle_granger":
        return analyzer.run_engle_granger_test(frame[columns[0]], frame[columns[1]])
    if spec.id == "johansen":
        return analyzer.run_johansen_test(
            frame, det_order=params["det_order"], k_ar_diff=params["k_ar_diff"]
        )
    if spec.id == "pca":
        return PrincipalComponentAnalyzer(
            frame, n_components=params["n_components"]
        ).run()

    raise ValueError(f"No dispatch for test '{spec.id}'")  # pragma: no cover


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=TestListResponse, summary="List available tests")
async def list_tests() -> TestListResponse:
    """
    Every registered test with its arity, input kind and parameter schema — a
    UI builds its whole panel from this, including how many symbols to ask for.
    """
    specs = registry.all_tests()
    return TestListResponse(count=len(specs), tests=[_to_schema(s) for s in specs])


@router.get(
    "/{test_id}",
    response_model=TestSchema,
    summary="One test's schema",
    responses={404: {"description": "No such test"}},
)
async def get_test(test_id: str = Path(description="Registry id, e.g. 'adf'")) -> TestSchema:
    try:
        return _to_schema(registry.get(test_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


@router.post(
    "/{test_id}",
    response_model=StatisticsResponse,
    summary="Run a statistical test over stored history",
    responses={
        404: {"description": "Unknown test or symbol"},
        422: {"description": "Wrong symbol count, invalid parameters, or no data"},
    },
)
async def run_test(
    request: StatisticsRequest,
    test_id: str = Path(description="Registry id, e.g. 'johansen'"),
    repo: TimescaleMarketDataRepo = Depends(get_market_data_repo),
) -> StatisticsResponse:
    """
    Fetch each symbol's closes, align them, convert to returns if the test
    needs returns, then run it.

    Symbol ORDER is preserved and significant: OLS treats the first symbol as
    the asset and the second as the benchmark, so sorting them would silently
    invert alpha and beta.
    """
    try:
        spec = registry.get(test_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None

    try:
        params = registry.resolve_params(spec, request.params)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    if request.start > request.end:
        raise HTTPException(status_code=422, detail="`start` must not be after `end`.")

    minimum = registry.MIN_SYMBOLS[spec.arity]
    if len(request.symbols) < minimum:
        raise HTTPException(
            status_code=422,
            detail=(
                f"'{spec.id}' is a {spec.arity} test and needs at least "
                f"{minimum} symbol(s); {len(request.symbols)} given."
            ),
        )
    if spec.arity == "pair" and len(request.symbols) != 2:
        raise HTTPException(
            status_code=422,
            detail=f"'{spec.id}' takes exactly 2 symbols; {len(request.symbols)} given.",
        )
    if spec.arity == "single" and len(request.symbols) != 1:
        raise HTTPException(
            status_code=422,
            detail=f"'{spec.id}' takes exactly 1 symbol; {len(request.symbols)} given.",
        )
    if len(request.symbols) > MAX_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"{len(request.symbols)} symbols requested; the limit is {MAX_SYMBOLS}.",
        )

    start = (
        request.start.replace(tzinfo=timezone.utc)
        if request.start.tzinfo is None else request.start
    )
    end = (
        request.end.replace(tzinfo=timezone.utc)
        if request.end.tzinfo is None else request.end
    )

    closes: Dict[str, pd.Series] = {}
    for symbol in request.symbols:
        asset = await repo.find_asset(symbol)
        if asset is None:
            raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol!r}")
        records = await repo.fetch_range(
            symbol=symbol, asset_class=None, start=start, end=end
        )
        frame = records_to_frame(records)
        if frame.empty:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"No bars stored for {symbol!r} between "
                    f"{start.date()} and {end.date()}."
                ),
            )
        closes[symbol] = frame["Close"]

    # Inner join on the index: the test needs observations where EVERY series
    # has a value. Crypto trades weekends and equities do not, so pairing the
    # two without aligning would compare Monday's equity move against the
    # weekend's crypto move.
    aligned = pd.DataFrame(closes)[request.symbols].dropna()

    if spec.input_kind == "returns":
        aligned = aligned.pct_change().dropna()

    if len(aligned) < 2:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Only {len(aligned)} overlapping observation(s) across "
                f"{request.symbols} in this range — not enough to test. "
                "Crypto and equities share only weekdays."
            ),
        )

    try:
        result = await run_in_threadpool(_run_test_sync, spec, aligned, params)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    return StatisticsResponse(
        test_id=spec.id,
        display_name=spec.display_name,
        symbols=list(request.symbols),
        input_kind=spec.input_kind,
        observations=len(aligned),
        params=params,
        result=_json_safe(result),
    )
