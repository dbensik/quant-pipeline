"""
api/routers/valuation.py

DCF valuation on point-in-time SEC fundamentals. Tier 3 phase 4 of
research/dcf-plan-2026-10-08.md.

    GET  /api/v1/valuation/coverage               CIKs resolved, facts stored, newest filing
    GET  /api/v1/valuation/{symbol}/statements    standardised line items as of a date
    POST /api/v1/valuation/dcf                    one valuation: value, bridge, inputs with
                                                  sources, WACC, sensitivity, implied growth
    GET  /api/v1/valuation/{symbol}/history       value vs price the day after each filing

Everything is as of a date and uses only what was public by then (facts filed
strictly before it). A valuation that cannot be made answers 422 with the
reason (a bank, no CIK, no facts, no price); no risk-free rate stored is 409.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db, get_market_data_repo
from core import valuation_service as svc
from core.options_surface import json_safe
from core.rates import RateRepository
from db.repositories.rates import TimescaleRateRepo
from db.repositories.valuation import TimescaleValuationRepo

router = APIRouter(prefix="/api/v1/valuation", tags=["valuation"])


async def get_valuation_data(session: AsyncSession = Depends(get_db)) -> svc.ValuationData:
    return TimescaleValuationRepo(session)


async def get_valuation_rates(session: AsyncSession = Depends(get_db)) -> RateRepository:
    return TimescaleRateRepo(session)


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _raise(exc: svc.ValuationError):
    raise HTTPException(status_code=exc.status, detail=exc.reason) from None


# ---------------------------------------------------------------------------
# Models (all prefixed Valuation: see tests/api/test_openapi_contract.py)
# ---------------------------------------------------------------------------


class ValuationCoverage(BaseModel):
    equities: int
    with_cik: int
    facts: int
    ciks_with_facts: int
    newest_filed: Optional[date]


class ValuationLineItem(BaseModel):
    period_start: Optional[date]
    period_end: date
    value: float
    concept: str
    filed: date
    derived: bool = False


class ValuationStatements(BaseModel):
    symbol: str
    cik: int
    as_of: date
    annual: Dict[str, List[ValuationLineItem]]
    ttm: Dict[str, Optional[float]]
    ttm_through: Dict[str, Optional[date]]
    balances: Dict[str, Optional[ValuationLineItem]]


class ValuationRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=12)
    as_of: Optional[date] = Field(default=None, description="Default today")
    growth_first: Optional[float] = Field(default=None, ge=-0.5, le=1.0)
    growth_terminal: Optional[float] = Field(default=None, ge=-0.05, le=0.06)
    wacc: Optional[float] = Field(default=None, gt=0, le=0.5)
    exit_multiple: Optional[float] = Field(default=None, gt=0, le=100)
    years: Optional[int] = Field(default=None, ge=1, le=20)
    beta: Optional[float] = Field(
        default=None, ge=-1, le=4, description="Replaces the daily 3-year beta"
    )


class ValuationBeta(BaseModel):
    beta: float
    std_error: Optional[float]
    observations: int


class ValuationInputLine(BaseModel):
    value: Optional[float]
    source: str


class ValuationDiscount(BaseModel):
    wacc: float
    cost_of_equity: float
    cost_of_debt_pre_tax: float
    cost_of_debt_after_tax: float
    weight_equity: float
    weight_debt: float
    cost_of_debt_estimated: bool
    #: The CAPM-weighted WACC before decision 14's floor.
    wacc_unfloored: float
    floored: bool
    notes: List[str]


class ValuationDCFInputs(BaseModel):
    base_fcf: float
    growth_first: float
    growth_terminal: float
    wacc: float
    net_debt: float
    shares: float
    years: int
    exit_metric_base: Optional[float]
    exit_multiple: Optional[float]


class ValuationResult(BaseModel):
    inputs: ValuationDCFInputs
    growth_path: List[float]
    fcf: List[float]
    pv_fcf: List[float]
    terminal_value: Optional[float]
    pv_terminal: Optional[float]
    enterprise_value: Optional[float]
    equity_value: Optional[float]
    per_share: Optional[float]
    terminal_share: Optional[float]
    exit_enterprise_value: Optional[float]
    exit_per_share: Optional[float]
    reason: Optional[str]
    warnings: List[str]


class ValuationGrid(BaseModel):
    wacc: List[float]
    growth_terminal: List[float]
    per_share: List[List[Optional[float]]] = Field(
        description="Rows by WACC, columns by terminal growth; null where wacc <= growth"
    )


class ValuationResponse(BaseModel):
    symbol: str
    cik: int
    predecessor_ciks: List[int] = Field(
        description="Earlier filers read with `cik` (holding-company reorganisations)"
    )
    sector: Optional[str]
    as_of: date
    price: float = Field(description="Close AS TRADED on price_date: the basis of the share count")
    price_date: date
    market_cap: float
    result: ValuationResult
    discount: ValuationDiscount
    lines: Dict[str, ValuationInputLine]
    implied_growth_first: Optional[float] = Field(description="First-year growth the price implies")
    implied_reason: Optional[str]
    notes: List[str]
    betas: Dict[str, Optional[ValuationBeta]] = Field(
        description="daily_3y (decision 4, the default), daily_3y_blume, weekly_5y"
    )
    overridden: List[str]
    sensitivity: ValuationGrid


class ValuationHistoryPoint(BaseModel):
    as_of: date
    price: Optional[float]
    per_share: Optional[float]
    exit_per_share: Optional[float] = None
    wacc: Optional[float] = None
    implied_growth_first: Optional[float] = None
    reason: Optional[str] = None


class ValuationHistory(BaseModel):
    symbol: str
    points: List[ValuationHistoryPoint]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/coverage", response_model=ValuationCoverage, summary="What fundamentals are stored")
async def coverage(data: svc.ValuationData = Depends(get_valuation_data)) -> ValuationCoverage:
    return ValuationCoverage(**await data.coverage())


ANNUAL_ITEMS = (
    "revenue",
    "operating_income",
    "net_income",
    "cfo",
    "capex",
    "share_based_comp",
    "d_and_a",
    "diluted_shares",
)
BALANCE_ITEMS = (
    "cash",
    "securities_current",
    "securities_noncurrent",
    "long_term_debt",
    "commercial_paper",
    "equity",
    "shares_outstanding",
)


@router.get(
    "/{symbol}/statements",
    response_model=ValuationStatements,
    summary="Standardised line items as of a date",
    responses={
        404: {"description": "Not a registered equity"},
        422: {"description": "No CIK or no facts"},
    },
)
async def statements(
    symbol: str,
    as_of: Optional[date] = Query(default=None),
    data: svc.ValuationData = Depends(get_valuation_data),
) -> ValuationStatements:
    as_of = as_of or _today()
    try:
        st, cik, _, _ = await svc.load_statements(data, symbol)
    except svc.ValuationError as exc:
        _raise(exc)
    item = lambda v: ValuationLineItem(**v.__dict__) if v else None  # noqa: E731
    ttm = {k: st.ttm(k, as_of) for k in ANNUAL_ITEMS if k != "diluted_shares"}
    return ValuationStatements(
        symbol=symbol.upper(),
        cik=cik,
        as_of=as_of,
        annual={k: [item(v) for v in st.annual(k, as_of)] for k in ANNUAL_ITEMS},
        ttm={k: (t[0] if t else None) for k, t in ttm.items()},
        ttm_through={k: (t[1][-1].period_end if t else None) for k, t in ttm.items()},
        balances={k: item(st.instant(k, as_of)) for k in BALANCE_ITEMS},
    )


@router.post(
    "/dcf",
    response_model=ValuationResponse,
    summary="Discounted cash flow valuation as of a date",
    responses={
        404: {"description": "Not a registered equity"},
        409: {"description": "No risk-free rate stored for the date"},
        422: {"description": "Cannot be valued: the reason is in `detail`"},
    },
)
async def dcf(
    request: ValuationRequest,
    data: svc.ValuationData = Depends(get_valuation_data),
    prices: Any = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_valuation_rates),
) -> ValuationResponse:
    try:
        out = await svc.value(
            data,
            prices,
            rates,
            request.symbol.upper(),
            request.as_of or _today(),
            svc.Overrides(
                request.growth_first,
                request.beta,
                request.growth_terminal,
                request.wacc,
                request.exit_multiple,
                request.years,
            ),
        )
    except svc.ValuationError as exc:
        _raise(exc)
    return ValuationResponse(**json_safe(out))


@router.get(
    "/{symbol}/history",
    response_model=ValuationHistory,
    summary="DCF value against price the day after each 10-K/10-Q filing",
    responses={
        404: {"description": "Not a registered equity"},
        422: {"description": "No CIK or no facts"},
    },
)
async def value_history(
    symbol: str,
    since: Optional[date] = Query(default=None),
    data: svc.ValuationData = Depends(get_valuation_data),
    prices: Any = Depends(get_market_data_repo),
    rates: RateRepository = Depends(get_valuation_rates),
) -> ValuationHistory:
    try:
        points = await svc.history(data, prices, rates, symbol.upper(), since)
    except svc.ValuationError as exc:
        _raise(exc)
    return ValuationHistory(symbol=symbol.upper(), points=json_safe(points))
