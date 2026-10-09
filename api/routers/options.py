"""
api/routers/options.py

Option pricing and the implied-volatility surface. Tier 2 phase 3 of
research/option-pricing-plan-2026-10-08.md.

    GET  /api/v1/options/archive        captures on disk: tickers, days, rows
    GET  /api/v1/options/surface        one ticker-day's surface (cached) + the
                                        realised-vol cone (fresh, see below)
    POST /api/v1/options/price          every pricer side by side on one input
    GET  /api/v1/options/pricers        the pricer catalogue
    GET  /api/v1/options/realised-vol   realised vol and the cone for a symbol

A surface miss costs 10-35 s of CPU and is cached on disk; see
core/options_surface.py for the cache key and why a surface for today's
capture is recomputed tomorrow. The realised-vol cone is built from stored
bars ending at the capture date on every request, because bars are not part
of the cache key; when it cannot be built the surface is still returned, with
`cone` null and `cone_reason` saying why.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db, get_market_data_repo
from config import settings
from core.options_surface import (
    ChainArchive,
    DividendHistory,
    NoRateError,
    SurfaceCache,
    compute_for,
    json_safe,
    pricing_fingerprint,
    surface_inputs,
)
from core.rates import RateRepository
from db.repositories.dividends import TimescaleDividendRepo
from db.repositories.rates import TimescaleRateRepo
from pricing import registry as pricers
from pricing import volatility as V

router = APIRouter(prefix="/api/v1/options", tags=["options"])


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

def get_chain_archive() -> ChainArchive:
    return ChainArchive(Path(settings.OPTION_CHAIN_ARCHIVE_DIR))


@lru_cache(maxsize=1)
def _shared_cache() -> SurfaceCache:
    # One instance per process, so its per-key locks and semaphore are shared.
    return SurfaceCache(Path(settings.OPTIONS_SURFACE_CACHE_DIR))


def get_surface_cache() -> SurfaceCache:
    return _shared_cache()


async def get_rate_repo(session: AsyncSession = Depends(get_db)) -> RateRepository:
    return TimescaleRateRepo(session)


async def get_dividend_repo(session: AsyncSession = Depends(get_db)) -> DividendHistory:
    return TimescaleDividendRepo(session)


def get_pricing_fingerprint() -> str:
    return pricing_fingerprint()


# ---------------------------------------------------------------------------
# Models (all prefixed Option/Options: see tests/api/test_openapi_contract.py)
# ---------------------------------------------------------------------------

class OptionsCaptureDay(BaseModel):
    day: date
    rows: int
    partial: bool


class OptionsArchiveTicker(BaseModel):
    ticker: str
    captures: List[OptionsCaptureDay]


class OptionsArchiveResponse(BaseModel):
    tickers: List[OptionsArchiveTicker]
    total_rows: int


class OptionsRate(BaseModel):
    series: str
    observation_date: date
    quoted_pct: float = Field(description="As served: 13-week T-bill, bank-discount basis, percent")
    continuous: float
    stale: bool


class OptionsDividend(BaseModel):
    ex_date: date
    amount: float
    projected: bool


class OptionsExpiry(BaseModel):
    expiry: str
    T: float
    forward: float
    raw_parity_forward: float
    forward_passes: int
    forward_converged: bool
    implied_spot: float
    dividends_before_expiry: int
    dividend_date_uncertain: bool


class OptionsTermPoint(BaseModel):
    expiry: str
    T: float
    forward: float
    atm_iv: Optional[float]
    put25_iv: Optional[float]
    call25_iv: Optional[float]
    skew_25d: Optional[float]
    points: int
    dividend_date_uncertain: bool


class OptionsSmilePoint(BaseModel):
    strike: float
    is_call: bool
    log_moneyness: float
    forward_delta: Optional[float]
    iv: float
    vendor_iv: Optional[float] = Field(description="Yahoo's IV, for comparison only; not used")
    bid: float
    ask: float
    premium: Optional[float] = Field(description="Early-exercise premium removed before inversion")


class OptionsConeWindow(BaseModel):
    window: int
    min: float
    p25: float
    median: float
    p75: float
    max: float
    current: Optional[float]


class OptionsVolCone(BaseModel):
    symbol: str
    estimator: str
    as_of: date
    bars: int
    windows: List[OptionsConeWindow]


class OptionsSurfaceResponse(BaseModel):
    ticker: str
    day: date
    captured_at: datetime
    rate: OptionsRate
    dividends: List[OptionsDividend]
    quality: Dict[str, float]
    expiries: List[OptionsExpiry]
    term_structure: List[OptionsTermPoint]
    smiles: Dict[str, List[OptionsSmilePoint]]
    compute_seconds: float
    computed_at: datetime
    cache_hit: bool
    cone: Optional[OptionsVolCone]
    cone_reason: Optional[str] = None


class OptionPriceRequest(BaseModel):
    spot: float = Field(gt=0)
    strike: float = Field(gt=0)
    expiry_years: float = Field(gt=0, description="ACT/365")
    rate: float = Field(description="Continuously compounded, decimal")
    dividend_yield: float = Field(default=0.0, description="Continuous, decimal")
    sigma: float = Field(gt=0, le=5)
    right: Literal["call", "put"]
    style: Literal["european", "american"] = "european"
    mc_paths: int = Field(default=settings.MC_PRICER_PATHS, ge=2, le=2_000_000)
    seed: int = 42


class OptionModelQuote(BaseModel):
    model: str
    display_name: str
    style: str
    price: float
    std_error: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None
    rho: Optional[float] = None
    early_exercise_premium: Optional[float] = None


class OptionPriceResponse(BaseModel):
    style: str
    quotes: List[OptionModelQuote]


class OptionPricerInfo(BaseModel):
    id: str
    display_name: str
    description: str
    styles: List[str]


class OptionsRealisedVolResponse(BaseModel):
    symbol: str
    as_of: date
    window: int
    estimators: Dict[str, Optional[float]]
    cone: OptionsVolCone


# ---------------------------------------------------------------------------
# Realised-vol cone from stored bars
# ---------------------------------------------------------------------------

class ConeUnavailable(Exception):
    pass


async def build_cone(repo: Any, symbol: str, as_of: date, estimator: str = "yang_zhang") -> OptionsVolCone:
    """
    Rolling realised vol over the full stored history ENDING AT `as_of`, and
    each window's latest value. Total-adjusted log returns before a later
    split or dividend do not change, so ending at `as_of` has no look-ahead.
    """
    asset = await repo.find_asset(symbol)
    if asset is None:
        raise ConeUnavailable(f"{symbol} is not a registered asset")
    if asset.price_basis != "served":
        raise ConeUnavailable(
            f"{symbol} is not read-time adjusted (price_basis={asset.price_basis!r}); "
            "its stored history may carry unadjusted splits"
        )
    end = datetime.combine(as_of, time(23, 59, 59), tzinfo=timezone.utc)
    start = datetime(2000, 1, 1, tzinfo=timezone.utc)
    records = await repo.fetch_range(symbol, None, start, end, adjust="total")
    bars = [r.ohlcv for r in records if None not in (r.ohlcv.open, r.ohlcv.high, r.ohlcv.low, r.ohlcv.close)]
    if len(bars) < max(settings.REALISED_VOL_WINDOWS) + 2:
        raise ConeUnavailable(f"{symbol} has {len(bars)} usable bars up to {as_of}; the cone needs more")
    o, h, lo, c = (np.array([getattr(b, f) for b in bars], dtype=float) for f in ("open", "high", "low", "close"))
    cone = V.vol_cone(o, h, lo, c, settings.REALISED_VOL_WINDOWS, estimator)
    windows = []
    for w, stats in cone.items():
        latest = V.rolling(estimator, o[-(w + 1):], h[-(w + 1):], lo[-(w + 1):], c[-(w + 1):], w)[-1]
        windows.append(OptionsConeWindow(window=w, current=json_safe(float(latest)), **stats))
    return OptionsVolCone(symbol=symbol, estimator=estimator, as_of=as_of, bars=len(bars), windows=windows)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/archive", response_model=OptionsArchiveResponse, summary="Option-chain captures on disk")
async def archive(archive: ChainArchive = Depends(get_chain_archive)) -> OptionsArchiveResponse:
    by_ticker: Dict[str, List[OptionsCaptureDay]] = {}
    total = 0
    for cap in archive.captures():
        by_ticker.setdefault(cap.ticker, []).append(OptionsCaptureDay(day=cap.day, rows=cap.rows, partial=cap.partial))
        total += cap.rows
    return OptionsArchiveResponse(
        tickers=[OptionsArchiveTicker(ticker=t, captures=c) for t, c in sorted(by_ticker.items())],
        total_rows=total,
    )


@router.get(
    "/surface",
    response_model=OptionsSurfaceResponse,
    summary="Implied-volatility surface for one ticker-day",
    responses={404: {"description": "No complete capture for that ticker-day"},
               409: {"description": "No risk-free rate stored on or before that day"}},
)
async def surface(
    ticker: str = Query(min_length=1, max_length=12),
    day: Optional[date] = Query(default=None, description="Capture date; default the latest complete one"),
    archive: ChainArchive = Depends(get_chain_archive),
    cache: SurfaceCache = Depends(get_surface_cache),
    rates: RateRepository = Depends(get_rate_repo),
    dividends: DividendHistory = Depends(get_dividend_repo),
    repo: Any = Depends(get_market_data_repo),
    code: str = Depends(get_pricing_fingerprint),
) -> OptionsSurfaceResponse:
    ticker = ticker.upper()
    if day is None:
        days = [c.day for c in archive.captures() if c.ticker == ticker and not c.partial]
        if not days:
            raise HTTPException(status_code=404, detail=f"No complete captures for {ticker}")
        day = max(days)
    capture = archive.find(ticker, day)
    if capture is None:
        raise HTTPException(status_code=404, detail=f"No complete capture for {ticker} on {day}")

    try:
        inputs = await surface_inputs(capture, rates, dividends, code)
    except NoRateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    payload, hit = await cache.get_or_compute(inputs, compute_for(inputs), run_in_threadpool)

    try:
        cone, cone_reason = await build_cone(repo, ticker, day), None
    except ConeUnavailable as exc:
        cone, cone_reason = None, str(exc)
    return OptionsSurfaceResponse(
        **{k: v for k, v in payload.items() if k != "key"},
        cache_hit=hit, cone=cone, cone_reason=cone_reason,
    )


@router.get("/pricers", response_model=List[OptionPricerInfo], summary="The option pricers")
async def list_pricers() -> List[OptionPricerInfo]:
    return [OptionPricerInfo(id=p.id, display_name=p.display_name, description=p.description, styles=list(p.styles))
            for p in pricers.PRICERS]


@router.post("/price", response_model=OptionPriceResponse, summary="Price one option with every applicable model")
async def price(request: OptionPriceRequest) -> OptionPriceResponse:
    x = pricers.PricerInputs(
        S=request.spot, K=request.strike, T=request.expiry_years, r=request.rate,
        q=request.dividend_yield, sigma=request.sigma, right=request.right,
        seed=request.seed, mc_paths=request.mc_paths + (request.mc_paths % 2),
    )

    def run() -> List[OptionModelQuote]:
        out = []
        for spec in pricers.PRICERS:
            if request.style not in spec.styles:
                continue
            q = spec.run(x, request.style)
            out.append(OptionModelQuote(model=spec.id, display_name=spec.display_name, style=request.style,
                                        **json_safe(q.__dict__)))
        return out

    return OptionPriceResponse(style=request.style, quotes=await run_in_threadpool(run))


@router.get(
    "/realised-vol",
    response_model=OptionsRealisedVolResponse,
    summary="Realised volatility and the vol cone for a symbol",
    responses={404: {"description": "Unknown symbol"}, 422: {"description": "Not read-time adjusted, or too few bars"}},
)
async def realised_vol(
    symbol: str = Query(min_length=1, max_length=20),
    window: int = Query(default=20, ge=5, le=252),
    as_of: Optional[date] = Query(default=None),
    repo: Any = Depends(get_market_data_repo),
) -> OptionsRealisedVolResponse:
    symbol = symbol.upper()
    as_of = as_of or datetime.now(timezone.utc).date()
    if await repo.find_asset(symbol) is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol {symbol}")
    try:
        cone = await build_cone(repo, symbol, as_of)
    except ConeUnavailable as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    end = datetime.combine(as_of, time(23, 59, 59), tzinfo=timezone.utc)
    records = await repo.fetch_range(symbol, None, datetime(2000, 1, 1, tzinfo=timezone.utc), end, adjust="total")
    bars = [r.ohlcv for r in records][-(window + 1):]
    o, h, lo, c = (np.array([getattr(b, f) for b in bars], dtype=float) for f in ("open", "high", "low", "close"))
    estimators = {name: json_safe(float(fn(o[1:], h[1:], lo[1:], c[1:]) if name not in ("close_to_close", "yang_zhang")
                                       else fn(o, h, lo, c)))
                  for name, fn in V.ESTIMATORS.items()}
    return OptionsRealisedVolResponse(symbol=symbol, as_of=as_of, window=window, estimators=estimators, cone=cone)
