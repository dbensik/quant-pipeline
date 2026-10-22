"""
api/routers/options.py

Option pricing and the implied-volatility surface. Tier 2 phase 3 of
research/option-pricing-plan-2026-10-08.md.

    GET  /api/v1/options/archive        captures on disk: tickers, days, rows
    GET  /api/v1/options/surface        one ticker-day's surface (cached) + the
                                        realised-vol cone (fresh, see below)
    GET  /api/v1/options/quotes         one expiry's raw bid/ask, both rights
                                        (the payoff page's premiums, decision 54)
    POST /api/v1/options/position-curve a multi-leg position before expiry:
                                        profit and Greeks on a spot grid, each
                                        leg at its own smile vol (decision 55)
    GET  /api/v1/options/svi            an SVI fit per expiry and the butterfly
                                        and calendar checks, fit beside quotes
                                        (decision 56)
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
import pandas as pd
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
    projected_dividends,
    surface_inputs,
)
from core.rates import RateRepository
from core.svi_report import ReportCache
from db.repositories.dividends import TimescaleDividendRepo
from db.repositories.rates import TimescaleRateRepo
from pricing import position as P
from pricing import registry as pricers
from pricing import volatility as V
from pricing.dividends import near_ex_date

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


@lru_cache(maxsize=1)
def get_svi_cache() -> ReportCache:
    return ReportCache()


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


class OptionsQuote(BaseModel):
    strike: float
    call_bid: Optional[float]
    call_ask: Optional[float]
    put_bid: Optional[float]
    put_ask: Optional[float]


class OptionsQuotesResponse(BaseModel):
    ticker: str
    day: date
    captured_at: datetime
    expiries: List[str] = Field(description="Every expiry in the capture")
    expiry: str
    spot: Optional[float] = Field(
        description="The spot the capture read beside this expiry's quotes (median)"
    )
    quotes: List[OptionsQuote] = Field(description="By strike, as served")
    dividend_date_uncertain: List[str] = Field(
        description="Expiries within OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS of a projected"
        " ex-date: the surface's forward there may sit on the wrong side of the dividend"
    )


class OptionPositionLeg(BaseModel):
    kind: Literal["call", "put", "stock"]
    side: Literal[1, -1] = Field(description="+1 bought, -1 sold")
    quantity: float = Field(gt=0, description="Contracts, or shares for stock")
    strike: Optional[float] = Field(default=None, gt=0, description="Options only")
    expiry: Optional[date] = Field(default=None, description="Options only")
    premium: float = Field(ge=0, description="Per share; the stock leg's entry price")


class OptionPositionCurveRequest(BaseModel):
    ticker: str = Field(min_length=1, max_length=12)
    day: Optional[date] = Field(default=None, description="Capture; default the latest")
    legs: List[OptionPositionLeg] = Field(min_length=1)
    as_of: Optional[date] = Field(
        default=None, description="Date on the curve; default the capture day"
    )
    vol_rule: P.VolRule = "sticky_strike"
    spot: Optional[float] = Field(
        default=None, gt=0, description="Reference spot S0; default the capture's (median)"
    )
    low: Optional[float] = Field(default=None, gt=0)
    high: Optional[float] = Field(default=None, gt=0)


class OptionPositionLegQuote(BaseModel):
    T_remaining: float = Field(description="Years left on the curve's date")
    forward: Optional[float] = Field(description="Model forward at the reference spot")
    carry: Optional[float] = Field(description="q reproducing the parity forward")
    iv: Optional[float] = Field(description="The leg's vol at the reference spot")
    vol_source: Optional[Literal["interpolated", "extrapolated"]]
    model_price: float = Field(description="Per share at the reference spot on the date")


class OptionPositionPoint(BaseModel):
    spot: float
    value: float
    profit: float
    delta: float = Field(description="$ per $1 of the underlying")
    gamma: float = Field(description="Delta's change per $1")
    vega: float = Field(description="$ per vol point, every leg shifted together")
    theta: float = Field(description="$ per calendar day passing")
    rho: float = Field(description="$ per percentage point of the rate")


class OptionPositionCurveResponse(BaseModel):
    ticker: str
    day: date
    as_of: date
    last_expiry: Optional[date]
    vol_rule: str
    spot: float
    rate: float = Field(description="The capture's continuous rate")
    cache_hit: bool = Field(description="False: the surface was computed for this request")
    legs: List[OptionPositionLegQuote]
    points: List[OptionPositionPoint]


ArbitrageVerdict = Literal["none", "both", "fit_only", "quotes_only"]


class OptionSVIRange(BaseModel):
    low: float = Field(description="ln(strike / forward)")
    high: float


class OptionSVIParams(BaseModel):
    a: float
    b: float
    rho: float
    m: float
    sigma: float


class OptionSVICurvePoint(BaseModel):
    log_moneyness: float
    iv: float


class OptionSVIButterfly(BaseModel):
    verdict: ArbitrageVerdict
    checked: OptionSVIRange = Field(description="The quoted range plus the margin either side")
    fit: List[OptionSVIRange] = Field(description="Where the fit's density g(k) is negative")
    min_g: float
    quotes: List[OptionSVIRange] = Field(
        description="Adjacent strikes of one right whose butterfly costs less than nothing at bid/ask"
    )


class OptionSVIFit(BaseModel):
    expiry: str
    T: float
    days: float
    points: int
    params: OptionSVIParams
    rmse_vol_pts: float = Field(description="Root-mean-square miss of our IV, in vol points")
    within_spread_share: Optional[float] = Field(
        description="Share of quotes the fit passes within half their bid-ask spread, in vol"
    )
    atm_iv: float
    min_iv: float
    left_wing_slope: float
    right_wing_slope: float
    bottom_quoted: bool = Field(
        description="The smile's bottom m lies among the quotes; only then read the parameters one by one"
    )
    wings_within_lee: bool = Field(description="Both wing slopes of total variance are 2 or less")
    k_range: OptionSVIRange
    curve: List[OptionSVICurvePoint]
    butterfly: OptionSVIButterfly


class OptionSVISkipped(BaseModel):
    expiry: str
    reason: str


class OptionSVICalendar(BaseModel):
    near: str
    far: str
    verdict: ArbitrageVerdict
    shared: Optional[OptionSVIRange] = Field(description="Quoted ln(K/F) range both expiries cover")
    fit: List[OptionSVIRange] = Field(
        description="Where the far fit's total variance is below the near one's"
    )
    quotes: List[OptionSVIRange] = Field(
        description="The same on the quotes, interpolated in ln(K/F)"
    )


class OptionSVITally(BaseModel):
    none: int
    both: int
    fit_only: int
    quotes_only: int


class OptionSVIResponse(BaseModel):
    ticker: str
    day: date
    captured_at: datetime
    parameters: Dict[str, str] = Field(description="What each SVI parameter means")
    min_points: int
    min_days: float
    butterfly_margin: float
    fits: List[OptionSVIFit]
    skipped: List[OptionSVISkipped]
    calendar: List[OptionSVICalendar]
    butterfly_summary: OptionSVITally
    calendar_summary: OptionSVITally
    fit_seconds: float
    surface_cache_hit: bool


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


async def build_cone(
    repo: Any, symbol: str, as_of: date, estimator: str = "yang_zhang"
) -> OptionsVolCone:
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
    bars = [
        r.ohlcv
        for r in records
        if None not in (r.ohlcv.open, r.ohlcv.high, r.ohlcv.low, r.ohlcv.close)
    ]
    if len(bars) < max(settings.REALISED_VOL_WINDOWS) + 2:
        raise ConeUnavailable(
            f"{symbol} has {len(bars)} usable bars up to {as_of}; the cone needs more"
        )
    o, h, lo, c = (
        np.array([getattr(b, f) for b in bars], dtype=float)
        for f in ("open", "high", "low", "close")
    )
    cone = V.vol_cone(o, h, lo, c, settings.REALISED_VOL_WINDOWS, estimator)
    windows = []
    for w, stats in cone.items():
        latest = V.rolling(
            estimator, o[-(w + 1) :], h[-(w + 1) :], lo[-(w + 1) :], c[-(w + 1) :], w
        )[-1]
        windows.append(OptionsConeWindow(window=w, current=json_safe(float(latest)), **stats))
    return OptionsVolCone(
        symbol=symbol, estimator=estimator, as_of=as_of, bars=len(bars), windows=windows
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "/archive", response_model=OptionsArchiveResponse, summary="Option-chain captures on disk"
)
async def archive(archive: ChainArchive = Depends(get_chain_archive)) -> OptionsArchiveResponse:
    by_ticker: Dict[str, List[OptionsCaptureDay]] = {}
    total = 0
    for cap in archive.captures():
        by_ticker.setdefault(cap.ticker, []).append(
            OptionsCaptureDay(day=cap.day, rows=cap.rows, partial=cap.partial)
        )
        total += cap.rows
    return OptionsArchiveResponse(
        tickers=[OptionsArchiveTicker(ticker=t, captures=c) for t, c in sorted(by_ticker.items())],
        total_rows=total,
    )


@router.get(
    "/surface",
    response_model=OptionsSurfaceResponse,
    summary="Implied-volatility surface for one ticker-day",
    responses={
        404: {"description": "No complete capture for that ticker-day"},
        409: {"description": "No risk-free rate stored on or before that day"},
    },
)
async def surface(
    ticker: str = Query(min_length=1, max_length=12),
    day: Optional[date] = Query(
        default=None, description="Capture date; default the latest complete one"
    ),
    archive: ChainArchive = Depends(get_chain_archive),
    cache: SurfaceCache = Depends(get_surface_cache),
    rates: RateRepository = Depends(get_rate_repo),
    dividends: DividendHistory = Depends(get_dividend_repo),
    repo: Any = Depends(get_market_data_repo),
    code: str = Depends(get_pricing_fingerprint),
) -> OptionsSurfaceResponse:
    ticker = ticker.upper()
    capture = latest_capture(archive, ticker, day)

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
        cache_hit=hit,
        cone=cone,
        cone_reason=cone_reason,
    )


@router.get(
    "/svi",
    response_model=OptionSVIResponse,
    summary="An SVI fit per expiry, with butterfly and calendar arbitrage on the fit and the quotes",
    responses={
        404: {"description": "No complete capture for that ticker-day"},
        409: {"description": "No risk-free rate stored on or before that day"},
    },
)
async def svi_fits(
    ticker: str = Query(min_length=1, max_length=12),
    day: Optional[date] = Query(
        default=None, description="Capture date; default the latest complete one"
    ),
    archive: ChainArchive = Depends(get_chain_archive),
    cache: SurfaceCache = Depends(get_surface_cache),
    rates: RateRepository = Depends(get_rate_repo),
    dividends: DividendHistory = Depends(get_dividend_repo),
    code: str = Depends(get_pricing_fingerprint),
    reports: ReportCache = Depends(get_svi_cache),
) -> OptionSVIResponse:
    """Reads the surface (cached, or computed once as /surface would) and re-prices nothing."""
    ticker = ticker.upper()
    capture = latest_capture(archive, ticker, day)
    try:
        inputs = await surface_inputs(capture, rates, dividends, code)
    except NoRateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    payload, hit = await cache.get_or_compute(inputs, compute_for(inputs), run_in_threadpool)
    report = await run_in_threadpool(reports.get_or_build, payload)
    return OptionSVIResponse(**report, surface_cache_hit=hit)


def latest_capture(archive: ChainArchive, ticker: str, day: Optional[date]):
    if day is None:
        days = [c.day for c in archive.captures() if c.ticker == ticker and not c.partial]
        if not days:
            raise HTTPException(status_code=404, detail=f"No complete captures for {ticker}")
        day = max(days)
    capture = archive.find(ticker, day)
    if capture is None:
        raise HTTPException(status_code=404, detail=f"No complete capture for {ticker} on {day}")
    return capture


def _quote(v: Any) -> Optional[float]:
    v = float(v)
    return None if not np.isfinite(v) else v


def quotes_of(chain: pd.DataFrame, expiry: Optional[str], target_days: int) -> Dict[str, Any]:
    """One expiry's bid and ask per strike, both rights; expiry defaults to the one nearest a month."""
    expiries = sorted(str(e) for e in chain["expiry"].unique())
    day = date.fromisoformat(str(chain["trade_date"].iloc[0]))
    if expiry is None:
        expiry = min(expiries, key=lambda e: abs((date.fromisoformat(e) - day).days - target_days))
    if expiry not in expiries:
        raise HTTPException(status_code=404, detail=f"No expiry {expiry} in this capture")
    rows = chain[chain["expiry"].astype(str) == expiry]
    by_strike: Dict[float, Dict[str, Optional[float]]] = {}
    for strike, right, bid, ask in rows[["strike", "right", "bid", "ask"]].itertuples(index=False):
        side = "call" if right == "C" else "put"
        q = by_strike.setdefault(float(strike), {})
        q[f"{side}_bid"], q[f"{side}_ask"] = _quote(bid), _quote(ask)
    quotes = [
        OptionsQuote(
            strike=k,
            **{f: q.get(f) for f in ("call_bid", "call_ask", "put_bid", "put_ask")},
        )
        for k, q in sorted(by_strike.items())
    ]
    spot = rows["spot"].dropna()
    return {
        "captured_at": pd.Timestamp(chain["captured_at_utc"].min()).to_pydatetime(),
        "expiries": expiries,
        "expiry": expiry,
        "spot": float(spot.median()) if len(spot) else None,
        "quotes": quotes,
    }


@router.get(
    "/quotes",
    response_model=OptionsQuotesResponse,
    summary="One expiry's bid and ask from a capture, both rights",
    responses={404: {"description": "No complete capture, or no such expiry in it"}},
)
async def quotes(
    ticker: str = Query(min_length=1, max_length=12),
    day: Optional[date] = Query(
        default=None, description="Capture date; default the latest complete one"
    ),
    expiry: Optional[str] = Query(
        default=None, description="YYYY-MM-DD; default the expiry nearest a month out"
    ),
    archive: ChainArchive = Depends(get_chain_archive),
    dividends: DividendHistory = Depends(get_dividend_repo),
) -> OptionsQuotesResponse:
    ticker = ticker.upper()
    capture = latest_capture(archive, ticker, day)
    chain = await run_in_threadpool(pd.read_parquet, capture.path)
    body = quotes_of(chain, expiry, settings.OPTION_QUOTES_TARGET_DAYS)
    # The surface's own rule (pricing/chains.py), so the page flags what it prices.
    schedule = projected_dividends(
        await dividends.dividend_history(ticker, capture.day), capture.day
    )
    uncertain = [
        e
        for e in body["expiries"]
        if near_ex_date(
            schedule, date.fromisoformat(e), settings.OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS
        )
    ]
    return OptionsQuotesResponse(
        ticker=ticker, day=capture.day, dividend_date_uncertain=uncertain, **body
    )


def expiries_of(surface: Dict[str, Any]) -> Dict[date, P.Expiry]:
    """Every expiry of a surface with a usable smile."""
    out = {}
    for e in surface["expiries"]:
        points = surface["smiles"].get(e["expiry"]) or []
        if len(points) < 2 or not e["T"] or e["T"] <= 0:
            continue
        expiry = date.fromisoformat(e["expiry"])
        try:
            smile = P.Smile([p["log_moneyness"] for p in points], [p["iv"] for p in points])
        except ValueError:
            continue
        out[expiry] = P.Expiry(expiry, float(e["T"]), float(e["forward"]), smile)
    return out


def spot_grid(spot0: float, low: float, high: float, strikes: List[float], n: int) -> np.ndarray:
    extra = [x for x in [spot0, *strikes] if low < x < high]
    return np.unique(np.concatenate([np.linspace(low, high, n), extra]))


@router.post(
    "/position-curve",
    response_model=OptionPositionCurveResponse,
    summary="A multi-leg position before expiry: profit and Greeks on a spot grid",
    responses={
        404: {"description": "No complete capture for that ticker-day"},
        409: {"description": "No risk-free rate stored on or before that day"},
        422: {"description": "A leg without a strike or a smile, or a date out of range"},
    },
)
async def position_curve(
    request: OptionPositionCurveRequest,
    archive: ChainArchive = Depends(get_chain_archive),
    cache: SurfaceCache = Depends(get_surface_cache),
    rates: RateRepository = Depends(get_rate_repo),
    dividends: DividendHistory = Depends(get_dividend_repo),
    code: str = Depends(get_pricing_fingerprint),
) -> OptionPositionCurveResponse:
    ticker = request.ticker.upper()
    if len(request.legs) > settings.OPTION_POSITION_MAX_LEGS:
        raise HTTPException(
            status_code=422, detail=f"At most {settings.OPTION_POSITION_MAX_LEGS} legs"
        )
    for i, leg in enumerate(request.legs):
        if leg.kind != "stock" and (leg.strike is None or leg.expiry is None):
            raise HTTPException(status_code=422, detail=f"Leg {i + 1} needs a strike and an expiry")
    capture = latest_capture(archive, ticker, request.day)
    try:
        inputs = await surface_inputs(capture, rates, dividends, code)
    except NoRateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    surface, hit = await cache.get_or_compute(inputs, compute_for(inputs), run_in_threadpool)

    expiries = expiries_of(surface)
    legs = [
        P.Leg(leg.kind, leg.side, leg.quantity, leg.premium, leg.strike, leg.expiry)
        for leg in request.legs
    ]
    for i, leg in enumerate(legs):
        if leg.kind != "stock" and leg.expiry not in expiries:
            raise HTTPException(
                status_code=422,
                detail=f"Leg {i + 1}: no smile for expiry {leg.expiry} in this capture",
            )
    option_expiries = [leg.expiry for leg in legs if leg.kind != "stock"]
    last = max(option_expiries) if option_expiries else None
    as_of = request.as_of or capture.day
    if as_of < capture.day or (last is not None and as_of > last):
        raise HTTPException(
            status_code=422,
            detail=f"as_of must be between the capture ({capture.day}) and the last expiry ({last})",
        )

    if request.spot is not None:
        spot0 = request.spot
    else:
        chain = await run_in_threadpool(pd.read_parquet, capture.path, columns=["spot"])
        spot0 = float(chain["spot"].dropna().median())
    low = request.low or spot0 * 0.75
    high = request.high or spot0 * 1.25
    if not low < high:
        raise HTTPException(status_code=422, detail="low must be below high")
    rate = float(surface["rate"]["continuous"])
    strikes = [leg.strike for leg in legs if leg.kind != "stock"]
    spots = spot_grid(spot0, low, high, strikes, settings.OPTION_POSITION_CURVE_POINTS)

    def run():
        curve = P.position_curve(
            legs, expiries, spots, spot0, rate, capture.day, as_of, request.vol_rule
        )
        quotes = P.leg_quotes(legs, expiries, spot0, rate, capture.day, as_of, request.vol_rule)
        prices = [
            P.position_curve(
                [P.Leg(leg.kind, 1, 1, 0.0, leg.strike, leg.expiry)],
                expiries,
                [spot0],
                spot0,
                rate,
                capture.day,
                as_of,
                request.vol_rule,
            )["value"][0]
            / P.MULTIPLIER[leg.kind]
            for leg in legs
        ]
        return curve, quotes, prices

    curve, quotes, prices = await run_in_threadpool(run)
    return OptionPositionCurveResponse(
        ticker=ticker,
        day=capture.day,
        as_of=as_of,
        last_expiry=last,
        vol_rule=request.vol_rule,
        spot=spot0,
        rate=rate,
        cache_hit=hit,
        legs=[
            OptionPositionLegQuote(**q.__dict__, model_price=float(p))
            for q, p in zip(quotes, prices, strict=True)
        ],
        points=[
            OptionPositionPoint(
                spot=float(s), **{f: float(curve[f][i]) for f in ("value", "profit") + P.GREEKS}
            )
            for i, s in enumerate(spots)
        ],
    )


@router.get("/pricers", response_model=List[OptionPricerInfo], summary="The option pricers")
async def list_pricers() -> List[OptionPricerInfo]:
    return [
        OptionPricerInfo(
            id=p.id, display_name=p.display_name, description=p.description, styles=list(p.styles)
        )
        for p in pricers.PRICERS
    ]


@router.post(
    "/price",
    response_model=OptionPriceResponse,
    summary="Price one option with every applicable model",
)
async def price(request: OptionPriceRequest) -> OptionPriceResponse:
    x = pricers.PricerInputs(
        S=request.spot,
        K=request.strike,
        T=request.expiry_years,
        r=request.rate,
        q=request.dividend_yield,
        sigma=request.sigma,
        right=request.right,
        seed=request.seed,
        mc_paths=request.mc_paths + (request.mc_paths % 2),
    )

    def run() -> List[OptionModelQuote]:
        out = []
        for spec in pricers.PRICERS:
            if request.style not in spec.styles:
                continue
            q = spec.run(x, request.style)
            out.append(
                OptionModelQuote(
                    model=spec.id,
                    display_name=spec.display_name,
                    style=request.style,
                    **json_safe(q.__dict__),
                )
            )
        return out

    return OptionPriceResponse(style=request.style, quotes=await run_in_threadpool(run))


@router.get(
    "/realised-vol",
    response_model=OptionsRealisedVolResponse,
    summary="Realised volatility and the vol cone for a symbol",
    responses={
        404: {"description": "Unknown symbol"},
        422: {"description": "Not read-time adjusted, or too few bars"},
    },
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
    records = await repo.fetch_range(
        symbol, None, datetime(2000, 1, 1, tzinfo=timezone.utc), end, adjust="total"
    )
    bars = [r.ohlcv for r in records][-(window + 1) :]
    o, h, lo, c = (
        np.array([getattr(b, f) for b in bars], dtype=float)
        for f in ("open", "high", "low", "close")
    )
    estimators = {
        name: json_safe(
            float(
                fn(o[1:], h[1:], lo[1:], c[1:])
                if name not in ("close_to_close", "yang_zhang")
                else fn(o, h, lo, c)
            )
        )
        for name, fn in V.ESTIMATORS.items()
    }
    return OptionsRealisedVolResponse(
        symbol=symbol, as_of=as_of, window=window, estimators=estimators, cone=cone
    )
