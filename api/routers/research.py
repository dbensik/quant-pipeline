"""
api/routers/research.py
Company profiles, financial statements and news — the "Asset Deep Dive" and
"Market Intelligence" features.

THE ONLY ROUTER THAT REACHES THE NETWORK, and it does so through
api/upstream.py. Profiles and news have no database source, so they are
proxied from yfinance with explicit TTL caching. Financial statements come
from the stored SEC filings (since 2026-10-12, core/research_financials.py),
the same point-in-time figures the Valuation page uses; yfinance only for a
symbol with no usable filings (ETFs, crypto, IFRS filers), labelled `source`. Everything else,
including the deep dive's PRICE HISTORY, comes from TimescaleDB: use
/api/v1/ohlcv for the chart. Streamlit pulled 5 years of history from
yfinance here as well, which meant the deep dive could disagree with every
other view in the app about what a price was.

Upstream failures are 503 with the symbol named — never a 500 (this is not
our bug) and never a silent empty list (indistinguishable from "no news",
which is a real answer).

Phase 5 — decommissioning Streamlit
"""

import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from api.dependencies import get_portfolio_repo, get_watchlist_repo
from api.routers.valuation import get_valuation_data
from api.upstream import UpstreamError, YFinanceGateway, get_upstream
from core import research_financials
from core import valuation_service as svc
from core.portfolio import derive_state
from db.repositories.portfolios import TimescalePortfolioRepo
from db.repositories.watchlists import TimescaleWatchlistRepo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/research", tags=["research"])

# Proxies for "the market" when no portfolio or watchlist is named — the same
# four the Streamlit widget used.
MARKET_PROXIES = ["SPY", "QQQ", "DIA", "BTC-USD"]
MAX_NEWS_SYMBOLS = 10


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Profile(BaseModel):
    symbol: str
    long_name: Optional[str] = None
    short_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    full_time_employees: Optional[int] = None
    business_summary: Optional[str] = None
    market_cap: Optional[float] = None
    trailing_pe: Optional[float] = None
    forward_pe: Optional[float] = None
    dividend_yield: Optional[float] = None
    website: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None


class StatementLine(BaseModel):
    line_item: str
    values: Dict[str, Optional[float]] = Field(description="Period end date (ISO) to value")
    filed: Dict[str, Optional[date]] = Field(
        default_factory=dict, description="Period end to the date the figure was filed (SEC only)"
    )
    concept: Dict[str, Optional[str]] = Field(
        default_factory=dict, description="Period end to the XBRL concept read (SEC only)"
    )
    derived: Dict[str, bool] = Field(
        default_factory=dict,
        description="Period end to whether the figure is derived (a Q4 from the annual, a quarter "
        "from year-to-date cash flows), not filed as such",
    )


class Financials(BaseModel):
    symbol: str
    quarterly: bool
    source: str = Field(default="", description="Where the statements came from")
    income_statement: List[StatementLine]
    balance_sheet: List[StatementLine]
    cash_flow: List[StatementLine]


class NewsItem(BaseModel):
    id: str
    symbol: str
    title: str
    url: Optional[str] = None
    publisher: Optional[str] = None
    summary: Optional[str] = None
    published_at: Optional[datetime] = None


class NewsFeed(BaseModel):
    symbols: List[str] = Field(description="Tickers the feed was built from")
    source: str = Field(description="market | portfolio:<name> | watchlist:<name> | symbols")
    items: List[NewsItem]
    truncated_symbols: List[str] = Field(
        default_factory=list,
        description=(
            "Tickers dropped by the per-request cap. Reported rather than "
            "silently trimmed, so a long watchlist does not look fully covered."
        ),
    )


# ---------------------------------------------------------------------------
# Profile & financials
# ---------------------------------------------------------------------------


@router.get(
    "/{symbol}/profile",
    response_model=Profile,
    summary="Company profile",
    responses={503: {"description": "Upstream provider unavailable or unknown symbol"}},
)
async def get_profile(
    symbol: str,
    upstream: YFinanceGateway = Depends(get_upstream),
) -> Profile:
    try:
        profile = await run_in_threadpool(upstream.fetch_profile, symbol)
    except UpstreamError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    return Profile(**profile)


@router.get(
    "/{symbol}/financials",
    response_model=Financials,
    summary="Income statement, balance sheet and cash flow",
    responses={503: {"description": "Upstream provider unavailable"}},
)
async def get_financials(
    symbol: str,
    quarterly: bool = Query(default=False, description="Quarterly instead of annual"),
    upstream: YFinanceGateway = Depends(get_upstream),
    data: svc.ValuationData = Depends(get_valuation_data),
) -> Financials:
    as_of = datetime.now(timezone.utc).date()
    reason = ""
    try:
        st, *_ = await svc.filer_statements(data, symbol.upper())
        reason = research_financials.unusable_reason(st, as_of, quarterly)
        if not reason:
            return Financials(**research_financials.build(st, symbol, as_of, quarterly))
    except svc.ValuationError as exc:
        reason = exc.reason
    try:
        statements = await run_in_threadpool(upstream.fetch_financials, symbol, quarterly)
    except UpstreamError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    return Financials(
        **statements, source=f"yfinance, restated, no filing dates: no SEC statements ({reason})"
    )


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------


async def _resolve_symbols(
    symbols: Optional[List[str]],
    portfolio: Optional[str],
    watchlist: Optional[str],
    portfolios: TimescalePortfolioRepo,
    watchlists: TimescaleWatchlistRepo,
) -> tuple[List[str], str]:
    """
    Which tickers the feed covers.

    Portfolios and watchlists are read from the DATABASE, not from the legacy
    JSON files the Streamlit widget was handed. Both moved in 0003 and 0004;
    reading the files here would have shipped a feed that quietly tracked
    state nothing else uses.
    """
    if symbols:
        return [s.upper() for s in symbols], "symbols"

    if portfolio:
        found = await portfolios.get_portfolio(portfolio)
        if found is None:
            raise HTTPException(status_code=404, detail=f"No portfolio named {portfolio!r}.")
        # Open positions, not every ticker ever traded — news about a closed
        # position is not what "my portfolio's news" means.
        state = derive_state(found.trades, found.initial_cash)
        return [p.ticker for p in state.positions], f"portfolio:{portfolio}"

    if watchlist:
        found = await watchlists.get_watchlist(watchlist)
        if found is None:
            raise HTTPException(status_code=404, detail=f"No watchlist named {watchlist!r}.")
        return list(found.symbols), f"watchlist:{watchlist}"

    return list(MARKET_PROXIES), "market"


@router.get(
    "/news",
    response_model=NewsFeed,
    summary="News for the market, a portfolio, a watchlist, or given tickers",
    responses={
        404: {"description": "Unknown portfolio or watchlist"},
        503: {"description": "Upstream provider unavailable"},
    },
)
async def get_news(
    symbols: Optional[List[str]] = Query(default=None, description="Explicit tickers"),
    portfolio: Optional[str] = Query(default=None, description="Open positions of this portfolio"),
    watchlist: Optional[str] = Query(default=None, description="Members of this watchlist"),
    limit: int = Query(default=50, gt=0, le=200),
    upstream: YFinanceGateway = Depends(get_upstream),
    portfolios: TimescalePortfolioRepo = Depends(get_portfolio_repo),
    watchlists: TimescaleWatchlistRepo = Depends(get_watchlist_repo),
) -> NewsFeed:
    """
    Sources are mutually exclusive and resolved in order: explicit `symbols`,
    then `portfolio`, then `watchlist`, then market proxies.
    """
    resolved, source = await _resolve_symbols(symbols, portfolio, watchlist, portfolios, watchlists)

    covered = resolved[:MAX_NEWS_SYMBOLS]
    truncated = resolved[MAX_NEWS_SYMBOLS:]

    if not covered:
        # A portfolio with no open positions is a legitimate empty answer,
        # not an error and not the market feed.
        return NewsFeed(symbols=[], source=source, items=[], truncated_symbols=[])

    try:
        items = await run_in_threadpool(upstream.fetch_news, covered)
    except UpstreamError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None

    return NewsFeed(
        symbols=covered,
        source=source,
        items=[NewsItem(**item) for item in items[:limit]],
        truncated_symbols=truncated,
    )
