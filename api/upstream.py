"""
api/upstream.py
The ONLY module in the API that reaches the network.

Everything else reads TimescaleDB. Company profiles, financial statements and
news have no database source, so they are proxied from yfinance — but the
boundary is kept explicit and narrow so position valuation, backtests and
screens can never quietly acquire a network dependency. `api/routers/
portfolios.py` says outright that prices come from the migrated database and
not from here; that stays true.

CACHING. Streamlit got this free from `@st.cache_data(ttl=...)`. An API does
not, and without it every page render would hit Yahoo — so the TTLs are
reproduced explicitly: 600s for news, 3600s for profiles and financials,
matching the decorators being replaced.

FAILURE. An upstream problem raises UpstreamError, which routers turn into a
503 naming the symbol. Never a 500 (this is not our bug) and never a silent
empty list (indistinguishable from "no news", which is a real answer).

NEWS COMES FROM YAHOO'S SEARCH, not `Ticker.news` (decision 57, 2026-10-22,
`research/news-source-2026-10-22.md`). In yfinance 1.2.0 `Ticker.news` POSTs
to /xhr/ncp, which has answered 404 since some time before 2026-10-21;
yfinance parses the 404 body and returns [] without a word, so every feed
read "no news" and was cached that way. A search response always carries a
`news` key, even when it is empty; one without it is a failure, never an
empty answer.

The gateway is injected as a FastAPI dependency so tests can substitute a stub.
`pytest tests/` must need no Docker AND no network; a router suite that made
live Yahoo calls would be flaky and slow exactly when it matters.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from re import Pattern
from typing import Any, Callable, Dict, List, Optional, Tuple

from config.settings import NEWS_COPY_WINDOW_HOURS
from core.news import normalise_news_item
from core.news_archive import names_company

logger = logging.getLogger(__name__)

NEWS_TTL_SECONDS = 600
NEWS_PER_SYMBOL = 10
PROFILE_TTL_SECONDS = 3600
MAX_PARALLEL_FETCHES = 5


def collapse_copies(
    items: List[Dict[str, Any]], window_hours: float = NEWS_COPY_WINDOW_HOURS
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    One row per headline published again within `window_hours`, and how many
    copies each symbol lost (decision 78 B,
    research/news-duplicate-stories-2026-10-26.md).

    Yahoo serves one release on several wires under several story ids and
    URLs (Rockwell's report on CNW Group and twice on PR Newswire), so the
    URL key above never sees them as one. A copy is the same title published
    within the window of the earliest kept row with that title; the earliest
    is kept, with its own tags and publisher. A title recurring a day apart
    is a new story. A row without a publish time is never collapsed. Counted
    under the symbol the dropped copy was found under, never silently.
    """
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    window = timedelta(hours=window_hours)
    kept: List[Dict[str, Any]] = []
    shown: Dict[str, List[datetime]] = {}
    collapsed: Dict[str, int] = {}
    for item in sorted(items, key=lambda i: i.get("published_at") or epoch):
        title, published = (item.get("title") or "").strip(), item.get("published_at")
        if title and published is not None:
            if any(published - t <= window for t in shown.get(title, [])):
                symbol = item.get("symbol") or ""
                collapsed[symbol] = collapsed.get(symbol, 0) + 1
                continue
            shown.setdefault(title, []).append(published)
        kept.append(item)
    return kept, collapsed


class UpstreamError(RuntimeError):
    """The upstream provider failed or returned nothing usable."""


class TTLCache:
    """
    Minimal time-to-live cache.

    `clock` is injectable so expiry can be tested without sleeping — a test
    that sleeps for a 3600s TTL is not a test anyone runs.
    """

    def __init__(self, ttl: float, clock: Callable[[], float] = time.monotonic):
        self.ttl = ttl
        self._clock = clock
        self._entries: Dict[Any, Tuple[float, Any]] = {}
        # Entries are written from threadpool workers, so the dict is guarded.
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            stored_at, value = entry
            if self._clock() - stored_at >= self.ttl:
                del self._entries[key]
                return None
            return value

    def set(self, key: Any, value: Any) -> None:
        with self._lock:
            self._entries[key] = (self._clock(), value)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


PROFILE_FIELDS = {
    "longName": "long_name",
    "shortName": "short_name",
    "sector": "sector",
    "industry": "industry",
    "fullTimeEmployees": "full_time_employees",
    "longBusinessSummary": "business_summary",
    "marketCap": "market_cap",
    "trailingPE": "trailing_pe",
    "forwardPE": "forward_pe",
    "dividendYield": "dividend_yield",
    "website": "website",
    "country": "country",
    "currency": "currency",
}


def normalise_profile(symbol: str, info: Dict[str, Any]) -> Dict[str, Any]:
    """
    `.info` to the fields the deep dive actually renders.

    Selected rather than passed through whole: `.info` carries ~150 keys that
    change without warning between yfinance releases, and a response shaped by
    whatever Yahoo returned today cannot be typed for the frontend.

    Missing values stay None rather than becoming 0. Streamlit defaulted them
    (`info.get('trailingPE', 0)`), so a company with no P/E displayed "0.00" —
    indistinguishable from a real zero.
    """
    profile: Dict[str, Any] = {"symbol": info.get("symbol") or symbol.upper()}
    for source, target in PROFILE_FIELDS.items():
        profile[target] = info.get(source)
    return profile


def _frame_to_statement(frame: Any) -> List[Dict[str, Any]]:
    """A yfinance statement DataFrame to JSON-safe rows (periods as columns)."""
    import pandas as pd

    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for line_item, series in frame.iterrows():
        values: Dict[str, Any] = {}
        for period, value in series.items():
            key = period.date().isoformat() if hasattr(period, "date") else str(period)
            values[key] = None if pd.isna(value) else float(value)
        rows.append({"line_item": str(line_item), "values": values})
    return rows


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------


class YFinanceGateway:
    """Cached, normalised access to yfinance. The network boundary."""

    def __init__(
        self,
        news_ttl: float = NEWS_TTL_SECONDS,
        profile_ttl: float = PROFILE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.news_cache = TTLCache(news_ttl, clock)
        self.profile_cache = TTLCache(profile_ttl, clock)
        self.financials_cache = TTLCache(profile_ttl, clock)

    # -- overridable seam ------------------------------------------------
    def _ticker(self, symbol: str):
        import yfinance as yf

        return yf.Ticker(symbol)

    def _search(self, symbol: str):
        import yfinance as yf

        return yf.Search(symbol, news_count=NEWS_PER_SYMBOL, max_results=1)

    def _news_for(self, symbol: str) -> List[Dict[str, Any]]:
        """One symbol's raw stories; raises when the response is not a search answer."""
        response = self._search(symbol).response
        if not isinstance(response, dict) or "news" not in response:
            raise UpstreamError(f"Yahoo's search returned no news field for {symbol!r}.")
        return response["news"] or []

    # -- profile ---------------------------------------------------------
    def fetch_profile(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper()
        cached = self.profile_cache.get(symbol)
        if cached is not None:
            return cached

        try:
            info = self._ticker(symbol).info
        except Exception as exc:  # noqa: BLE001 — any upstream failure
            raise UpstreamError(f"Could not fetch a profile for {symbol!r}: {exc}") from exc

        # yfinance returns a sparse dict rather than raising for an unknown
        # ticker, so "did this resolve?" has to be asked explicitly.
        if not info or not (info.get("symbol") or info.get("longName")):
            raise UpstreamError(
                f"No profile available for {symbol!r} — it may be delisted or invalid."
            )

        profile = normalise_profile(symbol, info)
        self.profile_cache.set(symbol, profile)
        return profile

    # -- financials ------------------------------------------------------
    def fetch_financials(self, symbol: str, quarterly: bool = False) -> Dict[str, Any]:
        symbol = symbol.upper()
        key = (symbol, quarterly)
        cached = self.financials_cache.get(key)
        if cached is not None:
            return cached

        try:
            ticker = self._ticker(symbol)
            if quarterly:
                income, balance, cashflow = (
                    ticker.quarterly_financials,
                    ticker.quarterly_balance_sheet,
                    ticker.quarterly_cashflow,
                )
            else:
                income, balance, cashflow = (
                    ticker.financials,
                    ticker.balance_sheet,
                    ticker.cashflow,
                )
        except Exception as exc:  # noqa: BLE001
            raise UpstreamError(f"Could not fetch financials for {symbol!r}: {exc}") from exc

        statements = {
            "symbol": symbol,
            "quarterly": quarterly,
            "income_statement": _frame_to_statement(income),
            "balance_sheet": _frame_to_statement(balance),
            "cash_flow": _frame_to_statement(cashflow),
        }
        self.financials_cache.set(key, statements)
        return statements

    # -- news ------------------------------------------------------------
    def fetch_news(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Every story for several tickers, unfiltered: `fetch_related_news` with no aliases."""
        return self.fetch_related_news(symbols)[0]

    def fetch_related_news(
        self,
        symbols: List[str],
        aliases: Optional[Dict[str, List[str]]] = None,
        terms: Optional[Dict[str, List[str]]] = None,
        names: Optional[Dict[str, List[Pattern]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int], Dict[str, int], Dict[str, int]]:
        """
        News for several tickers, newest first, de-duplicated, how many
        stories each ticker lost to the relevance rule, how many it kept
        by name alone and how many copies of one headline it lost.

        TERMS (2026-10-22, research/news-fallbacks-2026-10-22.md). With
        `terms` (symbol -> what to search), a symbol's stories are every
        story its terms return: a coin by its pair, base symbol and name
        (the pair alone answered for 1 of 20 large coins), a word ticker by
        its ticker and its NEWS_SEARCH_TERMS entry. A symbol absent from
        `terms` is searched by itself. Each term is cached on its own.

        RELEVANCE (2026-10-22, research/news-source-2026-10-22.md). With
        `aliases` (symbol -> its other tickers: share classes, a coin's
        provider ticker), a story found under a symbol is kept only when
        Yahoo tags it with that symbol or one of its aliases. A symbol absent
        from `aliases` keeps everything. The rule runs per symbol BEFORE
        de-duplication across symbols, so a story found under two symbols
        survives if it is tagged with either; filtering the merged list would
        test it only against whichever symbol came first. A story two of one
        symbol's terms found is judged, and counted, once. The cache stays
        unfiltered.

        NAMES (decision 77, research/headline-tagging-2026-10-26.md). With
        `names` (symbol -> core.news_archive.name_patterns), a story Yahoo
        left with NO tags at all is kept, and counted in the third dict,
        when its title names the symbol's company. Found by the symbol's own
        search and untagged, such stories were about the company 70 times in
        86 (PDD's earnings, Trane's acquisition); a story tagged with
        anyone else is never rescued. It keeps its empty tags.

        COPIES (decision 78 B). After relevance, a headline published again
        within NEWS_COPY_WINDOW_HOURS under another id and URL is shown once
        (`collapse_copies`), counted in the fourth dict.

        A ticker that fails contributes nothing rather than failing the batch:
        one delisted symbol in a watchlist should not blank the whole feed. A
        symbol fails only when EVERY one of its terms failed, and
        UpstreamError is raised only when every symbol failed, which is the
        case that actually means the provider is unreachable. A term answered
        from the cache has not failed.

        An empty answer is returned but NOT cached: measured 2026-10-22, QQQ
        came back with no stories and then ten a minute later, and a cached
        empty would have shown "no news" for ten minutes.
        """
        wanted = [s.upper() for s in symbols if s and s.strip()]
        if not wanted:
            return [], {}, {}, {}
        searched = {s: list(dict.fromkeys((terms or {}).get(s) or [s])) for s in wanted}

        pending = list(
            dict.fromkeys(t for s in wanted for t in searched[s] if self.news_cache.get(t) is None)
        )
        failed: set = set()

        if pending:

            def fetch_one(term: str):
                try:
                    return term, self._news_for(term)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("News fetch failed for %s: %s", term, exc)
                    return term, None

            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as pool:
                for term, raw in pool.map(fetch_one, pending):
                    if raw is None:
                        failed.add(term)
                        continue
                    items = [normalise_news_item(item) for item in raw]
                    items = [i for i in items if i and i.get("title")]
                    if items:
                        self.news_cache.set(term, items)

            dead = [s for s in wanted if all(t in failed for t in searched[s])]
            if len(dead) == len(wanted):
                raise UpstreamError("Could not fetch news for any of: " + ", ".join(wanted))

        merged: Dict[str, Dict[str, Any]] = {}
        dropped: Dict[str, int] = {}
        rescued: Dict[str, int] = {}
        for symbol in wanted:
            own = None if aliases is None or symbol not in aliases else {symbol, *aliases[symbol]}
            patterns = (names or {}).get(symbol) or []
            seen: set = set()
            for term in searched[symbol]:
                for item in self.news_cache.get(term) or []:
                    # De-duplicate on URL, falling back to id. The Streamlit
                    # widget keyed on `link`, which yfinance 1.x does not
                    # emit, so every item collapsed onto the single key None.
                    key = item.get("url") or item.get("id") or item.get("title")
                    if key in seen:
                        continue
                    seen.add(key)
                    tags = item.get("related_tickers") or []
                    if own is not None and own.isdisjoint(tags):
                        if tags or not names_company(item.get("title") or "", patterns):
                            dropped[symbol] = dropped.get(symbol, 0) + 1
                            continue
                        rescued[symbol] = rescued.get(symbol, 0) + 1
                    if key and key not in merged:
                        merged[key] = {**item, "symbol": symbol}

        shown, collapsed = collapse_copies(list(merged.values()))
        items = sorted(
            shown,
            key=lambda i: i.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return items, dropped, rescued, collapsed


_gateway = YFinanceGateway()


def get_upstream() -> YFinanceGateway:
    """FastAPI dependency. Overridden in tests so the suite stays offline."""
    return _gateway
