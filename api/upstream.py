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
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

NEWS_TTL_SECONDS = 600
PROFILE_TTL_SECONDS = 3600
MAX_PARALLEL_FETCHES = 5


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

def _first_url(*candidates: Any) -> Optional[str]:
    for candidate in candidates:
        if isinstance(candidate, dict):
            url = candidate.get("url")
            if url:
                return url
        elif isinstance(candidate, str) and candidate:
            return candidate
    return None


def normalise_news_item(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    One yfinance news item to a stable shape.

    HANDLES BOTH LAYOUTS. yfinance >= 1.x nests everything under `content`
    ({"id": ..., "content": {"title", "pubDate", "provider", "canonicalUrl"}}),
    while older releases returned a flat dict with title/link/publisher/
    providerPublishTime.

    The Streamlit widget only ever read the flat keys. Against yfinance 1.2.0
    that meant every item had title None and link None — so its dedupe on
    `link` collapsed ten stories into one and rendered it as "[None](None)"
    from "Unknown", dated 1970-01-01. Reading both layouts is why this
    function exists.
    """
    if not isinstance(raw, dict):
        return None

    content = raw.get("content")
    if isinstance(content, dict):
        provider = content.get("provider") or {}
        published = content.get("pubDate") or content.get("displayTime")
        return {
            "id": str(content.get("id") or raw.get("id") or ""),
            "title": content.get("title"),
            "summary": content.get("summary") or content.get("description"),
            "url": _first_url(
                content.get("canonicalUrl"), content.get("clickThroughUrl")
            ),
            "publisher": provider.get("displayName") if isinstance(provider, dict) else None,
            "published_at": _parse_time(published),
        }

    # Legacy flat layout.
    return {
        "id": str(raw.get("uuid") or raw.get("id") or ""),
        "title": raw.get("title"),
        "summary": raw.get("summary"),
        "url": raw.get("link"),
        "publisher": raw.get("publisher"),
        "published_at": _parse_time(raw.get("providerPublishTime")),
    }


def _parse_time(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # Epoch seconds, the legacy layout's providerPublishTime.
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


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
            key = (
                period.date().isoformat()
                if hasattr(period, "date")
                else str(period)
            )
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
            raise UpstreamError(
                f"Could not fetch financials for {symbol!r}: {exc}"
            ) from exc

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
        """
        News for several tickers, newest first, de-duplicated.

        A ticker that fails contributes nothing rather than failing the batch:
        one delisted symbol in a watchlist should not blank the whole feed.
        UpstreamError is raised only when EVERY ticker failed, which is the
        case that actually means the provider is unreachable.
        """
        wanted = [s.upper() for s in symbols if s and s.strip()]
        if not wanted:
            return []

        pending = [s for s in wanted if self.news_cache.get(s) is None]
        failures = 0

        if pending:
            def fetch_one(symbol: str):
                try:
                    return symbol, self._ticker(symbol).news or []
                except Exception as exc:  # noqa: BLE001
                    logger.warning("News fetch failed for %s: %s", symbol, exc)
                    return symbol, None

            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as pool:
                for symbol, raw in pool.map(fetch_one, pending):
                    if raw is None:
                        failures += 1
                        continue
                    items = [normalise_news_item(item) for item in raw]
                    self.news_cache.set(symbol, [i for i in items if i and i.get("title")])

            if failures == len(pending):
                raise UpstreamError(
                    "Could not fetch news for any of: " + ", ".join(pending)
                )

        merged: Dict[str, Dict[str, Any]] = {}
        for symbol in wanted:
            for item in self.news_cache.get(symbol) or []:
                # De-duplicate on URL, falling back to id. The Streamlit widget
                # keyed on `link`, which yfinance 1.x does not emit, so every
                # item collapsed onto the single key None.
                key = item.get("url") or item.get("id") or item.get("title")
                if key and key not in merged:
                    merged[key] = {**item, "symbol": symbol}

        return sorted(
            merged.values(),
            key=lambda i: i.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )


_gateway = YFinanceGateway()


def get_upstream() -> YFinanceGateway:
    """FastAPI dependency. Overridden in tests so the suite stays offline."""
    return _gateway
