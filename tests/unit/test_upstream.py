"""
api/upstream.py — TTL caching and yfinance normalisation.

No network. The gateway's only network seam is `_ticker`, which is
substituted below, so the real caching, de-duplication, ordering and
normalisation code runs while the suite stays offline.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from api.upstream import (
    TTLCache,
    UpstreamError,
    YFinanceGateway,
    normalise_news_item,
    normalise_profile,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ---------------------------------------------------------------------------
# TTLCache
# ---------------------------------------------------------------------------

def test_cache_returns_a_stored_value():
    cache = TTLCache(ttl=60, clock=FakeClock())
    cache.set("k", "v")
    assert cache.get("k") == "v"


def test_cache_expires_after_its_ttl():
    """
    The clock is injected precisely so this does not sleep — a test that
    waits out a 3600s profile TTL is a test nobody runs.
    """
    clock = FakeClock()
    cache = TTLCache(ttl=60, clock=clock)
    cache.set("k", "v")

    clock.advance(59)
    assert cache.get("k") == "v"

    clock.advance(2)
    assert cache.get("k") is None


def test_expired_entries_are_dropped_not_merely_hidden():
    clock = FakeClock()
    cache = TTLCache(ttl=60, clock=clock)
    cache.set("k", "v")
    clock.advance(61)
    cache.get("k")
    assert len(cache) == 0


def test_cache_miss_is_none():
    assert TTLCache(ttl=60).get("absent") is None


# ---------------------------------------------------------------------------
# News normalisation — the regression
# ---------------------------------------------------------------------------

NESTED_ITEM = {
    "id": "abc123",
    "content": {
        "id": "abc123",
        "title": "Apple announces something",
        "summary": "A summary.",
        "pubDate": "2026-08-09T16:04:46Z",
        "provider": {"displayName": "24/7 Wall St."},
        "canonicalUrl": {"url": "https://example.com/story"},
    },
}

FLAT_ITEM = {
    "uuid": "legacy-1",
    "title": "Older layout story",
    "link": "https://example.com/legacy",
    "publisher": "Reuters",
    "providerPublishTime": 1723219200,
}


def test_nested_yfinance_layout_is_read():
    """
    THE regression. yfinance 1.2.0 nests everything under `content`, but the
    Streamlit widget read the flat keys — so every item had title None and
    link None, and it rendered "[None](None)" from "Unknown" dated
    1970-01-01. Verified against live data: 10 items in, all with title None.
    """
    item = normalise_news_item(NESTED_ITEM)
    assert item["title"] == "Apple announces something"
    assert item["url"] == "https://example.com/story"
    assert item["publisher"] == "24/7 Wall St."
    assert item["published_at"] == datetime(2026, 8, 9, 16, 4, 46, tzinfo=timezone.utc)


def test_legacy_flat_layout_still_works():
    item = normalise_news_item(FLAT_ITEM)
    assert item["title"] == "Older layout story"
    assert item["url"] == "https://example.com/legacy"
    assert item["publisher"] == "Reuters"
    assert item["published_at"].tzinfo is not None


def test_click_through_url_is_used_when_canonical_is_absent():
    raw = {"content": {"title": "T", "clickThroughUrl": {"url": "https://x/y"}}}
    assert normalise_news_item(raw)["url"] == "https://x/y"


def test_missing_url_does_not_raise():
    assert normalise_news_item({"content": {"title": "T"}})["url"] is None


def test_unparseable_time_becomes_none_not_epoch_zero():
    """
    Epoch 0 would render as 1970-01-01 and look like a real timestamp — which
    is exactly what the widget showed.
    """
    raw = {"content": {"title": "T", "pubDate": "not a date"}}
    assert normalise_news_item(raw)["published_at"] is None


# ---------------------------------------------------------------------------
# Profile normalisation
# ---------------------------------------------------------------------------

def test_profile_selects_the_rendered_fields():
    profile = normalise_profile("aapl", {"symbol": "AAPL", "longName": "Apple Inc.", "sector": "Tech"})
    assert profile["symbol"] == "AAPL"
    assert profile["long_name"] == "Apple Inc."
    assert profile["sector"] == "Tech"


def test_missing_metrics_stay_none_rather_than_zero():
    """
    Streamlit defaulted these (`info.get('trailingPE', 0)`), so a company with
    no P/E displayed "0.00" — indistinguishable from a real zero.
    """
    profile = normalise_profile("AAPL", {"symbol": "AAPL"})
    assert profile["trailing_pe"] is None
    assert profile["market_cap"] is None


def test_profile_falls_back_to_the_requested_symbol():
    assert normalise_profile("msft", {"longName": "Microsoft"})["symbol"] == "MSFT"


# ---------------------------------------------------------------------------
# Gateway — caching and failure, with the network seam stubbed
# ---------------------------------------------------------------------------

class StubTicker:
    def __init__(self, symbol, calls, fail=False, news=None, info=None):
        self.symbol = symbol
        calls.append(symbol)
        self._fail = fail
        self._news = news
        self._info = info

    @property
    def news(self):
        if self._fail:
            raise RuntimeError("upstream down")
        return self._news if self._news is not None else [NESTED_ITEM]

    @property
    def info(self):
        if self._fail:
            raise RuntimeError("upstream down")
        return self._info if self._info is not None else {"symbol": self.symbol, "longName": "X"}

    @property
    def financials(self):
        return pd.DataFrame(
            {pd.Timestamp("2025-12-31"): [1.0, 2.0]}, index=["Revenue", "Net Income"]
        )

    balance_sheet = financials
    cashflow = financials
    quarterly_financials = financials
    quarterly_balance_sheet = financials
    quarterly_cashflow = financials


def gateway(**kwargs) -> tuple:
    """A gateway whose `_ticker` is stubbed; returns (gateway, calls)."""
    calls: list = []

    class Stubbed(YFinanceGateway):
        def _ticker(self, symbol):
            return StubTicker(symbol, calls, **kwargs)

    return Stubbed(clock=FakeClock()), calls


def test_profile_is_cached():
    gw, calls = gateway()
    gw.fetch_profile("AAPL")
    gw.fetch_profile("AAPL")
    assert calls == ["AAPL"], "second call must be served from cache"


def test_profile_refetches_after_the_ttl():
    clock = FakeClock()
    calls: list = []

    class Stubbed(YFinanceGateway):
        def _ticker(self, symbol):
            return StubTicker(symbol, calls)

    gw = Stubbed(profile_ttl=3600, clock=clock)
    gw.fetch_profile("AAPL")
    clock.advance(3601)
    gw.fetch_profile("AAPL")
    assert calls == ["AAPL", "AAPL"]


def test_unknown_symbol_raises_upstream_error():
    """
    yfinance returns a sparse dict rather than raising for a bad ticker, so
    "did this resolve?" has to be asked explicitly.
    """
    gw, _ = gateway(info={})
    with pytest.raises(UpstreamError):
        gw.fetch_profile("NOSUCH")


def test_upstream_exception_becomes_upstream_error():
    gw, _ = gateway(fail=True)
    with pytest.raises(UpstreamError):
        gw.fetch_profile("AAPL")


def test_news_deduplicates_across_symbols():
    """
    Two tickers reporting the same story yield one item. The widget keyed on
    `link`, absent in yfinance 1.x, so every story collapsed onto None.
    """
    gw, _ = gateway()
    items = gw.fetch_news(["AAPL", "MSFT"])
    assert len(items) == 1


def test_news_is_newest_first():
    older = {"content": {"id": "1", "title": "Older", "pubDate": "2026-01-01T00:00:00Z",
                         "canonicalUrl": {"url": "https://x/1"}}}
    newer = {"content": {"id": "2", "title": "Newer", "pubDate": "2026-08-01T00:00:00Z",
                         "canonicalUrl": {"url": "https://x/2"}}}
    gw, _ = gateway(news=[older, newer])
    assert [i["title"] for i in gw.fetch_news(["AAPL"])] == ["Newer", "Older"]


def test_news_is_cached_per_symbol():
    gw, calls = gateway()
    gw.fetch_news(["AAPL"])
    gw.fetch_news(["AAPL", "MSFT"])
    assert calls == ["AAPL", "MSFT"], "AAPL must not be refetched"


def test_one_failing_ticker_does_not_blank_the_feed():
    """A single delisted symbol in a watchlist must not empty the whole feed."""
    calls: list = []

    class Stubbed(YFinanceGateway):
        def _ticker(self, symbol):
            return StubTicker(symbol, calls, fail=(symbol == "BAD"))

    gw = Stubbed(clock=FakeClock())
    assert len(gw.fetch_news(["AAPL", "BAD"])) == 1


def test_all_tickers_failing_raises():
    """That case genuinely means the provider is unreachable."""
    gw, _ = gateway(fail=True)
    with pytest.raises(UpstreamError):
        gw.fetch_news(["AAPL", "MSFT"])


def test_items_without_a_title_are_dropped():
    gw, _ = gateway(news=[{"content": {"id": "1", "pubDate": "2026-01-01T00:00:00Z"}}])
    assert gw.fetch_news(["AAPL"]) == []


def test_no_symbols_means_no_calls():
    gw, calls = gateway()
    assert gw.fetch_news([]) == []
    assert calls == []


def test_financials_become_json_safe_rows():
    gw, _ = gateway()
    statements = gw.fetch_financials("AAPL")
    income = statements["income_statement"]
    assert {row["line_item"] for row in income} == {"Revenue", "Net Income"}
    assert list(income[0]["values"]) == ["2025-12-31"]


def test_financials_cache_separates_annual_from_quarterly():
    gw, calls = gateway()
    gw.fetch_financials("AAPL", quarterly=False)
    gw.fetch_financials("AAPL", quarterly=True)
    assert len(calls) == 2, "quarterly must not be served the annual cache entry"
