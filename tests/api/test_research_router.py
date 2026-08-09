"""
/api/v1/research — profile, financials, news.

The only router that reaches the network. Its gateway is stubbed at the
`_ticker` seam (see StubGateway in conftest.py), so the caching,
de-duplication, ordering and normalisation code all runs for real while these
tests stay offline — `pytest tests/` needs no Docker AND no network.

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

BASE = "/api/v1/research"


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------

def test_returns_a_company_profile(client: TestClient):
    body = client.get(f"{BASE}/AAPL/profile").json()
    assert body["symbol"] == "AAPL"
    assert body["long_name"] == "AAPL Inc."
    assert body["sector"] == "Information Technology"


def test_absent_metrics_are_null_not_zero(client: TestClient):
    """
    Streamlit defaulted these to 0 (`info.get('trailingPE', 0)`), so a company
    with no P/E displayed "0.00" — indistinguishable from a real zero.
    """
    body = client.get(f"{BASE}/AAPL/profile").json()
    assert body["trailing_pe"] is None
    assert body["dividend_yield"] is None


def test_unknown_symbol_is_503_with_the_symbol_named(client: TestClient):
    """
    yfinance returns a sparse dict rather than raising for a bad ticker, so
    this has to be detected rather than passed through as an empty profile.
    """
    response = client.get(f"{BASE}/NOSUCH/profile")
    assert response.status_code == 503
    assert "NOSUCH" in response.json()["detail"]


def test_upstream_failure_is_503_not_500(client: TestClient):
    """An upstream outage is not our bug, and must not read as one."""
    response = client.get(f"{BASE}/BROKEN/profile")
    assert response.status_code == 503


# ---------------------------------------------------------------------------
# Financials
# ---------------------------------------------------------------------------

def test_returns_three_statements(client: TestClient):
    body = client.get(f"{BASE}/AAPL/financials").json()
    assert {r["line_item"] for r in body["income_statement"]} == {
        "Total Revenue",
        "Net Income",
    }
    assert body["balance_sheet"] and body["cash_flow"]
    assert body["quarterly"] is False


def test_quarterly_is_requestable(client: TestClient):
    body = client.get(f"{BASE}/AAPL/financials", params={"quarterly": True}).json()
    assert body["quarterly"] is True


def test_statement_periods_are_iso_dates(client: TestClient):
    body = client.get(f"{BASE}/AAPL/financials").json()
    assert list(body["income_statement"][0]["values"]) == ["2025-12-31"]


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

def test_news_items_carry_a_real_title_and_url(client: TestClient):
    """
    THE regression. yfinance 1.2.0 nests items under `content`, but the
    Streamlit widget read the flat keys — every story rendered as
    "[None](None)" from "Unknown", dated 1970-01-01.
    """
    items = client.get(f"{BASE}/news", params={"symbols": ["AAPL"]}).json()["items"]
    assert items
    assert all(i["title"] for i in items)
    assert all(i["url"] for i in items)
    assert all(i["publisher"] for i in items)
    assert all(i["published_at"] for i in items)


def test_news_deduplicates_a_story_shared_by_two_symbols(client: TestClient):
    """
    The widget deduped on `link`, which yfinance 1.x does not emit, so ten
    stories collapsed onto the single key None — verified against live data.
    """
    items = client.get(
        f"{BASE}/news", params={"symbols": ["AAPL", "MSFT"]}
    ).json()["items"]
    titles = [i["title"] for i in items]
    assert titles.count("Tech selloff") == 1
    assert len(titles) == 2


def test_news_is_newest_first(client: TestClient):
    items = client.get(f"{BASE}/news", params={"symbols": ["AAPL"]}).json()["items"]
    stamps = [i["published_at"] for i in items]
    assert stamps == sorted(stamps, reverse=True)


def test_limit_truncates_the_feed(client: TestClient):
    body = client.get(
        f"{BASE}/news", params={"symbols": ["AAPL", "MSFT"], "limit": 1}
    ).json()
    assert len(body["items"]) == 1


def test_default_source_is_the_market_proxies(client: TestClient):
    body = client.get(f"{BASE}/news").json()
    assert body["source"] == "market"
    assert body["symbols"] == ["SPY", "QQQ", "DIA", "BTC-USD"]


def test_news_for_a_watchlist(client: TestClient):
    body = client.get(f"{BASE}/news", params={"watchlist": "MAG7"}).json()
    assert body["source"] == "watchlist:MAG7"
    assert body["symbols"] == ["AAPL", "MSFT", "NVDA"]


def test_news_for_a_portfolio_uses_its_open_positions(client: TestClient):
    """
    Open positions, not every ticker ever traded — news about a closed
    position is not what "my portfolio's news" means.
    """
    body = client.get(f"{BASE}/news", params={"portfolio": "Growth"}).json()
    assert body["source"] == "portfolio:Growth"
    assert set(body["symbols"]) == {"AAPL", "MSFT"}


def test_portfolio_news_reads_the_database_not_the_legacy_json(client: TestClient):
    """
    Portfolios moved to the database in 0003 and watchlists in 0004. The
    Streamlit widget was handed the JSON dicts; reading those here would ship
    a feed quietly tracking state nothing else uses.
    """
    client.post(
        "/api/v1/portfolios/Growth/trades",
        json={"ticker": "NVDA", "action": "BUY", "quantity": 1, "price": 10.0},
    )
    body = client.get(f"{BASE}/news", params={"portfolio": "Growth"}).json()
    assert "NVDA" in body["symbols"]


def test_a_portfolio_with_no_open_positions_is_empty_not_the_market(client: TestClient):
    body = client.get(f"{BASE}/news", params={"portfolio": "Empty"}).json()
    assert body["symbols"] == []
    assert body["items"] == []
    assert body["source"] == "portfolio:Empty"


def test_explicit_symbols_win_over_a_watchlist(client: TestClient):
    body = client.get(
        f"{BASE}/news", params={"symbols": ["TSLA"], "watchlist": "MAG7"}
    ).json()
    assert body["symbols"] == ["TSLA"]
    assert body["source"] == "symbols"


def test_symbols_beyond_the_cap_are_reported_not_silently_dropped(
    client: TestClient,
):
    """
    A long watchlist must not look fully covered when only the first ten were
    fetched.
    """
    many = [f"T{i}" for i in range(15)]
    client.put("/api/v1/watchlists/Big", json={"symbols": many})
    body = client.get(f"{BASE}/news", params={"watchlist": "Big"}).json()
    assert len(body["symbols"]) == 10
    assert body["truncated_symbols"] == [s.upper() for s in many[10:]]


def test_unknown_watchlist_is_404(client: TestClient):
    assert client.get(f"{BASE}/news", params={"watchlist": "Nope"}).status_code == 404


def test_unknown_portfolio_is_404(client: TestClient):
    assert client.get(f"{BASE}/news", params={"portfolio": "Nope"}).status_code == 404


def test_total_upstream_failure_is_503_not_an_empty_feed(client: TestClient):
    """
    An empty list is indistinguishable from "no news", which is a real answer.
    """
    response = client.get(f"{BASE}/news", params={"symbols": ["BROKEN"]})
    assert response.status_code == 503


def test_one_broken_symbol_does_not_blank_the_feed(client: TestClient):
    """A single delisted ticker in a watchlist must not empty the whole feed."""
    body = client.get(
        f"{BASE}/news", params={"symbols": ["AAPL", "BROKEN"]}
    ).json()
    assert body["items"]


def test_a_symbol_with_no_stories_is_an_empty_feed_not_an_error(client: TestClient):
    body = client.get(f"{BASE}/news", params={"symbols": ["QUIET"]})
    assert body.status_code == 200
    assert body.json()["items"] == []
