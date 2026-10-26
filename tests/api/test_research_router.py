"""
/api/v1/research — profile, financials, news.

The only router that reaches the network. Its gateway is stubbed at the
`_ticker` and `_search` seams (see StubGateway in conftest.py), so the caching,
de-duplication, ordering and normalisation code all runs for real while these
tests stay offline — `pytest tests/` needs no Docker AND no network.

Phase 5 — decommissioning Streamlit
"""

import pytest
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
    "[None](None)" from "Unknown", dated 1970-01-01. Yahoo's search, the
    source since decision 57, returns the flat layout again; both layouts are
    pinned in tests/unit/test_upstream.py.
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
    items = client.get(f"{BASE}/news", params={"symbols": ["AAPL", "MSFT"]}).json()["items"]
    titles = [i["title"] for i in items]
    assert titles.count("Tech selloff") == 1
    assert len(titles) == 2


def test_news_is_newest_first(client: TestClient):
    items = client.get(f"{BASE}/news", params={"symbols": ["AAPL"]}).json()["items"]
    stamps = [i["published_at"] for i in items]
    assert stamps == sorted(stamps, reverse=True)


def test_limit_truncates_the_feed(client: TestClient):
    body = client.get(f"{BASE}/news", params={"symbols": ["AAPL", "MSFT"], "limit": 1}).json()
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
    body = client.get(f"{BASE}/news", params={"symbols": ["TSLA"], "watchlist": "MAG7"}).json()
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


def test_a_response_with_no_news_field_is_503_not_an_empty_feed(client: TestClient):
    """
    Decision 57. yfinance parsed the dead endpoint's 404 body and returned []
    without raising, so every feed read "no news" for weeks. A search answer
    always has a `news` field, even when empty; one without it is a failure.
    """
    response = client.get(f"{BASE}/news", params={"symbols": ["GONE"]})
    assert response.status_code == 503


def test_news_items_carry_the_tickers_yahoo_tags(client: TestClient):
    items = client.get(f"{BASE}/news", params={"symbols": ["AAPL"]}).json()["items"]
    shared = next(i for i in items if i["title"] == "Tech selloff")
    assert shared["related_tickers"] == ["AAPL", "MSFT"]


class ShareClasses:
    """The registry's answer for GOOG/GOOGL; every other symbol is unregistered."""

    async def equity(self, symbol):
        return None

    async def share_classes(self, symbols):
        known = {"GOOGL": ["GOOG"], "GOOG": ["GOOGL"]}
        return {s: known[s] for s in symbols if s in known}

    async def news_identities(self, symbols):
        return {}

    async def news_names(self, symbols):
        return {}


@pytest.fixture
def classes_client(client: TestClient):
    from api.main import app
    from api.routers.valuation import get_valuation_data

    app.dependency_overrides[get_valuation_data] = lambda: ShareClasses()
    return client


def test_a_story_tagged_with_another_share_class_is_kept(classes_client: TestClient):
    body = classes_client.get(f"{BASE}/news", params={"symbols": ["GOOGL"]}).json()
    assert [i["title"] for i in body["items"]] == ["Alphabet earnings"]
    assert body["unrelated_dropped"] == {}


def test_without_its_share_class_the_story_is_dropped(client: TestClient):
    """The same request with no registry answer: GOOG is not GOOGL."""
    body = client.get(f"{BASE}/news", params={"symbols": ["GOOGL"]}).json()
    assert body["items"] == []
    assert body["unrelated_dropped"] == {"GOOGL": 1}


def test_a_story_not_tagged_with_its_ticker_is_dropped_and_counted(client: TestClient):
    """Measured 2026-10-22: FOX's search returned Fox Factory and Terry Fox."""
    body = client.get(f"{BASE}/news", params={"symbols": ["FOX"]}).json()
    assert [i["title"] for i in body["items"]] == ["Fox recasts its legal team"]
    assert body["unrelated_dropped"] == {"FOX": 1}


def test_relevance_is_judged_per_symbol_before_de_duplication(client: TestClient):
    """
    Found under IBM (untagged) and ORCL (tagged): kept. Filtering the merged
    list would judge it only as IBM's, the first symbol it was found under.
    """
    body = client.get(f"{BASE}/news", params={"symbols": ["IBM", "ORCL"]}).json()
    assert "Cloud price war" in [i["title"] for i in body["items"]]
    assert body["unrelated_dropped"] == {"IBM": 1}


def test_one_broken_symbol_does_not_blank_the_feed(client: TestClient):
    """A single delisted ticker in a watchlist must not empty the whole feed."""
    body = client.get(f"{BASE}/news", params={"symbols": ["AAPL", "BROKEN"]}).json()
    assert body["items"]


def test_a_symbol_with_no_stories_is_an_empty_feed_not_an_error(client: TestClient):
    body = client.get(f"{BASE}/news", params={"symbols": ["QUIET"]})
    assert body.status_code == 200
    assert body.json()["items"] == []


# ---------------------------------------------------------------------------
# Search terms: coins and word tickers (2026-10-22,
# research/news-fallbacks-2026-10-22.md)
# ---------------------------------------------------------------------------


def story(uuid, tags, title=None):
    return {
        "uuid": uuid,
        "title": title or f"Story {uuid}",
        "providerPublishTime": 1786291200,
        "publisher": "Reuters",
        "link": f"https://example.com/{uuid}",
        "relatedTickers": list(tags),
    }


class TermsSearch:
    """Yahoo as measured: a coin's pair answers nothing, its base and name do."""

    NEWS = {
        "ETH-USD": [],
        "ETH": [story("e1", ["ETH-USD"]), story("e2", ["SOL-USD"])],
        # e1 and e2 again: found by two of ETH-USD's terms, judged once each.
        "Ethereum": [
            story("e1", ["ETH-USD"]),
            story("e2", ["SOL-USD"]),
            story("e3", ["ETH-USD", "BTC-USD"]),
        ],
        "UNI-USD": [],
        "UNI": [],
        # Tagged with the provider ticker alone, as 3 of 10 were.
        "Uniswap": [story("u1", ["UNI7083-USD"])],
        "KEY": [story("k1", ["KEYS"]), story("k2", ["K"])],
        "KEYCORP": [story("k3", ["KEY"])],
        "XYZ-USD": [],
        "XYZ": [],
        # Decision 77: Yahoo tagged PDD's own earnings story with nothing.
        "PDD": [
            story("p1", [], "PDD Holdings Q2 2026 earnings: profit falls 12%, beats estimates"),
            story("p2", ["BABA"], "Alibaba takes on PDD Holdings in a price war"),
            story("p3", [], "Temu's ad push reaches Europe"),
            story("p4", ["PDD"], "Story p4"),
        ],
    }

    def __init__(self, term, calls):
        self.term = term
        calls.append(term)

    @property
    def response(self):
        if self.term in ("BROKEN", "Brokencoin"):
            raise RuntimeError("upstream down")
        return {"quotes": [], "news": self.NEWS.get(self.term, [])}


class Registry(ShareClasses):
    """ETH-USD, UNI-USD and XYZ-USD are coins and KEY an equity; nothing else is registered."""

    async def news_identities(self, symbols):
        known = {
            "ETH-USD": ("crypto", "Ethereum"),
            "UNI-USD": ("crypto", "Uniswap"),
            "XYZ-USD": ("crypto", "Brokencoin"),
            "KEY": ("equity", None),
        }
        return {s: known[s] for s in symbols if s in known}

    async def news_names(self, symbols):
        known = {"PDD": ["PDD Holdings Inc."], "ETH-USD": ["Ethereum"]}
        return {s: known[s] for s in symbols if s in known}


@pytest.fixture
def terms_client(client: TestClient):
    from api.main import app
    from api.routers.valuation import get_valuation_data
    from api.upstream import get_upstream
    from tests.api.conftest import StubGateway

    calls = []

    class Gateway(StubGateway):
        def _search(self, term):
            return TermsSearch(term, calls)

    gateway = Gateway()
    app.dependency_overrides[get_valuation_data] = lambda: Registry()
    app.dependency_overrides[get_upstream] = lambda: gateway
    client.calls = calls
    return client


def test_a_coin_is_searched_by_its_pair_base_symbol_and_name(terms_client: TestClient):
    body = terms_client.get(f"{BASE}/news", params={"symbols": ["ETH-USD"]}).json()
    assert sorted(terms_client.calls) == ["ETH", "ETH-USD", "Ethereum"]  # in parallel
    assert sorted(i["title"] for i in body["items"]) == ["Story e1", "Story e3"]
    # e2 (tagged SOL-USD) dropped, and counted once though both terms found it.
    assert body["unrelated_dropped"] == {"ETH-USD": 1}


def test_a_coin_s_provider_ticker_counts_as_its_own_tag(terms_client: TestClient):
    body = terms_client.get(f"{BASE}/news", params={"symbols": ["UNI-USD"]}).json()
    assert [i["title"] for i in body["items"]] == ["Story u1"]
    assert body["unrelated_dropped"] == {}


def test_a_word_ticker_is_searched_by_its_measured_term_too(terms_client: TestClient):
    body = terms_client.get(f"{BASE}/news", params={"symbols": ["KEY"]}).json()
    assert sorted(terms_client.calls) == ["KEY", "KEYCORP"]
    assert [i["title"] for i in body["items"]] == ["Story k3"]
    assert body["unrelated_dropped"] == {"KEY": 2}


def test_an_unregistered_symbol_is_searched_by_itself_only(terms_client: TestClient):
    terms_client.get(f"{BASE}/news", params={"symbols": ["AAPL"]})
    assert terms_client.calls == ["AAPL"]


def test_a_symbol_fails_only_when_every_term_fails(terms_client: TestClient):
    """XYZ-USD's name search raises; its pair and base answered (empty): no news, not 503."""
    response = terms_client.get(f"{BASE}/news", params={"symbols": ["XYZ-USD"]})
    assert response.status_code == 200
    assert response.json()["items"] == []


def test_a_cached_symbol_beside_a_broken_one_is_not_a_503(terms_client: TestClient):
    """
    KEY's terms answered on the first request and are cached; on the second
    only BROKEN is fetched, and it fails. That is one symbol of two, not all.
    """
    terms_client.get(f"{BASE}/news", params={"symbols": ["KEY"]})
    response = terms_client.get(f"{BASE}/news", params={"symbols": ["KEY", "BROKEN"]})
    assert response.status_code == 200
    assert [i["title"] for i in response.json()["items"]] == ["Story k3"]


def test_an_untagged_story_naming_the_company_is_kept_and_counted(terms_client: TestClient):
    """
    Decision 77 (research/headline-tagging-2026-10-26.md). p1 names PDD
    Holdings and carries no tag: kept, in `name_rescued`. p2 names it too but
    Yahoo tagged it BABA, so it is someone else's story; p3 names nothing.
    """
    body = terms_client.get(f"{BASE}/news", params={"symbols": ["PDD"]}).json()
    assert sorted(i["title"] for i in body["items"]) == [
        "PDD Holdings Q2 2026 earnings: profit falls 12%, beats estimates",
        "Story p4",
    ]
    rescued = next(i for i in body["items"] if i["title"].startswith("PDD Holdings"))
    assert rescued["related_tickers"] == []
    assert body["name_rescued"] == {"PDD": 1}
    assert body["unrelated_dropped"] == {"PDD": 2}


def test_a_symbol_without_a_name_rescues_nothing(terms_client: TestClient):
    body = terms_client.get(f"{BASE}/news", params={"symbols": ["KEY"]}).json()
    assert body["name_rescued"] == {}


# ---------------------------------------------------------------------------
# Financials from SEC filings (2026-10-12)
# ---------------------------------------------------------------------------


@pytest.fixture
def sec_client(client: TestClient):
    """The research API with Apple's real SEC facts behind it."""
    from api.main import app
    from api.routers.valuation import get_valuation_data
    from tests.api.test_valuation_router import FakeValuationData

    app.dependency_overrides[get_valuation_data] = lambda: FakeValuationData()
    return client


def test_financials_come_from_sec_filings_with_their_filing_dates(sec_client: TestClient):
    """
    Until 2026-10-12 these were yfinance's: four restated years, no filing
    date, and different numbers from the Valuation page's.
    """
    body = sec_client.get(f"{BASE}/AAPL/financials").json()
    assert body["source"].startswith("SEC filings")
    revenue = next(line for line in body["income_statement"] if line["line_item"] == "Revenue")
    assert revenue["values"]["2025-09-27"] == 416_161_000_000
    assert revenue["filed"]["2025-09-27"] == "2025-10-31"
    assert revenue["concept"]["2025-09-27"] == "RevenueFromContractWithCustomerExcludingAssessedTax"
    assert 4 < len(revenue["values"]) <= 10
    for statement in ("income_statement", "balance_sheet", "cash_flow"):
        keys = {tuple(line["values"]) for line in body[statement]}
        assert len(keys) == 1, f"{statement} lines have different columns"


def test_quarterly_cash_flow_is_one_quarter_each_not_year_to_date(sec_client: TestClient):
    body = sec_client.get(f"{BASE}/AAPL/financials", params={"quarterly": True}).json()
    cfo = next(line for line in body["cash_flow"] if line["line_item"] == "Operating cash flow")
    assert len(cfo["values"]) == 8
    assert any(cfo["derived"].values()) and not all(cfo["derived"].values())
    # A quarter, not a year to date: no figure is near the annual total.
    annual = sec_client.get(f"{BASE}/AAPL/financials").json()
    fy = next(line for line in annual["cash_flow"] if line["line_item"] == "Operating cash flow")
    assert max(v for v in cfo["values"].values() if v) < 0.5 * max(fy["values"].values())


def test_a_bank_gets_its_statements_though_the_dcf_refuses_it(sec_client: TestClient, monkeypatch):
    from tests.api.test_valuation_router import FakeValuationData

    monkeypatch.setitem(FakeValuationData.EQUITIES, "AAPL", ([320193], "Financials"))
    body = sec_client.get(f"{BASE}/AAPL/financials").json()
    assert body["source"].startswith("SEC filings")


def test_no_filings_falls_back_to_yfinance_and_says_why(sec_client: TestClient):
    """MSFT has a CIK in the fake but no stored facts."""
    body = sec_client.get(f"{BASE}/MSFT/financials").json()
    assert body["source"].startswith("yfinance") and "no SEC facts" in body["source"]
    assert body["income_statement"]


def test_filings_without_revenue_or_cash_flow_fall_back(sec_client: TestClient, monkeypatch):
    """CCEP and GFS file IFRS: facts exist, but no us-gaap revenue or cash flow."""
    from tests.api.test_valuation_router import FakeValuationData

    real = FakeValuationData.facts

    async def balances_only(self, ciks, concepts):
        return [f for f in await real(self, ciks, concepts) if f.period_start is None]

    monkeypatch.setattr(FakeValuationData, "facts", balances_only)
    body = sec_client.get(f"{BASE}/AAPL/financials").json()
    assert body["source"].startswith("yfinance") and "no standard annual revenue" in body["source"]


def test_an_annual_only_filer_falls_back_for_quarters_only(sec_client: TestClient, monkeypatch):
    """ASML files a US GAAP 20-F but no 10-Qs: years from the SEC, quarters elsewhere."""
    from tests.api.test_valuation_router import FakeValuationData

    real = FakeValuationData.facts

    async def annual_report_only(self, ciks, concepts):
        return [f for f in await real(self, ciks, concepts) if f.form == "10-K"]

    monkeypatch.setattr(FakeValuationData, "facts", annual_report_only)
    annual = sec_client.get(f"{BASE}/AAPL/financials").json()
    quarterly = sec_client.get(f"{BASE}/AAPL/financials", params={"quarterly": True}).json()
    assert annual["source"].startswith("SEC filings")
    # Apple's pre-2020 10-Ks carried quarterly data, so the newest "quarter"
    # here is 2020's: stale, and the view falls back saying so.
    assert quarterly["source"].startswith("yfinance") and "2020-09-26" in quarterly["source"]
