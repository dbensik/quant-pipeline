"""
/api/v1/ingest — fetching new bars into TimescaleDB.

Every test passes explicit `symbols`. That is deliberate: omitting them makes
the route query the asset registry, which needs a real database. The session
from Depends(get_db) is lazy — creating it opens no connection — so with
symbols supplied these run with no Docker, and the registry path is covered
by test_ingest_repo_integration.py instead.

The fetcher is stubbed via dependency_overrides, so nothing here reaches the
network.

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

BASE = "/api/v1/ingest"


def ingest(client: TestClient, **overrides):
    return client.post(BASE, json={"symbols": ["AAPL"], **overrides})


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def test_writes_fetched_bars(client: TestClient, repo, fetcher):
    """
    THE point of this step. The Streamlit button shelled out to
    cli/run_pipeline.py, which writes SQLite — nothing in that path touches
    TimescaleDB, so every run since the cutover filled a database the API does
    not read while reporting success.
    """
    body = ingest(client).json()
    assert body["written"] == fetcher.bars_per_symbol
    assert body["failed"] == []
    assert body["results"][0]["symbol"] == "AAPL"


def test_reports_the_window_covered(client: TestClient):
    body = ingest(client).json()
    result = body["results"][0]
    assert result["first_bar"] is not None
    assert result["last_bar"] >= result["first_bar"]


def test_several_symbols_are_each_reported(client: TestClient):
    body = ingest(client, symbols=["AAPL", "MSFT"]).json()
    assert {r["symbol"] for r in body["results"]} == {"AAPL", "MSFT"}


def test_symbols_are_upper_cased(client: TestClient):
    assert ingest(client, symbols=["aapl"]).json()["results"][0]["symbol"] == "AAPL"


def test_full_backfill_asks_for_the_default_start(client: TestClient, fetcher):
    ingest(client, full_backfill=True)
    assert fetcher.calls[-1][1] == "2015-01-01"


def test_explicit_start_is_passed_through(client: TestClient, fetcher):
    ingest(client, start="2020-03-01T00:00:00Z")
    assert fetcher.calls[-1][1] == "2020-03-01"


def test_resume_starts_after_the_newest_stored_bar(client: TestClient, fetcher):
    """
    The fixture repo holds 400 daily bars from 2024-01-01, so the newest is
    2025-02-03; fetch_range is inclusive, so the request must begin the day
    after or it re-downloads a bar the upsert discards.
    """
    ingest(client)
    assert fetcher.calls[-1][1] == "2025-02-04"


# ---------------------------------------------------------------------------
# Asset identity
# ---------------------------------------------------------------------------

def test_crypto_keeps_its_asset_class(client: TestClient, repo):
    """
    yfinance_adapter hardcodes asset_class="equity", and assets are keyed on
    (symbol, asset_class, source) — writing BTC-USD as equity would create a
    SECOND asset row and file its bars under an id no query uses. The stub
    fetcher reproduces the hardcoding, so this asserts the retag.
    """
    ingest(client, symbols=["BTC-USD"])
    assert {r.asset.asset_class for r in repo.written} == {"crypto"}


# ---------------------------------------------------------------------------
# Failure isolation
# ---------------------------------------------------------------------------

def test_one_failing_symbol_does_not_end_the_run(client: TestClient, fetcher):
    fetcher.fail_for = {"MSFT"}
    body = ingest(client, symbols=["MSFT", "AAPL"]).json()
    assert body["failed"] == ["MSFT"]
    assert body["written"] == fetcher.bars_per_symbol


def test_an_unregistered_symbol_is_reported_not_fetched(client: TestClient, fetcher):
    body = ingest(client, symbols=["NOSUCH"]).json()
    assert body["results"][0]["error"]
    assert "registry" in body["results"][0]["error"]
    assert fetcher.calls == []


def test_a_failed_run_still_reports_the_others(client: TestClient, fetcher):
    fetcher.fail_for = {"AAPL"}
    body = ingest(client, symbols=["AAPL", "MSFT"]).json()
    assert len(body["results"]) == 2


# ---------------------------------------------------------------------------
# Single-flight
# ---------------------------------------------------------------------------

def test_status_is_idle_between_runs(client: TestClient):
    assert client.get(f"{BASE}/status").json()["running"] is False


def test_status_is_idle_after_a_run_completes(client: TestClient):
    ingest(client)
    assert client.get(f"{BASE}/status").json()["running"] is False


def test_the_guard_is_released_even_when_the_run_fails(client: TestClient, fetcher):
    """
    Without the finally block a mid-run failure would leave the job stuck
    "running" and 409 every later request until the process restarted.
    """
    fetcher.fail_for = {"AAPL"}
    ingest(client)
    assert client.get(f"{BASE}/status").json()["running"] is False
    assert ingest(client).status_code == 200


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

# NOTE: there is no test here for `symbols: []` or an omitted `symbols`.
# Both fall back to querying the asset registry, which needs a real database —
# and a version of that test written here PASSED by silently reaching the
# running container, which is exactly the kind of hidden dependency this
# conftest exists to prevent. The fallback is covered in
# test_ingest_repo_integration.py.


def test_too_many_symbols_is_422(client: TestClient):
    response = client.post(BASE, json={"symbols": [f"T{i}" for i in range(1001)]})
    assert response.status_code == 422
    assert "limit is 1000" in response.json()["detail"]


def test_blank_symbols_are_dropped(client: TestClient, fetcher):
    body = ingest(client, symbols=["AAPL", "", "  "]).json()
    assert body["symbols"] == ["AAPL"]


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

def test_unknown_universe_source_is_422(client: TestClient):
    response = client.get(f"{BASE}/universe", params={"source": "ftse"})
    assert response.status_code == 422
    assert "Unknown source" in response.json()["detail"]


def test_universe_source_is_required(client: TestClient):
    assert client.get(f"{BASE}/universe").status_code == 422


# ---------------------------------------------------------------------------
# Asset registration — accepted asset classes
# ---------------------------------------------------------------------------

class _FakeResult:
    """Enough of a SQLAlchemy Result for add_asset's "not found" branch."""

    def unique(self):
        return self

    def scalars(self):
        return self

    def first(self):
        return None


class _FakeSession:
    """
    Stands in for Depends(get_db).

    Unlike every other test in this file, POST /assets genuinely USES its
    session — it selects, adds and commits. The module docstring's "the session
    is lazy so these need no Docker" does not hold here, and a first version of
    this test wrote SPY to the real TimescaleDB. That is the exact hidden
    dependency the note above test_too_many_symbols_is_422 warns about.
    """

    def __init__(self):
        self.added = []

    async def execute(self, _stmt):
        return _FakeResult()

    def add(self, row):
        self.added.append(row)

    async def commit(self):
        pass


def test_etf_is_an_accepted_asset_class(client: TestClient):
    """
    'etf' is deliberately NOT filed under 'equity'. An ETF does not leave an
    index, so it carries no survivorship bias — which is why asset-allocation
    strategies are backtestable here when cross-sectional equity ones are not
    (membership history is 2 days deep and cannot be backdated). Filing SPY as
    an equity would place it in any cross-sectional stock ranking.
    """
    from api.dependencies import get_db

    fake = _FakeSession()
    client.app.dependency_overrides[get_db] = lambda: fake
    try:
        response = client.post(
            f"{BASE}/assets", json={"symbol": "SPY", "asset_class": "etf"}
        )
    finally:
        client.app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 201, response.text
    assert response.json() == {
        "symbol": "SPY",
        "asset_class": "etf",
        "source": "yfinance",
        "created": True,
    }
    assert [r.asset_class for r in fake.added] == ["etf"]


def test_an_unknown_asset_class_is_still_rejected(client: TestClient):
    """The guard must stay a closed set, not become a free-text column."""
    response = client.post(
        f"{BASE}/assets", json={"symbol": "SPY", "asset_class": "mutual_fund"}
    )
    assert response.status_code == 422
    assert "mutual_fund" in response.json()["detail"]
