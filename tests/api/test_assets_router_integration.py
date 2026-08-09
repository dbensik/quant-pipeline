"""
GET /api/v1/assets — integration tests against a REAL TimescaleDB.

WHY THESE ARE DIFFERENT FROM THE OTHER ROUTER TESTS
    Every other router reaches its data through the repository Protocol, which
    a fake satisfies. `assets` queries the session directly with SQLAlchemy —
    the behaviour worth testing IS the SQL (ilike filtering, the coverage
    aggregate, offset/limit), and a stubbed session would only test the stub.

    Restructuring the router purely to make it fakeable would be changing
    working production code to suit a test, so it is tested for real instead
    and deselected by default.

RUNNING THEM
    docker-compose up -d timescaledb
    poetry run pytest -m integration

    `pytest tests/` skips this file (addopts carries -m 'not integration'), so
    the default suite still needs no Docker.

These assert on shape and invariants, not on exact counts, so an ingest run
does not turn them red.

Phase 3/4 — API router tests
"""

import pytest
from fastapi.testclient import TestClient

from api.main import app

pytestmark = pytest.mark.integration


@pytest.fixture
def live_client() -> TestClient:
    """
    No dependency overrides — this talks to the real database.

    Used as a context manager, unlike the hermetic fixtures in conftest.py,
    so the app's lifespan runs and assigns app.state.db_engine. Without it
    /api/v1/health/ready cannot work and every test below would skip.
    """
    with TestClient(app) as client:
        yield client


def _skip_if_db_down(client: TestClient) -> None:
    response = client.get("/api/v1/health/ready")
    if response.status_code != 200:
        pytest.skip("TimescaleDB is not reachable — start docker-compose first.")


def test_lists_assets(live_client: TestClient):
    _skip_if_db_down(live_client)
    body = live_client.get("/api/v1/assets?limit=5").json()
    assert body["count"] == len(body["assets"]) == 5
    asset = body["assets"][0]
    assert set(asset) == {"symbol", "asset_class", "source", "metadata"}


def test_assets_are_alphabetical(live_client: TestClient):
    _skip_if_db_down(live_client)
    symbols = [a["symbol"] for a in live_client.get("/api/v1/assets?limit=50").json()["assets"]]
    assert symbols == sorted(symbols)


def test_asset_class_filter(live_client: TestClient):
    _skip_if_db_down(live_client)
    body = live_client.get("/api/v1/assets?asset_class=crypto&limit=100").json()
    assert body["count"] > 0
    assert all(a["asset_class"] == "crypto" for a in body["assets"])


def test_search_is_a_case_insensitive_substring_match(live_client: TestClient):
    _skip_if_db_down(live_client)
    body = live_client.get("/api/v1/assets?search=btc&limit=50").json()
    assert body["count"] > 0
    # Lowercase query matching uppercase symbols proves ilike, not like.
    assert all("BTC" in a["symbol"].upper() for a in body["assets"])


def test_offset_pages_through_results(live_client: TestClient):
    _skip_if_db_down(live_client)
    first = live_client.get("/api/v1/assets?limit=5").json()["assets"]
    second = live_client.get("/api/v1/assets?limit=5&offset=5").json()["assets"]
    assert {a["symbol"] for a in first}.isdisjoint({a["symbol"] for a in second})


def test_detail_reports_data_coverage(live_client: TestClient):
    _skip_if_db_down(live_client)
    body = live_client.get("/api/v1/assets/AAPL").json()
    assert body["symbol"] == "AAPL"
    assert body["asset_class"] == "equity"
    assert body["bar_count"] > 0
    assert body["first_bar"] < body["last_bar"]


def test_detail_carries_metadata(live_client: TestClient):
    _skip_if_db_down(live_client)
    body = live_client.get("/api/v1/assets/AAPL").json()
    # Sector/index metadata was carried across from the legacy `universe` table
    # during the Phase 2 migration.
    assert body["metadata"].get("sector")


def test_registered_symbol_with_no_bars_reports_zero_coverage(live_client: TestClient):
    """
    A registered symbol with no bars must report zero coverage rather than 404,
    so a consumer can tell "unknown" from "known but empty".

    This used to assert against TAO-USD, one of five crypto tickers the Phase 2
    migration left with zero bars because every legacy row was an all-NULL
    padding bar. That premise expired on 2026-08-09: /api/v1/ingest fetched
    real data for them, so TAO-USD now has bars and the test failed on its own
    fixture rather than on a defect. It registers its own empty asset instead,
    which no ingest will fill.
    """
    _skip_if_db_down(live_client)
    symbol = "PYTEST-EMPTY-COVERAGE"

    created = live_client.post(
        "/api/v1/ingest/assets", json={"symbol": symbol, "asset_class": "equity"}
    )
    if created.status_code >= 400:
        pytest.skip(f"Could not register {symbol}: {created.text}")

    try:
        body = live_client.get(f"/api/v1/assets/{symbol}").json()
        assert body["bar_count"] == 0
        assert body["first_bar"] is None and body["last_bar"] is None
    finally:
        _drop_asset(symbol)


def _drop_asset(symbol: str) -> None:
    """
    Remove the throwaway asset. No API endpoint deletes one, so this is direct
    SQL — over a SYNC engine, deliberately. The async engine in db/session.py
    is bound to TestClient's portal loop, and asyncio.run() here opens a second
    loop that asyncpg rejects ("unknown protocol state").
    """
    from sqlalchemy import create_engine, text

    from db.session import settings

    engine = create_engine(settings.SYNC_DATABASE_URL)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM assets WHERE symbol = :symbol"), {"symbol": symbol}
            )
    finally:
        engine.dispose()


def test_unknown_symbol_is_404(live_client: TestClient):
    _skip_if_db_down(live_client)
    assert live_client.get("/api/v1/assets/NOT-A-REAL-SYMBOL").status_code == 404
