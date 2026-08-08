"""
GET /api/v1/ohlcv/{symbol}

Phase 3/4 — API router tests
"""

from fastapi.testclient import TestClient

RANGE = "start=2024-01-01&end=2024-03-01"


def test_returns_bars_for_a_known_symbol(client: TestClient):
    body = client.get(f"/api/v1/ohlcv/AAPL?{RANGE}").json()
    assert body["symbol"] == "AAPL"
    assert body["asset_class"] == "equity"
    assert body["count"] == len(body["bars"]) > 0
    bar = body["bars"][0]
    assert set(bar) == {"time", "open", "high", "low", "close", "volume"}


def test_bars_are_ordered_and_inside_the_requested_range(client: TestClient):
    bars = client.get(f"/api/v1/ohlcv/AAPL?{RANGE}").json()["bars"]
    times = [bar["time"] for bar in bars]
    assert times == sorted(times)
    assert times[0][:10] >= "2024-01-01"
    assert times[-1][:10] <= "2024-03-01"


def test_narrower_range_returns_fewer_bars(client: TestClient):
    """Guards against a range filter that is ignored — the failure that would
    make every other range assertion meaningless."""
    wide = client.get("/api/v1/ohlcv/AAPL?start=2024-01-01&end=2024-06-01").json()
    narrow = client.get("/api/v1/ohlcv/AAPL?start=2024-01-01&end=2024-02-01").json()
    assert narrow["count"] < wide["count"]


def test_unknown_symbol_is_404(client: TestClient):
    response = client.get(f"/api/v1/ohlcv/NOPE?{RANGE}")
    assert response.status_code == 404
    assert "NOPE" in response.json()["detail"]


def test_known_symbol_with_no_bars_is_200_and_empty(client: TestClient):
    """
    A deliberate distinction from the 404 above, and easy to regress into
    "everything empty is an error": a registered symbol with nothing in the
    requested window is a valid, empty answer — not a failure. Five real crypto
    tickers are registered with zero bars.
    """
    response = client.get(f"/api/v1/ohlcv/EMPTY-USD?{RANGE}")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 0
    assert body["bars"] == []


def test_known_symbol_outside_its_data_range_is_200_and_empty(client: TestClient):
    response = client.get("/api/v1/ohlcv/AAPL?start=1990-01-01&end=1990-12-31")
    assert response.status_code == 200
    assert response.json()["count"] == 0


def test_start_after_end_is_422(client: TestClient):
    response = client.get("/api/v1/ohlcv/AAPL?start=2024-06-01&end=2024-01-01")
    assert response.status_code == 422


def test_naive_dates_are_accepted_as_utc(client: TestClient):
    """
    Callers pass YYYY-MM-DD. Rejecting date-only values, or shifting them by a
    local timezone, would silently drop boundary bars.
    """
    body = client.get(f"/api/v1/ohlcv/AAPL?{RANGE}").json()
    assert body["start"].startswith("2024-01-01")
    assert body["end"].startswith("2024-03-01")


def test_crypto_symbol_reports_its_asset_class(client: TestClient):
    body = client.get(f"/api/v1/ohlcv/BTC-USD?{RANGE}").json()
    assert body["asset_class"] == "crypto"
