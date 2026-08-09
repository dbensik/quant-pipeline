"""
/api/v1/watchlists — named lists of tickers.

Ported from dashboard_app/watchlist_manager.py and `watchlists.json`.

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

BASE = "/api/v1/watchlists"


def test_lists_watchlists(client: TestClient):
    assert [w["name"] for w in client.get(BASE).json()] == ["Crypto", "MAG7"]


def test_gets_one_watchlist(client: TestClient):
    body = client.get(f"{BASE}/MAG7").json()
    assert body["symbols"] == ["AAPL", "MSFT", "NVDA"]


def test_unknown_watchlist_is_404(client: TestClient):
    assert client.get(f"{BASE}/Nope").status_code == 404


def test_creates_a_watchlist(client: TestClient):
    body = client.put(f"{BASE}/Energy", json={"symbols": ["XOM", "CVX"]}).json()
    assert body["symbols"] == ["XOM", "CVX"]
    assert "Energy" in [w["name"] for w in client.get(BASE).json()]


def test_saving_replaces_rather_than_merges(client: TestClient):
    """
    The Streamlit form submitted the complete multiselect. A merge would make
    removing a ticker impossible through the same control that adds one.
    """
    client.put(f"{BASE}/MAG7", json={"symbols": ["AAPL"]})
    assert client.get(f"{BASE}/MAG7").json()["symbols"] == ["AAPL"]


def test_symbols_are_upper_cased(client: TestClient):
    body = client.put(f"{BASE}/Lower", json={"symbols": ["aapl", "msft"]}).json()
    assert body["symbols"] == ["AAPL", "MSFT"]


def test_duplicates_collapse_keeping_the_first_position(client: TestClient):
    """
    The unique constraint would reject a repeat outright; keeping the first
    occurrence is what a list of tickers means to the UI.
    """
    body = client.put(
        f"{BASE}/Dupes", json={"symbols": ["AAPL", "MSFT", "aapl", "NVDA"]}
    ).json()
    assert body["symbols"] == ["AAPL", "MSFT", "NVDA"]


def test_order_is_preserved(client: TestClient):
    """A set would not preserve the arrangement the user chose."""
    symbols = ["NVDA", "AAPL", "TSLA", "MSFT"]
    assert client.put(f"{BASE}/Ordered", json={"symbols": symbols}).json()[
        "symbols"
    ] == symbols


def test_an_empty_list_is_allowed(client: TestClient):
    """Clearing a watchlist is a legitimate edit, not an error."""
    assert client.put(f"{BASE}/MAG7", json={"symbols": []}).status_code == 200
    assert client.get(f"{BASE}/MAG7").json()["symbols"] == []


def test_too_many_symbols_is_422(client: TestClient):
    response = client.put(
        f"{BASE}/Huge", json={"symbols": [f"T{i}" for i in range(501)]}
    )
    assert response.status_code == 422
    assert "limit is 500" in response.json()["detail"]


def test_deletes_a_watchlist(client: TestClient):
    assert client.delete(f"{BASE}/MAG7").status_code == 204
    assert "MAG7" not in [w["name"] for w in client.get(BASE).json()]


def test_deleting_an_unknown_watchlist_is_404(client: TestClient):
    assert client.delete(f"{BASE}/Nope").status_code == 404


def test_filtering_by_symbol(client: TestClient):
    """
    The news feed asks "which watchlists hold this ticker?", which is why the
    symbols live in an indexed child table rather than a JSONB array.
    """
    names = [w["name"] for w in client.get(BASE, params={"symbol": "AAPL"}).json()]
    assert names == ["MAG7"]


def test_symbol_filter_is_case_insensitive(client: TestClient):
    assert [w["name"] for w in client.get(BASE, params={"symbol": "aapl"}).json()] == [
        "MAG7"
    ]


def test_symbol_filter_matching_nothing_returns_empty(client: TestClient):
    assert client.get(BASE, params={"symbol": "ZZZZ"}).json() == []
