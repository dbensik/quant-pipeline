"""
/api/v1/universe, and the screener's `index` option.

The behaviour under test is a REFUSAL: asked for a universe before any
snapshot, the API must not fall back to today's membership. That fallback is
survivorship bias and it is invisible in the output — the screen still returns
plausible names, just not the ones that were actually in the index.

Phase 5 — point-in-time universe
"""

from fastapi.testclient import TestClient

BASE = "/api/v1/universe"
SCREEN = "/api/v1/screeners/run"

# The fake repo observes sp500 on 2024-06-01 and 2024-12-01, with BTC-USD
# dropped in between and MSFT added.
EARLY = "2024-06-15"
LATE = "2024-12-15"
BEFORE_ANY = "2023-01-01"


def screen(client: TestClient, **overrides):
    # A permissive step: the router requires at least one, and these tests are
    # about which UNIVERSE is screened, not about the filtering.
    body = {
        "start": f"{EARLY}T00:00:00Z",
        "end": "2024-12-31T00:00:00Z",
        "screeners": [{"screener_id": "low_volatility", "params": {"quantile": 1.0}}],
        **overrides,
    }
    return client.post(SCREEN, json=body)


# ---------------------------------------------------------------------------
# Membership
# ---------------------------------------------------------------------------

def test_lists_indexes_with_history(client: TestClient):
    assert client.get(BASE).json() == ["sp500"]


def test_members_as_of_a_covered_date(client: TestClient):
    body = client.get(f"{BASE}/sp500/members", params={"as_of": EARLY}).json()
    assert body["observed"] is True
    assert "BTC-USD" in body["symbols"]


def test_a_dropped_member_is_absent_later_but_present_earlier(client: TestClient):
    """The point of the whole table: a name that left stays queryable in its
    own window."""
    early = client.get(f"{BASE}/sp500/members", params={"as_of": EARLY}).json()
    late = client.get(f"{BASE}/sp500/members", params={"as_of": LATE}).json()

    assert "BTC-USD" in early["symbols"]
    assert "BTC-USD" not in late["symbols"]
    assert "MSFT" in late["symbols"]


def test_a_date_before_any_snapshot_is_not_observed(client: TestClient):
    body = client.get(f"{BASE}/sp500/members", params={"as_of": BEFORE_ANY}).json()
    assert body["observed"] is False
    assert body["symbols"] == []


def test_the_unobserved_response_says_today_is_not_a_substitute(client: TestClient):
    """
    A caller reading `symbols: []` as "empty index" would draw the wrong
    conclusion; one substituting today's list would reintroduce the bias.
    """
    body = client.get(f"{BASE}/sp500/members", params={"as_of": BEFORE_ANY}).json()
    assert "not" in body["detail"].lower()
    assert "substitute" in body["detail"]


def test_an_unknown_index_is_reported_as_never_snapshotted(client: TestClient):
    body = client.get(f"{BASE}/ftse/members").json()
    assert body["observed"] is False
    assert "never been snapshotted" in body["detail"]


def test_snapshot_history_is_listed(client: TestClient):
    history = client.get(f"{BASE}/sp500/snapshots").json()
    assert len(history) == 2
    assert history[0]["taken_at"] > history[1]["taken_at"]


def test_snapshotting_an_unknown_index_is_422(client: TestClient):
    response = client.post(f"{BASE}/ftse/snapshot")
    assert response.status_code == 422
    assert "Unknown index" in response.json()["detail"]


# ---------------------------------------------------------------------------
# The screener's index option — where the bias actually bites
# ---------------------------------------------------------------------------

def test_screening_by_index_uses_membership_as_of_the_start(client: TestClient):
    """
    BTC-USD was in the index at EARLY and is not a member today. Screening the
    EARLY window must include it — excluding it is the survivorship bias.
    """
    early = screen(client, index="sp500").json()
    late = screen(client, index="sp500", start=f"{LATE}T00:00:00Z").json()

    # Asserted on the resolved UNIVERSE, not on `passed`: whether a symbol
    # survives the filter is a different question from whether it was in the
    # index, and only the second one is being tested here.
    assert "BTC-USD" in early["universe"]
    assert "BTC-USD" not in late["universe"]
    assert "MSFT" in late["universe"]


def test_screening_by_index_before_any_snapshot_is_refused(client: TestClient):
    """
    THE refusal. Falling back to today's membership here is the survivorship
    bias the index option exists to avoid, so it is an error rather than a
    silent substitution.
    """
    response = screen(client, index="sp500", start=f"{BEFORE_ANY}T00:00:00Z")
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "No snapshot" in detail
    assert "not" in detail and "substitute" in detail


def test_symbols_and_index_together_are_rejected(client: TestClient):
    response = screen(client, index="sp500", symbols=["AAPL"])
    assert response.status_code == 422
    assert "not both" in response.json()["detail"]


def test_explicit_symbols_still_work(client: TestClient):
    body = screen(client, symbols=["AAPL", "MSFT"]).json()
    assert body["requested"] == 2


def test_neither_symbols_nor_index_is_422(client: TestClient):
    response = screen(client)
    assert response.status_code == 422
    assert "must not be empty" in response.json()["detail"]
