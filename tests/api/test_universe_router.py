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


# ---------------------------------------------------------------------------
# Reconstructed membership and the index cap (decision 42)
# ---------------------------------------------------------------------------

BETWEEN = "2024-04-15"  # after the reconstructed 2024-03-31 list, before the first snapshot


def test_before_the_first_snapshot_the_reconstructed_list_is_used_and_said(
    client: TestClient, monkeypatch
):
    monkeypatch.setattr("api.routers.screeners._renames", lambda: {"OLDMSFT": ("MSFT", None)})
    body = screen(client, index="sp500", start=f"{BETWEEN}T00:00:00Z").json()

    assert body["membership_source"] == "reconstructed"
    assert body["membership_date"].startswith("2024-03-31")
    assert "not observed" in body["membership_note"]
    # The ticker of the day is priced under today's ticker...
    assert body["renamed"] == {"OLDMSFT": "MSFT"}
    assert body["universe"] == ["AAPL", "GONE", "MSFT"]
    # ...and a member with no prices is named, not silently dropped.
    assert body["requested"] == 3
    assert body["with_data"] == 2
    assert body["without_data"] == ["GONE"]


def test_an_observed_date_says_it_is_observed(client: TestClient):
    body = screen(client, index="sp500").json()
    assert body["membership_source"] == "observed"
    assert body["renamed"] == {}


def test_typed_symbols_report_no_membership_source(client: TestClient):
    body = screen(client, symbols=["AAPL", "MSFT"]).json()
    assert body["membership_source"] is None
    assert body["without_data"] == []


def test_an_index_larger_than_the_typed_symbol_cap_can_be_screened(
    client: TestClient, universe_repo
):
    """
    The S&P 500 is ~503 securities; with the 200-symbol cap applied to
    index-resolved universes every S&P 500 screen was refused.
    """
    first, second = universe_repo.FIRST, universe_repo.SECOND
    for i in range(300):
        universe_repo.members[f"X{i:03d}"] = (first, second)
    response = screen(client, index="sp500")
    assert response.status_code == 200
    assert response.json()["requested"] > 300


def test_typed_symbols_keep_their_cap(client: TestClient):
    response = screen(client, symbols=[f"X{i:03d}" for i in range(201)])
    assert response.status_code == 422
    assert "limit is 200" in response.json()["detail"]


def test_a_listed_share_class_is_priced_under_the_registry_form(client: TestClient, universe_repo):
    """The source page writes BRK.B; its bars are stored as BRK-B."""
    universe_repo.members["BRK.B"] = (universe_repo.FIRST, universe_repo.SECOND)
    body = screen(client, index="sp500").json()
    assert body["renamed"] == {"BRK.B": "BRK-B"}
    assert "BRK-B" in body["universe"] and "BRK.B" not in body["universe"]


def test_an_observed_member_renamed_since_is_priced_under_todays_ticker(
    client: TestClient, universe_repo, monkeypatch
):
    """PSKY was observed in the index until 2026-10-08; its bars are now stored as SKYD."""
    monkeypatch.setattr("api.routers.screeners._renames", lambda: {"OLDAAPL": ("AAPL", None)})
    first, second = universe_repo.FIRST, universe_repo.SECOND
    del universe_repo.members["AAPL"]
    universe_repo.members["OLDAAPL"] = (first, second)
    body = screen(client, index="sp500").json()
    assert body["renamed"] == {"OLDAAPL": "AAPL"}
    assert "AAPL" in body["universe"] and "AAPL" not in body["without_data"]
