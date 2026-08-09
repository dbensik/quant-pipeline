"""
/api/v1/screeners

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
UNIVERSE = ["AAPL", "BTC-USD"]


def run(client: TestClient, screeners, symbols=None, **overrides):
    body = {
        "symbols": symbols if symbols is not None else UNIVERSE,
        **WINDOW,
        "screeners": screeners,
        **overrides,
    }
    return client.post("/api/v1/screeners/run", json=body)


# ---------------------------------------------------------------------------
# Catalogue
# ---------------------------------------------------------------------------

def test_lists_every_registered_screener(client: TestClient):
    body = client.get("/api/v1/screeners").json()
    assert body["count"] == len(body["screeners"]) == 3
    assert {s["id"] for s in body["screeners"]} == {
        "low_volatility",
        "momentum",
        "fundamental",
    }


def test_each_screener_carries_a_usable_param_schema(client: TestClient):
    """
    Both UIs generate their controls from this, exactly as they do for
    strategies. A missing type, label or default is a control that cannot be
    rendered.
    """
    for screener in client.get("/api/v1/screeners").json()["screeners"]:
        assert screener["id"] and screener["display_name"] and screener["description"]
        for param in screener["params"]:
            assert param["name"]
            assert param["type"] in {"int", "float", "str"}
            assert param["default"] is not None
            assert param["label"]


def test_momentum_schema_is_specific(client: TestClient):
    body = client.get("/api/v1/screeners/momentum").json()
    params = {p["name"]: p for p in body["params"]}
    assert params["momentum_window"]["default"] == 126
    assert params["momentum_window"]["type"] == "int"
    assert params["min_momentum"]["default"] == 0.10


def test_unknown_screener_404s_and_lists_valid_ids(client: TestClient):
    response = client.get("/api/v1/screeners/not_a_screener")
    assert response.status_code == 404
    assert "momentum" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Running a screen
# ---------------------------------------------------------------------------

def test_returns_the_symbols_that_passed(client: TestClient):
    body = run(client, [{"screener_id": "fundamental", "params": {"min_price": 0}}]).json()
    assert body["requested"] == 2
    assert body["with_data"] == 2
    assert set(body["passed"]) <= set(UNIVERSE)


def test_reports_per_step_counts(client: TestClient):
    """
    A screen returning nothing must show WHERE it emptied. Without per-step
    counts, an over-tight first filter and an over-tight last one look
    identical.
    """
    body = run(
        client,
        [
            {"screener_id": "fundamental", "params": {"min_price": 0}},
            {"screener_id": "low_volatility", "params": {"quantile": 1.0}},
        ],
    ).json()
    assert [s["screener_id"] for s in body["steps"]] == [
        "fundamental",
        "low_volatility",
    ]
    assert all("passed" in s for s in body["steps"])


def test_steps_compose_narrowing_each_time(client: TestClient):
    """
    Screeners filter the SURVIVORS of the previous step, not the original
    universe — the composition ScreenerPipeline performs, and what the
    Streamlit sidebar's stacked checkboxes meant.

    The ordering here is deliberate and the assertion only holds under
    composition: a restrictive step FIRST, then a permissive one. Composed, the
    permissive step sees only BTC-USD and returns it. Run independently against
    the full universe it would return both symbols, so the final result
    discriminates.

    (An earlier version put the restrictive step last. Both orderings ended at
    zero, so it passed whether or not the steps composed — vacuous.)
    """
    body = run(
        client,
        [
            {"screener_id": "fundamental", "params": {"min_price": 1000}},
            {"screener_id": "fundamental", "params": {"min_price": 0}},
        ],
    ).json()
    counts = [s["passed"] for s in body["steps"]]
    assert counts == sorted(counts, reverse=True)
    assert body["passed"] == ["BTC-USD"]
    assert "AAPL" not in body["passed"]


def test_a_price_floor_excludes_the_cheaper_symbol(client: TestClient):
    """
    Discriminating: the fixture's AAPL trades near 100 and BTC-USD near 30,000,
    so a floor between them must keep exactly one. A screener that ignored its
    parameters would return both.
    """
    body = run(
        client, [{"screener_id": "fundamental", "params": {"min_price": 1000}}]
    ).json()
    assert body["passed"] == ["BTC-USD"]


def test_effective_params_are_echoed_with_defaults_filled(client: TestClient):
    body = run(client, [{"screener_id": "momentum"}]).json()
    assert body["steps"][0]["params"] == {
        "momentum_window": 126,
        "min_momentum": 0.10,
    }


def test_symbols_without_data_are_counted_not_silently_failed(client: TestClient):
    """
    A symbol with no bars cannot be screened. Counting it separately keeps
    "nothing passed the filter" distinguishable from "nothing had data" — five
    registered crypto tickers really do hold zero bars.
    """
    body = run(
        client,
        [{"screener_id": "fundamental", "params": {"min_price": 0}}],
        symbols=["AAPL", "EMPTY-USD"],
    ).json()
    assert body["requested"] == 2
    assert body["with_data"] == 1


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_unknown_screener_id_is_404(client: TestClient):
    assert run(client, [{"screener_id": "nope"}]).status_code == 404


def test_unknown_param_name_is_rejected_not_ignored(client: TestClient):
    response = run(client, [{"screener_id": "momentum", "params": {"windwo": 10}}])
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


def test_empty_symbol_list_is_422(client: TestClient):
    assert run(client, [{"screener_id": "momentum"}], symbols=[]).status_code == 422


def test_empty_screener_list_is_422(client: TestClient):
    assert run(client, []).status_code == 422


def test_start_after_end_is_422(client: TestClient):
    response = run(
        client, [{"screener_id": "momentum"}], start="2024-12-31", end="2024-01-01"
    )
    assert response.status_code == 422


def test_no_symbol_has_data_is_422(client: TestClient):
    response = run(
        client,
        [{"screener_id": "momentum"}],
        symbols=["EMPTY-USD"],
    )
    assert response.status_code == 422
    assert "bars" in response.json()["detail"]


def test_too_many_symbols_is_422(client: TestClient):
    """
    Screening loads full OHLCV per symbol; the whole 616-symbol universe over
    five years is ~800k rows in one request cycle. The cap makes that an
    explicit error rather than a timeout.
    """
    response = run(client, [{"screener_id": "momentum"}], symbols=["AAPL"] * 201)
    assert response.status_code == 422
    assert "limit" in response.json()["detail"]


def test_invalid_params_fail_before_any_data_is_loaded(client: TestClient):
    """
    Screeners are built up-front so a bad parameter fails immediately rather
    than after fetching history for every symbol.
    """
    response = run(client, [{"screener_id": "momentum", "params": {"bogus": 1}}])
    assert response.status_code == 422
