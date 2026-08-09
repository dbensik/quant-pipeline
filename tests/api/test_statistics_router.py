"""
/api/v1/statistics

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}


def run(client: TestClient, test_id: str, symbols, **overrides):
    body = {"symbols": symbols, **WINDOW, **overrides}
    return client.post(f"/api/v1/statistics/{test_id}", json=body)


# ---------------------------------------------------------------------------
# Catalogue
# ---------------------------------------------------------------------------

def test_lists_every_registered_test(client: TestClient):
    body = client.get("/api/v1/statistics").json()
    assert body["count"] == len(body["tests"]) == 6
    assert {t["id"] for t in body["tests"]} == {
        "adf", "kalman", "ols", "engle_granger", "johansen", "pca",
    }


def test_each_test_declares_arity_and_input_kind(client: TestClient):
    """
    A UI needs both to build its panel: arity says how many symbols to ask for,
    input_kind tells the user whether they are looking at a price- or
    returns-based result.
    """
    for test in client.get("/api/v1/statistics").json()["tests"]:
        assert test["arity"] in {"single", "pair", "multi"}
        assert test["input_kind"] in {"price", "returns"}
        assert test["min_symbols"] >= 1


def test_input_kinds_are_the_statistically_correct_ones(client: TestClient):
    """
    Pins the choice rather than leaving it to drift. ADF and cointegration need
    price LEVELS — run on returns they are near-meaningless, since returns are
    almost always stationary. OLS alpha/beta and PCA need RETURNS; on levels
    PCA measures shared drift rather than co-movement.
    """
    kinds = {t["id"]: t["input_kind"] for t in client.get("/api/v1/statistics").json()["tests"]}
    assert kinds["adf"] == "price"
    assert kinds["engle_granger"] == "price"
    assert kinds["johansen"] == "price"
    assert kinds["kalman"] == "price"
    assert kinds["ols"] == "returns"
    assert kinds["pca"] == "returns"


def test_unknown_test_404s_and_lists_valid_ids(client: TestClient):
    response = client.get("/api/v1/statistics/not_a_test")
    assert response.status_code == 404
    assert "johansen" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Running tests
# ---------------------------------------------------------------------------

def test_adf_runs_on_price_levels(client: TestClient):
    body = run(client, "adf", ["AAPL"]).json()
    assert body["input_kind"] == "price"
    assert body["observations"] > 100
    assert body["result"]


def test_ols_runs_on_returns_and_loses_one_observation(client: TestClient):
    """
    pct_change drops the first row. Asserting the exact relationship proves the
    conversion happened rather than being skipped.
    """
    prices = run(client, "adf", ["AAPL"]).json()["observations"]
    returns = run(client, "ols", ["AAPL", "BTC-USD"]).json()
    assert returns["input_kind"] == "returns"
    assert returns["observations"] == prices - 1


def test_pca_returns_explained_variance_per_component(client: TestClient):
    body = run(client, "pca", ["AAPL", "BTC-USD"], params={"n_components": 2}).json()
    ratios = body["result"]["explained_variance_ratio"]
    assert len(ratios) == 2
    assert all(0 <= r <= 1 for r in ratios)
    assert sum(ratios) <= 1.0000001


def test_symbol_order_is_preserved(client: TestClient):
    """
    OLS treats the FIRST symbol as the asset and the second as the benchmark.
    Sorting them would silently invert alpha and beta, so order must survive.
    """
    body = run(client, "ols", ["BTC-USD", "AAPL"]).json()
    assert body["symbols"] == ["BTC-USD", "AAPL"]


def test_effective_params_are_echoed_with_defaults_filled(client: TestClient):
    body = run(client, "johansen", ["AAPL", "BTC-USD"]).json()
    assert body["params"] == {"det_order": 0, "k_ar_diff": 1}


def test_results_are_json_safe(client: TestClient):
    """
    statsmodels returns numpy scalars and arrays, and NaN is not valid JSON.
    A response that parses at all proves the conversion ran.
    """
    response = run(client, "adf", ["AAPL"])
    assert response.status_code == 200
    assert isinstance(response.json()["result"], dict)


# ---------------------------------------------------------------------------
# Alignment
# ---------------------------------------------------------------------------

def test_series_are_aligned_to_shared_dates(client: TestClient):
    """
    Crypto trades weekends and equities do not. Without an inner join, pairing
    the two would compare Monday's equity move against the weekend's crypto
    move — so the observation count must not exceed the shorter series.
    """
    equity_only = run(client, "adf", ["AAPL"]).json()["observations"]
    crypto_only = run(client, "adf", ["BTC-USD"]).json()["observations"]
    paired = run(client, "engle_granger", ["AAPL", "BTC-USD"]).json()["observations"]
    assert paired <= min(equity_only, crypto_only)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_single_test_rejects_two_symbols(client: TestClient):
    response = run(client, "adf", ["AAPL", "BTC-USD"])
    assert response.status_code == 422
    assert "exactly 1" in response.json()["detail"]


def test_pair_test_rejects_one_symbol(client: TestClient):
    response = run(client, "ols", ["AAPL"])
    assert response.status_code == 422


def test_pair_test_rejects_three_symbols(client: TestClient):
    response = run(client, "engle_granger", ["AAPL", "BTC-USD", "EMPTY-USD"])
    assert response.status_code == 422
    assert "exactly 2" in response.json()["detail"]


def test_multi_test_rejects_one_symbol(client: TestClient):
    response = run(client, "pca", ["AAPL"])
    assert response.status_code == 422
    assert "at least 2" in response.json()["detail"]


def test_unknown_symbol_is_404(client: TestClient):
    assert run(client, "adf", ["NOPE"]).status_code == 404


def test_symbol_with_no_bars_is_422(client: TestClient):
    response = run(client, "adf", ["EMPTY-USD"])
    assert response.status_code == 422
    assert "No bars stored" in response.json()["detail"]


def test_unknown_param_name_is_rejected_not_ignored(client: TestClient):
    response = run(client, "pca", ["AAPL", "BTC-USD"], params={"components": 2})
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


def test_start_after_end_is_422(client: TestClient):
    response = run(client, "adf", ["AAPL"], start="2024-12-31", end="2024-01-01")
    assert response.status_code == 422


def test_too_few_overlapping_observations_is_422(client: TestClient):
    """
    A single day is one observation, which no test can use. The range is one
    day, not two: the fixture generates calendar-daily bars, so 2024-01-01..02
    yields exactly 2 observations and clears the guard — an earlier version of
    this test used that range and asserted a 422 it never got.
    """
    response = run(client, "adf", ["AAPL"], start="2024-01-01", end="2024-01-01")
    assert response.status_code == 422
    assert "observation" in response.json()["detail"]


def test_returns_conversion_can_leave_too_few_observations(client: TestClient):
    """
    Two price bars become one return, which is below the threshold — so a range
    that is just usable for a price test is not for a returns test.
    """
    assert run(client, "adf", ["AAPL"], start="2024-01-01", end="2024-01-02").status_code == 200
    response = run(client, "ols", ["AAPL", "BTC-USD"], start="2024-01-01", end="2024-01-02")
    assert response.status_code == 422
