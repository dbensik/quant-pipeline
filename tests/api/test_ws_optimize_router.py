"""
WS /api/v1/ws/optimize/strategy
WS /api/v1/ws/optimize/portfolio

Like the backtest socket, these have no OpenAPI document, so these tests are
the contract check for the message shapes a client codes against.

They also cover the one thing the REST tests cannot: progress reported from
INSIDE the threadpool worker. `await websocket.send_json(...)` is unavailable
there, so _ProgressBridge hands messages back to the event loop via
call_soon_threadsafe. If that bridge is wrong the run still finishes and the
REST-equivalent assertions still pass — only the progress stream disappears.

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

GRID_REQUEST = {
    "symbol": "AAPL",
    "strategy_id": "ma_crossover",
    "start": "2024-01-01",
    "end": "2024-12-31",
    "grid": {"short_window": [5, 10, 15], "long_window": [30, 40]},
    "seed": 42,
}

PORTFOLIO_REQUEST = {
    # AAPL + MSFT: the other fixture pair is perfectly correlated, so no
    # weighting changes the risk. See the note in test_optimize_router.py.
    "symbols": ["AAPL", "MSFT"],
    "start": "2024-01-01",
    "end": "2024-12-31",
    "num_portfolios": 400,
    "seed": 42,
}


def drain(ws) -> list:
    messages = []
    while True:
        message = ws.receive_json()
        messages.append(message)
        if message["type"] in {"result", "error"}:
            return messages


def run_grid(ws_client: TestClient, **overrides) -> list:
    with ws_client.websocket_connect("/api/v1/ws/optimize/strategy") as ws:
        ws.send_json({**GRID_REQUEST, **overrides})
        return drain(ws)


def run_allocation(ws_client: TestClient, **overrides) -> list:
    with ws_client.websocket_connect("/api/v1/ws/optimize/portfolio") as ws:
        ws.send_json({**PORTFOLIO_REQUEST, **overrides})
        return drain(ws)


# ---------------------------------------------------------------------------
# Grid search
# ---------------------------------------------------------------------------

def test_streams_accepted_progress_then_result(ws_client: TestClient):
    kinds = [m["type"] for m in run_grid(ws_client)]
    assert kinds[0] == "accepted"
    assert kinds[-1] == "result"
    assert kinds.count("progress") >= 2


def test_accepted_announces_the_grid_size_before_any_work(ws_client: TestClient):
    """
    A client cannot render "combination 3 of 6" until it knows the 6, and the
    count is known from the request alone — so it is sent up front rather than
    waiting for the first progress message.
    """
    accepted = run_grid(ws_client)[0]
    assert accepted["combinations"] == 6
    assert accepted["strategy_name"] == "Moving Average Crossover"
    assert accepted["metric"] == "Sharpe Ratio"


def test_per_combination_progress_reaches_the_client(ws_client: TestClient):
    """
    THE test for _ProgressBridge. This progress originates in the threadpool
    worker; without the bridge the messages never make it onto the socket and
    the only remaining evidence is their absence.

    Coalescing means fewer than one message per combination may arrive, so the
    assertion is that per-combination counters appear at all and that the last
    one reports completion.
    """
    messages = run_grid(ws_client)
    counted = [m for m in messages if m.get("total")]
    assert counted, "no per-combination progress arrived"
    assert all(m["total"] == 6 for m in counted)
    assert counted[-1]["completed"] == 6


def test_progress_is_monotonic_and_bounded(ws_client: TestClient):
    progress = [m for m in run_grid(ws_client) if m["type"] == "progress"]
    pcts = [m["pct"] for m in progress]
    assert pcts == sorted(pcts)
    assert all(0 <= p <= 100 for p in pcts)
    assert all(m["stage"] in {"fetching", "running", "summarising"} for m in progress)


def test_result_carries_the_same_payload_as_the_rest_route(
    ws_client: TestClient, client: TestClient
):
    """
    Both transports build their payload from build_optimize_payload, so a
    client can share one result type. This asserts they actually agree rather
    than trusting that they do.
    """
    over_socket = run_grid(ws_client)[-1]
    over_http = client.post("/api/v1/optimize/strategy", json=GRID_REQUEST).json()

    assert over_socket["best_params"] == over_http["best_params"]
    assert over_socket["best_metrics"] == over_http["best_metrics"]
    assert over_socket["combinations_evaluated"] == over_http["combinations_evaluated"]
    assert over_socket["results"] == over_http["results"]


def test_invalid_grid_parameter_is_an_error_message_not_a_crash(
    ws_client: TestClient,
):
    message = run_grid(ws_client, grid={"short_windwo": [5, 10]})[-1]
    assert message["type"] == "error"
    assert message["code"] == 422
    assert "Unknown parameter" in message["detail"]


def test_unknown_strategy_is_a_404_error_message(ws_client: TestClient):
    message = run_grid(ws_client, strategy_id="not_a_strategy")[-1]
    assert message["type"] == "error"
    assert message["code"] == 404


def test_unknown_symbol_is_a_404_error_message(ws_client: TestClient):
    message = run_grid(ws_client, symbol="NOPE")[-1]
    assert message["type"] == "error"
    assert message["code"] == 404


def test_entirely_invalid_grid_is_an_error_not_an_empty_result(
    ws_client: TestClient,
):
    message = run_grid(ws_client, grid={"short_window": [50], "long_window": [10]})[-1]
    assert message["type"] == "error"
    assert message["code"] == 422


def test_malformed_request_is_rejected(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/optimize/strategy") as ws:
        ws.send_json({"symbol": "AAPL"})  # no strategy_id, dates or grid
        message = ws.receive_json()
    assert message["type"] == "error"
    assert message["code"] == 422


# ---------------------------------------------------------------------------
# Portfolio Monte Carlo
# ---------------------------------------------------------------------------

def test_portfolio_streams_progress_then_result(ws_client: TestClient):
    kinds = [m["type"] for m in run_allocation(ws_client)]
    assert kinds[0] == "accepted"
    assert kinds[-1] == "result"
    # simulate_random_portfolios calls back every 100 trials; 400 trials plus
    # the final 1.0 is enough that a working bridge delivers several.
    assert kinds.count("progress") >= 2


def test_portfolio_result_carries_both_allocations(ws_client: TestClient):
    result = run_allocation(ws_client)[-1]
    for key in ("max_sharpe", "min_volatility"):
        assert set(result[key]["weights"]) == {"AAPL", "MSFT"}
        assert result[key]["sharpe_ratio"] is not None


def test_portfolio_result_matches_the_rest_route(
    ws_client: TestClient, client: TestClient
):
    over_socket = run_allocation(ws_client)[-1]
    over_http = client.post(
        "/api/v1/optimize/portfolio", json=PORTFOLIO_REQUEST
    ).json()
    assert over_socket["max_sharpe"] == over_http["max_sharpe"]
    assert over_socket["min_volatility"] == over_http["min_volatility"]


def test_portfolio_one_symbol_is_an_error(ws_client: TestClient):
    message = run_allocation(ws_client, symbols=["AAPL"])[-1]
    assert message["type"] == "error"
    assert message["code"] == 422


def test_portfolio_unknown_symbol_is_a_404_error(ws_client: TestClient):
    message = run_allocation(ws_client, symbols=["AAPL", "NOPE"])[-1]
    assert message["type"] == "error"
    assert message["code"] == 404
