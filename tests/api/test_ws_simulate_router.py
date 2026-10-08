"""
WS /api/v1/ws/simulate

No OpenAPI document covers a socket, so these tests are the contract for the
message shapes a client codes against — and the only place the per-path
progress from inside the threadpool worker is exercised.

Phase 3 of research/monte-carlo-plan-2026-10-07.md.
"""

from fastapi.testclient import TestClient

REQUEST = {
    "symbol": "AAPL",
    "strategy_id": "mean_reversion",
    "params": {"window": 10, "threshold": 0.5},
    "start": "2024-01-01",
    "end": "2024-12-31",
    "n_paths": 50,
    "seed": 42,
}


def drain(ws) -> list:
    messages = []
    while True:
        message = ws.receive_json()
        messages.append(message)
        if message["type"] in {"result", "error"}:
            return messages


def run(ws_client: TestClient, **overrides) -> list:
    with ws_client.websocket_connect("/api/v1/ws/simulate") as ws:
        ws.send_json({**REQUEST, **overrides})
        return drain(ws)


def test_message_sequence_and_result_shape(ws_client: TestClient):
    messages = run(ws_client)
    types = [m["type"] for m in messages]
    assert types[0] == "accepted"
    assert types[-1] == "result"
    assert all(t == "progress" for t in types[1:-1])
    accepted = messages[0]
    assert accepted["strategy_name"] == "Mean Reversion"
    assert accepted["mode"] == "returns" and accepted["n_paths"] == 50
    result = messages[-1]
    assert len(result["bands"]) == result["horizon_days"] + 1
    assert result["bands"][0]["p50"] == 100_000.0
    assert "depend on the path" in result["caveat"]
    assert isinstance(result["start"], str)  # JSON-mode dump, not datetime


def test_socket_result_matches_rest_for_the_same_request(ws_client: TestClient, client: TestClient):
    over_socket = run(ws_client)[-1]
    over_rest = client.post("/api/v1/simulate", json=REQUEST).json()
    for key in ("bands", "terminal", "drawdown", "risk", "historical", "resampled_from"):
        assert over_socket[key] == over_rest[key], key


def test_progress_percentages_never_move_backwards(ws_client: TestClient):
    pcts = [m["pct"] for m in run(ws_client) if m["type"] == "progress"]
    assert pcts and pcts == sorted(pcts)
    assert pcts[-1] <= 100


def test_prices_mode_reports_per_path_progress_from_the_worker(ws_client: TestClient):
    """
    The bridge coalesces, so not every PROGRESS_EVERY-th update need arrive —
    but the final one is flushed before the bridge closes, so `completed ==
    total` must appear, and it must come from the worker (it carries counts).
    """
    messages = run(ws_client, mode="prices", n_paths=50, horizon_days=40)
    counted = [m for m in messages if m["type"] == "progress" and "completed" in m]
    assert counted, "no per-path progress reached the socket"
    assert counted[-1]["completed"] == counted[-1]["total"] == 50
    assert all(10 <= m["pct"] <= 95 for m in counted)
    assert messages[-1]["type"] == "result"
    assert messages[-1]["mode"] == "prices"


def test_returns_mode_has_no_per_path_progress(ws_client: TestClient):
    messages = run(ws_client, mode="returns")
    assert not [m for m in messages if m["type"] == "progress" and "completed" in m]
    assert messages[-1]["type"] == "result"


def test_errors_use_the_error_message(ws_client: TestClient):
    assert run(ws_client, symbol="NOPE")[-1] == {"type": "error", "code": 404, "detail": "Unknown symbol: 'NOPE'"}
    assert run(ws_client, strategy_id="nope")[-1]["code"] == 404
    assert run(ws_client, strategy_id="pairs_trading")[-1]["code"] == 422
    assert run(ws_client, n_paths=10 ** 7)[-1]["code"] == 422


def test_unverified_asset_is_gated_on_the_socket_too(ws_client: TestClient):
    refused = run(ws_client, symbol="BTC-USD")[-1]
    assert refused["type"] == "error" and refused["code"] == 422
    assert "allow_unverified" in refused["detail"]
    allowed = run(ws_client, symbol="BTC-USD", allow_unverified=True)[-1]
    assert allowed["type"] == "result" and "not read-time adjusted" in allowed["caveat"]


def test_never_invested_strategy_errors_after_accept(ws_client: TestClient):
    messages = run(ws_client, strategy_id="ma_crossover", params={"short_window": 10, "long_window": 30})
    assert messages[0]["type"] == "accepted"
    assert messages[-1]["type"] == "error" and "never invested" in messages[-1]["detail"]


def test_invalid_request_body(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/simulate") as ws:
        ws.send_json({"symbol": "AAPL"})
        message = ws.receive_json()
    assert message["type"] == "error" and message["code"] == 422
