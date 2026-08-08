"""
WS /api/v1/ws/backtest

The websocket protocol is the one part of the API surface with no OpenAPI
document, so the frontend's types in `frontend/src/api/ws.ts` are hand-written
against ws.py's docstring and nothing machine-checks them. These tests are the
closest thing to a contract check: they assert the message shapes that ws.ts
declares.

Phase 3/4 — API router tests
"""

import json

from fastapi.testclient import TestClient

REQUEST = {
    "symbol": "AAPL",
    "strategy_id": "ma_crossover",
    "start": "2024-01-01",
    "end": "2024-12-31",
    "params": {"short_window": 10, "long_window": 30},
    "include_equity_curve": False,
}


def drain(ws) -> list:
    """Collect messages until the run terminates (result or error)."""
    messages = []
    while True:
        message = ws.receive_json()
        messages.append(message)
        if message["type"] in {"result", "error"}:
            return messages


def test_streams_accepted_progress_then_result(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json(REQUEST)
        messages = drain(ws)

    kinds = [m["type"] for m in messages]
    assert kinds[0] == "accepted"
    assert kinds[-1] == "result"
    # Progress must actually stream — a socket that only sends the final result
    # is just a slow POST and defeats the point of the endpoint.
    assert kinds.count("progress") >= 2


def test_progress_messages_match_the_declared_shape(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json(REQUEST)
        messages = drain(ws)

    progress = [m for m in messages if m["type"] == "progress"]
    for message in progress:
        assert message["stage"] in {"fetching", "running", "summarising"}
        assert 0 <= message["pct"] <= 100
        assert message["detail"]
    # Monotonic, so a client can drive a progress bar without it going backwards.
    assert [m["pct"] for m in progress] == sorted(m["pct"] for m in progress)


def test_result_carries_metrics_and_counts(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json(REQUEST)
        result = drain(ws)[-1]

    assert result["symbol"] == "AAPL"
    assert result["strategy_name"] == "Moving Average Crossover"
    assert isinstance(result["trades"], int)

    # 366 == the fake's calendar-daily bars for 2024; the real database holds
    # trading days only (252 for the same window). This pins the data SOURCE,
    # so a future change that let these tests read the live database would show
    # up here as 252 rather than passing quietly.
    assert result["bars"] == 366
    assert result["metrics"]["Final Value"] is not None
    # include_equity_curve=False, so the curve must be absent.
    assert "equity_curve" not in result


def test_equity_curve_is_included_when_requested(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({**REQUEST, "include_equity_curve": True})
        result = drain(ws)[-1]

    assert len(result["equity_curve"]) == result["bars"]
    assert set(result["equity_curve"][0]) == {"time", "total"}


def test_matches_the_rest_endpoint_for_the_same_request(
    ws_client: TestClient, client: TestClient
):
    """
    Two code paths, one answer. ws.py reuses _run_backtest_sync from the REST
    router precisely so they cannot diverge — this asserts that they haven't.
    """
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json(REQUEST)
        streamed = drain(ws)[-1]

    posted = client.post("/api/v1/backtest", json=REQUEST).json()
    assert streamed["metrics"] == posted["metrics"]
    assert streamed["bars"] == posted["bars"]


# ---------------------------------------------------------------------------
# Error paths — the socket must report and close, never hang
# ---------------------------------------------------------------------------

def test_unknown_symbol_yields_an_error_message(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({**REQUEST, "symbol": "NOPE"})
        final = drain(ws)[-1]
    assert final["type"] == "error"
    assert final["code"] == 404


def test_unknown_strategy_yields_an_error_message(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({**REQUEST, "strategy_id": "not_a_strategy"})
        final = drain(ws)[-1]
    assert final["type"] == "error"
    assert final["code"] == 404


def test_multi_asset_strategy_yields_an_error_message(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({**REQUEST, "strategy_id": "pairs_trading", "params": {}})
        final = drain(ws)[-1]
    assert final["type"] == "error"
    assert final["code"] == 422


def test_invalid_strategy_params_yield_an_error_message(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({**REQUEST, "params": {"short_window": 100, "long_window": 50}})
        final = drain(ws)[-1]
    assert final["type"] == "error"
    assert final["code"] == 422


def test_malformed_request_yields_an_error_message(ws_client: TestClient):
    """A request missing required fields must be reported, not crash the socket."""
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({"symbol": "AAPL"})
        final = drain(ws)[-1]
    assert final["type"] == "error"
    assert final["code"] == 422


def test_non_json_payload_yields_an_error_message(ws_client: TestClient):
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_text("this is not json")
        final = drain(ws)[-1]
    assert final["type"] == "error"
    assert final["code"] == 422


def test_error_payloads_are_json_serialisable(ws_client: TestClient):
    """Pydantic's error objects are not JSON-native; a raw dump would 500."""
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json({"symbol": "AAPL"})
        final = drain(ws)[-1]
    json.dumps(final)  # raises if the server sent something unserialisable
