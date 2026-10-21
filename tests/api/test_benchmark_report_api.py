"""
The benchmark report on /backtest (and its websocket), /backtest/compare and
/backtest/portfolio (decision 53, research/benchmark-regime-report-2026-10-20.md).

The fixture has no SPY, so an equity run's DEFAULT benchmark is unavailable
and says so; these tests name MSFT (an independent walk) or AAPL itself.
"""

import pytest
from fastapi.testclient import TestClient

from config import settings

WINDOW = {"start": "2024-01-01", "end": "2025-02-03"}


def backtest(client, **overrides):
    body = {"symbol": "AAPL", "strategy_id": "buy_and_hold", **WINDOW, **overrides}
    return client.post("/api/v1/backtest", json=body)


def test_buy_and_hold_against_its_own_asset_is_beta_one(client: TestClient):
    body = backtest(client, benchmark_symbol="aapl").json()
    report = body["benchmark_report"]
    assert body["benchmark_unavailable"] is None
    assert report["symbol"] == "AAPL"
    # Whole shares leave a sliver of cash, so a hair under 1.
    assert 0.99 < report["beta"] <= 1.0
    assert report["r_squared"] > 0.9999
    assert abs(report["alpha"]) < 0.01


def test_the_report_has_regimes_and_a_rolling_sharpe(client: TestClient):
    body = backtest(client, strategy_id="ma_crossover", benchmark_symbol="MSFT").json()
    report = body["benchmark_report"]
    assert report["bars"] == body["bars"] - 1
    assert [r["name"] for r in report["regimes"]] == ["low", "middle", "high"]
    ranked = sum(r["days"] for r in report["regimes"])
    assert ranked > 0 and ranked + report["unranked_days"] == report["bars"]
    window = settings.BENCHMARK_ROLLING_SHARPE_WINDOW
    assert report["rolling_window"] == window
    assert len(report["rolling_sharpe"]) == body["bars"] - window
    assert report["rolling_sharpe"][-1]["time"] == body["equity_curve"][-1]["time"]
    assert report["alpha_low"] < report["alpha"] < report["alpha_high"]


def test_without_the_default_benchmark_the_report_says_why(client: TestClient):
    body = backtest(client).json()
    assert body["benchmark_report"] is None
    assert "SPY" in body["benchmark_unavailable"]
    assert body["metrics"]["Sharpe Ratio"] is not None


def test_a_crypto_run_defaults_to_btc(client: TestClient):
    body = backtest(client, symbol="BTC-USD").json()
    assert body["benchmark_report"]["symbol"] == "BTC-USD"


def test_a_named_benchmark_must_exist(client: TestClient):
    assert backtest(client, benchmark_symbol="NOPE").status_code == 404
    assert backtest(client, benchmark_symbol="EMPTY-USD").status_code == 422


def test_the_report_can_be_left_out(client: TestClient):
    body = backtest(client, benchmark_symbol="MSFT", include_benchmark_report=False).json()
    assert body["benchmark_report"] is None and body["benchmark_unavailable"] is None


def test_the_websocket_carries_the_same_report(ws_client: TestClient, client: TestClient):
    request = {
        "symbol": "AAPL",
        "strategy_id": "ma_crossover",
        **WINDOW,
        "benchmark_symbol": "MSFT",
    }
    rest = client.post("/api/v1/backtest", json=request).json()["benchmark_report"]
    with ws_client.websocket_connect("/api/v1/ws/backtest") as ws:
        ws.send_json(request)
        while (message := ws.receive_json())["type"] not in {"result", "error"}:
            pass
    assert message["type"] == "result", message
    socket = message["benchmark_report"]
    assert socket["beta"] == pytest.approx(rest["beta"])
    assert socket["regimes"] == rest["regimes"]
    assert len(socket["rolling_sharpe"]) == len(rest["rolling_sharpe"])


def test_each_compared_row_has_its_own_report(client: TestClient):
    body = client.post(
        "/api/v1/backtest/compare",
        json={
            "symbol": "AAPL",
            **WINDOW,
            "strategies": [{"strategy_id": "buy_and_hold"}, {"strategy_id": "ma_crossover"}],
            "benchmark_symbol": "AAPL",
        },
    ).json()
    rows = {r["strategy_id"]: r["benchmark_report"] for r in body["results"]}
    assert rows["buy_and_hold"]["beta"] > 0.99
    assert rows["ma_crossover"]["beta"] < rows["buy_and_hold"]["beta"]
    assert body["benchmark"]["symbol"] == "AAPL"


def test_a_basket_is_measured_against_its_benchmark(client: TestClient):
    body = client.post(
        "/api/v1/backtest/portfolio",
        json={
            "symbols": ["AAPL", "MSFT"],
            "strategy_id": "index_rebalancing",
            **WINDOW,
            "benchmark_symbol": "MSFT",
        },
    ).json()
    report = body["benchmark_report"]
    assert report["symbol"] == "MSFT"
    # Half in MSFT, half in a series independent of it.
    assert 0.3 < report["beta"] < 0.7
    default = client.post(
        "/api/v1/backtest/portfolio",
        json={"symbols": ["AAPL", "MSFT"], "strategy_id": "index_rebalancing", **WINDOW},
    ).json()
    assert default["benchmark_report"] is None and "SPY" in default["benchmark_unavailable"]
