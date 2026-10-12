"""
The Sharpe ratio every strategy endpoint reports is in excess of the stored
risk-free rate over the window (api/risk_free.py), not of 0.

Until 2026-10-12 PerformanceAnalyzer computed CAGR / volatility, a Sharpe
against a zero rate, while the portfolio optimizer used 0.02: two pages
ranking by "Sharpe" measured different things. Each test here was checked
failing against the zero-rate analyzer.

The conftest rate series quotes 2.00% bank discount (2.0305% bond-equivalent)
on every weekday, so a zero-rate Sharpe cannot pass by accident.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.risk_free import get_rate_repo
from core.rates import discount_to_bond_equivalent
from tests.api.conftest import FakeRates

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
IRX_2PCT = discount_to_bond_equivalent(2.0)


def metric_sets(path: str, body: dict) -> list:
    """Every metrics dict a response carries that must obey the rate."""
    if path == "/api/v1/backtest/portfolio":
        return [body["metrics"]]
    if path == "/api/v1/backtest/compare":
        return [row["metrics"] for row in body["results"]] + [body["benchmark"]["metrics"]]
    if path == "/api/v1/optimize/strategy":
        return [body["best_metrics"], *body["results"]]
    if path == "/api/v1/simulate":
        return [body["historical"]]
    return [body["metrics"]]


ENDPOINTS = {
    "backtest": (
        "/api/v1/backtest",
        {
            "symbol": "AAPL",
            # Buy and hold: invested every bar, so the volatility is never 0.
            "strategy_id": "buy_and_hold",
            **WINDOW,
        },
    ),
    "compare": (
        "/api/v1/backtest/compare",
        {
            "symbol": "AAPL",
            "strategies": [{"strategy_id": "ma_crossover"}, {"strategy_id": "mean_reversion"}],
            "benchmark_symbol": "MSFT",
            **WINDOW,
        },
    ),
    "optimize": (
        "/api/v1/optimize/strategy",
        {
            "symbol": "AAPL",
            "strategy_id": "ma_crossover",
            "grid": {"short_window": [5, 10], "long_window": [30, 40]},
            **WINDOW,
        },
    ),
    "portfolio_backtest": (
        "/api/v1/backtest/portfolio",
        {"symbols": ["AAPL", "MSFT"], "strategy_id": "basket_trading", **WINDOW},
    ),
    "simulate": (
        "/api/v1/simulate",
        {"symbol": "AAPL", "strategy_id": "buy_and_hold", "n_paths": 50, **WINDOW},
    ),
}


def post(client: TestClient, name: str, **overrides):
    path, body = ENDPOINTS[name]
    return path, client.post(path, json={**body, **overrides})


def assert_sharpe_is_excess_of(metrics: dict, rate: float) -> None:
    vol = metrics["Annualized Volatility"]
    expected = (metrics["Annualized Return"] - rate) / vol
    assert metrics["Sharpe Ratio"] == pytest.approx(expected, rel=1e-9)


@pytest.mark.parametrize("name", list(ENDPOINTS))
def test_sharpe_is_in_excess_of_the_stored_rate(client: TestClient, name: str):
    path, response = post(client, name)
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["risk_free_rate"] == pytest.approx(IRX_2PCT)
    assert body["risk_free_source"].startswith("^IRX bond-equivalent, mean over")
    checked = [m for m in metric_sets(path, body) if m.get("Annualized Volatility")]
    assert checked, "no metrics with a volatility to check: the fixture exercises nothing"
    for metrics in checked:
        assert_sharpe_is_excess_of(metrics, body["risk_free_rate"])


@pytest.mark.parametrize("name", list(ENDPOINTS))
def test_a_supplied_rate_is_used_and_the_store_is_never_read(
    client: TestClient, rates: FakeRates, name: str
):
    path, response = post(client, name, risk_free_rate=0.035)
    body = response.json()
    assert body["risk_free_rate"] == 0.035
    assert body["risk_free_source"] == "supplied in the request"
    assert rates.calls == 0
    for metrics in metric_sets(path, body):
        if metrics.get("Annualized Volatility"):
            assert_sharpe_is_excess_of(metrics, 0.035)


@pytest.mark.parametrize("name", list(ENDPOINTS))
def test_no_stored_rate_is_422_naming_the_override(client: TestClient, name: str):
    app.dependency_overrides[get_rate_repo] = lambda: FakeRates(None)
    _path, response = post(client, name)
    assert response.status_code == 422
    assert "risk_free_rate" in response.json()["detail"]


def test_a_window_starting_before_the_series_is_422(client: TestClient):
    app.dependency_overrides[get_rate_repo] = lambda: FakeRates(2.0, first=date(2024, 6, 3))
    _path, response = post(client, "backtest")
    assert response.status_code == 422
    assert "2024-06-03" in response.json()["detail"]


def test_the_rate_moves_sharpe_and_nothing_else(client: TestClient):
    """
    The rate enters the metrics only, never the run: every other KPI and the
    whole equity curve are identical at 0.5% and 8%.
    """
    app.dependency_overrides[get_rate_repo] = lambda: FakeRates(0.5)
    _path, low = post(client, "backtest")
    app.dependency_overrides[get_rate_repo] = lambda: FakeRates(8.0)
    _path, high = post(client, "backtest")
    low, high = low.json(), high.json()

    assert low["equity_curve"] == high["equity_curve"]
    assert low["trades"] == high["trades"]
    others = set(low["metrics"]) - {"Sharpe Ratio"}
    assert {k: low["metrics"][k] for k in others} == {k: high["metrics"][k] for k in others}
    assert low["metrics"]["Sharpe Ratio"] > high["metrics"]["Sharpe Ratio"]


def test_compare_ranks_every_row_and_the_benchmark_on_one_rate(client: TestClient):
    path, response = post(client, "compare")
    body = response.json()
    rate = body["risk_free_rate"]
    checked = [m for m in metric_sets(path, body) if m.get("Annualized Volatility")]
    assert body["benchmark"]["metrics"] in checked
    for metrics in checked:
        assert_sharpe_is_excess_of(metrics, rate)


# --- websockets ----------------------------------------------------------------


WS = {
    "/api/v1/ws/backtest": ENDPOINTS["backtest"][1],
    "/api/v1/ws/simulate": ENDPOINTS["simulate"][1],
    "/api/v1/ws/optimize/strategy": ENDPOINTS["optimize"][1],
}


def ws_run(ws_client: TestClient, path: str, **overrides) -> dict:
    with ws_client.websocket_connect(path) as ws:
        ws.send_json({**WS[path], **overrides})
        while True:
            message = ws.receive_json()
            if message["type"] in {"result", "error"}:
                return message


@pytest.mark.parametrize("path", list(WS))
def test_websocket_results_carry_the_window_rate(ws_client: TestClient, path: str):
    result = ws_run(ws_client, path)
    assert result["type"] == "result", result
    assert result["risk_free_rate"] == pytest.approx(IRX_2PCT)
    assert result["risk_free_source"].startswith("^IRX bond-equivalent")
    metrics = result.get("metrics") or result.get("historical") or result["best_metrics"]
    assert_sharpe_is_excess_of(metrics, result["risk_free_rate"])


@pytest.mark.parametrize("path", list(WS))
def test_websocket_without_a_stored_rate_is_an_error(
    ws_client: TestClient, rates: FakeRates, path: str
):
    rates.quoted_pct = None
    error = ws_run(ws_client, path)
    assert error["type"] == "error"
    assert error["code"] == 422
    assert "risk_free_rate" in error["detail"]
