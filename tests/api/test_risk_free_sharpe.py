"""
Strategy backtests credit idle cash with the stored risk-free rate, and every
Sharpe ratio they report is measured against that cash account
(api/risk_free.py, analysis/performance_analyzer.py).

History, all 2026-10-12: PerformanceAnalyzer computed CAGR / volatility, a
Sharpe against 0, while the portfolio optimizer used 0.02. Subtracting ^IRX
alone then charged a strategy sitting in cash for interest the backtester
never paid it (SPY ma_crossover 2023-2025: 0.95 -> -2.71). Now cash earns the
rate bar by bar and Sharpe is the geometric excess over that cash account
divided by the volatility of the excess return, so a strategy that never
trades scores exactly 0.

The conftest rate series quotes 2.00% bank discount (2.0305% bond-equivalent)
on every weekday; the price fixtures are daily, weekends included, so each
bar after the first accrues one day of interest.
"""

import math
from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.risk_free import get_rate_repo
from core.rates import discount_to_bond_equivalent
from tests.api.conftest import FakeRates

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
IRX_2PCT = discount_to_bond_equivalent(2.0)
CAPITAL = 100_000.0
#: Both averages longer than the window: no signal at all, so the run is all
#: cash. (A short window that fits DOES signal, even with no long average.)
NEVER_TRADES = {"short_window": 400, "long_window": 900}


def metric_sets(path: str, body: dict) -> list:
    """Every metrics dict a response carries."""
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
        # Buy and hold: invested every bar, so the volatility is never 0.
        {"symbol": "AAPL", "strategy_id": "buy_and_hold", **WINDOW},
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


def daily_accrual(times: list, annual: float) -> np.ndarray:
    days = [(b.date() - a.date()).days for a, b in zip(times, times[1:], strict=False)]
    return np.concatenate([[0.0], annual * np.array(days, dtype=float) / 365.0])


def expected_sharpe(curve: list, annual: float, initial: float = CAPITAL) -> float:
    """The definition, recomputed from a returned equity curve."""
    times = [datetime.fromisoformat(p["time"]) for p in curve]
    totals = np.array([p["total"] for p in curve])
    accrual = daily_accrual(times, annual)
    returns = pd.Series(totals).pct_change().fillna(0).to_numpy()
    days = (times[-1] - times[0]).days
    excess = ((totals[-1] / initial) / np.prod(1.0 + accrual)) ** (365.0 / days) - 1
    # Annualized on the curve's own calendar (decision 38), restated here rather
    # than imported: 365 when >= 10% of its bars fall on a weekend, else 252.
    # The fixture has a bar every calendar day, so it is 365.
    weekend_share = sum(t.weekday() >= 5 for t in times) / len(times)
    bars_per_year = 365 if weekend_share >= 0.10 else 252
    return excess / (pd.Series(returns - accrual).std() * math.sqrt(bars_per_year))


# --- every endpoint ------------------------------------------------------------


@pytest.mark.parametrize("name", list(ENDPOINTS))
def test_every_endpoint_reports_the_window_rate(client: TestClient, name: str):
    _path, response = post(client, name)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["risk_free_rate"] == pytest.approx(IRX_2PCT)
    assert body["risk_free_source"].startswith("^IRX bond-equivalent, mean over")


@pytest.mark.parametrize("name", list(ENDPOINTS))
def test_a_supplied_rate_is_used_and_the_store_is_never_read(
    client: TestClient, rates: FakeRates, name: str
):
    _path, response = post(client, name, risk_free_rate=0.035)
    body = response.json()
    assert body["risk_free_rate"] == 0.035
    assert body["risk_free_source"] == "supplied in the request"
    assert rates.calls == 0


@pytest.mark.parametrize("name", list(ENDPOINTS))
def test_a_zero_rate_reproduces_the_old_ratio_exactly(client: TestClient, name: str):
    """
    risk_free_rate=0: cash earns nothing and Sharpe is the old CAGR / vol, bit
    for bit — the regression check that the loop is otherwise unchanged.
    """
    path, response = post(client, name, risk_free_rate=0.0)
    checked = [m for m in metric_sets(path, response.json()) if m.get("Annualized Volatility")]
    assert checked, "no metrics with a volatility: the fixture exercises nothing"
    for metrics in checked:
        assert (
            metrics["Sharpe Ratio"]
            == metrics["Annualized Return"] / metrics["Annualized Volatility"]
        )


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


# --- the cash account ----------------------------------------------------------


def test_idle_cash_compounds_at_the_stored_rate(client: TestClient):
    _path, response = post(client, "backtest", strategy_id="ma_crossover", params=NEVER_TRADES)
    body = response.json()
    assert body["trades"] == []
    curve = body["equity_curve"]
    times = [datetime.fromisoformat(p["time"]) for p in curve]
    grown = CAPITAL * np.cumprod(1.0 + daily_accrual(times, IRX_2PCT))
    assert [p["cash"] for p in curve] == pytest.approx(list(grown), rel=1e-12)
    assert curve[-1]["total"] > CAPITAL


def test_a_strategy_that_never_trades_scores_zero(client: TestClient):
    """
    It IS the cash account. Subtracting a scalar rate from its CAGR instead
    gave it a Sharpe of order 1 from compounding and weekend interest.
    """
    _path, response = post(client, "backtest", strategy_id="ma_crossover", params=NEVER_TRADES)
    metrics = response.json()["metrics"]
    assert metrics["Total Return"] > 0
    assert metrics["Sharpe Ratio"] == 0.0


def test_backtest_sharpe_is_excess_over_the_cash_account(client: TestClient):
    _path, response = post(client, "backtest")
    body = response.json()
    assert body["metrics"]["Sharpe Ratio"] == pytest.approx(
        expected_sharpe(body["equity_curve"], IRX_2PCT), rel=1e-9
    )


def test_compare_rows_and_benchmark_are_excess_over_the_cash_account(client: TestClient):
    _path, response = post(client, "compare")
    body = response.json()
    # Rows that traded: a row left in cash is the cash account, whose excess
    # is float noise the analyzer reports as 0 (test_..._scores_zero).
    rows = [r for r in body["results"] if r["metrics"].get("Trade Count")]
    assert rows
    for row in [*rows, body["benchmark"]]:
        assert row["metrics"]["Sharpe Ratio"] == pytest.approx(
            expected_sharpe(row["equity_curve"], IRX_2PCT), rel=1e-9
        )


def test_portfolio_backtest_sharpe_is_excess_over_the_cash_account(client: TestClient):
    _path, response = post(client, "portfolio_backtest")
    body = response.json()
    assert body["metrics"]["Sharpe Ratio"] == pytest.approx(
        expected_sharpe(body["equity_curve"], IRX_2PCT), rel=1e-9
    )


def test_portfolio_backtest_credits_its_cash(client: TestClient):
    """A basket never fully invests (whole shares), so its cash must grow."""
    low = post(client, "portfolio_backtest", risk_free_rate=0.0)[1].json()
    high = post(client, "portfolio_backtest", risk_free_rate=0.08)[1].json()
    assert high["equity_curve"][-1]["total"] > low["equity_curve"][-1]["total"]


def test_simulated_paths_earn_interest_on_cash(client: TestClient):
    """Prices mode re-runs the strategy per path; its cash accrues too."""
    common = {
        "strategy_id": "ma_crossover",
        "params": NEVER_TRADES,
        "mode": "prices",
        "n_paths": 5,
        "horizon_days": 30,
        "include_paths": 5,
    }
    flat = post(client, "simulate", **common, risk_free_rate=0.0)[1].json()
    earning = post(client, "simulate", **common, risk_free_rate=0.05)[1].json()
    assert all(path[-1] == CAPITAL for path in flat["sample_paths"])
    assert all(path[-1] > CAPITAL for path in earning["sample_paths"])


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


def test_websocket_backtest_matches_rest(ws_client: TestClient, client: TestClient):
    """Same accrual through both transports, so the same Sharpe."""
    rest = post(client, "backtest")[1].json()["metrics"]
    socket = ws_run(ws_client, "/api/v1/ws/backtest")["metrics"]
    assert socket["Sharpe Ratio"] == rest["Sharpe Ratio"]


@pytest.mark.parametrize("path", list(WS))
def test_websocket_without_a_stored_rate_is_an_error(
    ws_client: TestClient, rates: FakeRates, path: str
):
    rates.quoted_pct = None
    error = ws_run(ws_client, path)
    assert error["type"] == "error"
    assert error["code"] == 422
    assert "risk_free_rate" in error["detail"]
