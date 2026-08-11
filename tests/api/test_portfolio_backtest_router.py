"""
POST /api/v1/backtest/portfolio

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
PAIR = ["AAPL", "BTC-USD"]


def run(client: TestClient, **overrides):
    body = {
        "symbols": PAIR,
        "strategy_id": "basket_trading",
        **WINDOW,
        **overrides,
    }
    return client.post("/api/v1/backtest/portfolio", json=body)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_runs_a_multi_asset_strategy(client: TestClient):
    body = run(client).json()
    assert body["strategy_name"] == "Basket Trading"
    assert body["symbols"] == PAIR
    assert body["bars"] > 0
    assert body["metrics"]


def test_defaults_to_equal_weights(client: TestClient):
    body = run(client).json()
    assert body["weights"] == {"AAPL": 0.5, "BTC-USD": 0.5}


def test_explicit_weights_are_used_and_echoed(client: TestClient):
    weights = {"AAPL": 0.7, "BTC-USD": 0.3}
    body = run(client, weights=weights).json()
    assert body["weights"] == weights


def test_returns_an_equity_curve(client: TestClient):
    body = run(client).json()
    assert len(body["equity_curve"]) == body["bars"]
    assert set(body["equity_curve"][0]) == {"time", "total"}


def test_metrics_only_response_omits_the_curve(client: TestClient):
    body = run(client, include_equity_curve=False).json()
    assert body["equity_curve"] == []
    assert body["metrics"]


def test_risk_metrics_are_computed_by_default(client: TestClient):
    """
    Streamlit's portfolio view showed VaR/CVaR alongside the backtest, computed
    from the resulting returns. Including them here avoids a second round trip
    for what is always wanted together.
    """
    body = run(client).json()
    assert body["risk_metrics"]
    assert any("VaR" in key or "Var" in key for key in body["risk_metrics"])


def test_risk_metrics_can_be_skipped(client: TestClient):
    assert run(client, include_risk=False).json()["risk_metrics"] == {}


def test_echoes_initial_capital_and_seed(client: TestClient):
    body = run(client, initial_capital=250_000.0).json()
    assert body["initial_capital"] == 250_000.0
    assert body["seed"] == 42


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_same_seed_gives_identical_results(client: TestClient):
    """
    PortfolioBacktester constructed its execution handler without a seed, so
    identical portfolio backtests returned different numbers — the same defect
    already fixed for the single-symbol Backtester, and the same consequence:
    saved results could not be reproduced.
    """
    first = run(client, include_equity_curve=False, seed=42).json()
    second = run(client, include_equity_curve=False, seed=42).json()
    assert first["metrics"] == second["metrics"]


def test_different_seeds_can_differ(client: TestClient):
    finals = {
        run(client, include_equity_curve=False, seed=seed).json()["metrics"]["Final Value"]
        for seed in (1, 2, 3, 4, 5)
    }
    assert len(finals) > 1


# ---------------------------------------------------------------------------
# Strategy contract
# ---------------------------------------------------------------------------

def test_single_asset_strategy_is_rejected(client: TestClient):
    """
    The mirror of the single-symbol endpoint's multi rejection. Between them,
    every strategy has exactly one endpoint that accepts it.
    """
    response = run(client, strategy_id="ma_crossover")
    assert response.status_code == 422
    assert "single-asset" in response.json()["detail"]


def test_pairs_trading_requires_exactly_two_symbols(client: TestClient):
    response = run(
        client, strategy_id="pairs_trading", symbols=["AAPL", "BTC-USD", "EMPTY-USD"]
    )
    assert response.status_code == 422
    assert "exactly 2" in response.json()["detail"]


def test_pairs_trading_runs_on_a_pair(client: TestClient):
    body = run(client, strategy_id="pairs_trading", symbols=PAIR).json()
    assert body["strategy_id"] == "pairs_trading"
    assert body["bars"] > 0


def test_cointegrated_without_weights_is_422_not_500(client: TestClient):
    """
    Cointegrated Mean Reversion needs a Johansen `weights` mapping the registry
    cannot default, so building it raises TypeError. That is a client error —
    it must not surface as an unhandled 500.
    """
    response = run(client, strategy_id="cointegrated_mean_reversion")
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_one_symbol_is_422(client: TestClient):
    response = run(client, symbols=["AAPL"])
    assert response.status_code == 422
    assert "at least 2" in response.json()["detail"]


def test_partial_weights_are_rejected(client: TestClient):
    """
    A weights mapping missing a symbol would silently zero-weight it, producing
    a smaller portfolio than requested while still reporting the full symbol
    list.
    """
    response = run(client, weights={"AAPL": 1.0})
    assert response.status_code == 422
    assert "MSFT" in response.json()["detail"] or "BTC-USD" in response.json()["detail"]


def test_weights_naming_an_unrequested_symbol_are_rejected(client: TestClient):
    response = run(client, weights={"AAPL": 0.5, "BTC-USD": 0.4, "TSLA": 0.1})
    assert response.status_code == 422
    assert "Unexpected" in response.json()["detail"]


def test_unknown_symbol_is_404(client: TestClient):
    assert run(client, symbols=["AAPL", "NOPE"]).status_code == 404


def test_unknown_strategy_is_404(client: TestClient):
    assert run(client, strategy_id="not_a_strategy").status_code == 404


def test_symbol_with_no_bars_is_422(client: TestClient):
    response = run(client, symbols=["AAPL", "EMPTY-USD"])
    assert response.status_code == 422
    assert "No bars stored" in response.json()["detail"]


def test_start_after_end_is_422(client: TestClient):
    assert run(client, start="2024-12-31", end="2024-01-01").status_code == 422


def test_unknown_param_name_is_rejected_not_ignored(client: TestClient):
    response = run(client, strategy_id="pairs_trading", params={"windwo": 5})
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------
# These two dominate the metrics and were previously unreachable from the API,
# so a portfolio backtest reported the risk manager's behaviour rather than the
# strategy's: max_trade_risk_pct=0.02 scales every buy to 2% of equity, and
# max_portfolio_drawdown_pct=0.20 halts buying permanently once tripped.

def test_risk_limits_default_to_the_previous_hardcoded_values(client: TestClient):
    """
    Backward compatibility is the point of exposing rather than changing them:
    an existing request must return exactly what it returned before, so no
    saved result in results/ is retroactively invalidated.
    """
    body = run(client).json()
    assert body["max_trade_risk_pct"] == 0.02
    assert body["max_portfolio_drawdown_pct"] == 0.20


def test_risk_limits_are_echoed_so_a_result_is_interpretable(client: TestClient):
    """
    Same reason `weights` is echoed. Metrics produced under a 2% cap and under
    no cap differ by an order of magnitude, so a stored result without these is
    not interpretable.
    """
    body = run(client, max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=1.0).json()
    assert body["max_trade_risk_pct"] == 1.0
    assert body["max_portfolio_drawdown_pct"] == 1.0


def test_risk_limits_reach_the_risk_manager(monkeypatch, client: TestClient):
    """
    Asserting on the OBJECT the backtester is handed, not on the echo — the
    echo would pass even if the values were dropped on the way through, which
    is exactly the bug worth guarding.
    """
    import backtesting.risk_manager as rm

    captured = {}
    original = rm.PortfolioRiskManager

    def spy(**kwargs):
        captured.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(rm, "PortfolioRiskManager", spy)

    run(client, max_trade_risk_pct=0.5, max_portfolio_drawdown_pct=0.9)

    assert captured == {
        "max_trade_risk_pct": 0.5,
        "max_portfolio_drawdown_pct": 0.9,
    }


def test_a_zero_or_negative_cap_is_422(client: TestClient):
    """A 0% cap would size every trade to nothing and report a flat curve."""
    assert run(client, max_trade_risk_pct=0).status_code == 422
    assert run(client, max_portfolio_drawdown_pct=-0.1).status_code == 422


def test_a_cap_above_one_is_422(client: TestClient):
    """Above 1.0 is not "more permissive", it is meaningless — 1.0 is the max."""
    assert run(client, max_trade_risk_pct=1.5).status_code == 422
    assert run(client, max_portfolio_drawdown_pct=2.0).status_code == 422


# ---------------------------------------------------------------------------
# Allocation strategies, through the real router path
# ---------------------------------------------------------------------------
# These exercise the wide_per_asset shape end to end. It matters that they run
# HERE rather than only in tests/test_strategy_contract.py: this conftest's
# bars carry tz-AWARE timestamps, as TimescaleDB does, while the strategy
# fixtures were tz-naive. A rebalance-date bug that dropped the timezone passed
# every one of 665 strategy-level tests and raised KeyError on the first real
# backtest. Both halves of that gap are now closed — the fixtures are tz-aware,
# and these give the new strategies a tz-aware route through the router.

def test_paired_switching_runs_through_the_router(client: TestClient):
    body = run(
        client,
        strategy_id="paired_switching",
        params={"lookback": 20, "rebalance_frequency": "ME"},
        weights={"AAPL": 1.0, "BTC-USD": 1.0},
    ).json()
    assert body["strategy_name"] == "Paired Switching"
    assert body["metrics"], "a run that produces no metrics has not run"


def test_asset_class_trend_runs_through_the_router(client: TestClient):
    body = run(
        client,
        strategy_id="asset_class_trend",
        params={"window": 50, "rebalance_frequency": "ME"},
    ).json()
    assert body["strategy_name"] == "Asset Class Trend Following"
    assert body["metrics"]


def test_momentum_allocation_runs_through_the_router(client: TestClient):
    body = run(
        client,
        strategy_id="momentum_allocation",
        params={"lookback": 20, "top_n": 1, "rebalance_frequency": "ME"},
    ).json()
    assert body["strategy_name"] == "Momentum Asset Allocation"
    assert body["metrics"]


def test_paired_switching_requires_exactly_two_symbols(client: TestClient):
    """Its own guard, surfaced as a 422 rather than a 500."""
    response = run(
        client,
        symbols=["AAPL", "BTC-USD", "MSFT"],
        strategy_id="paired_switching",
        params={"lookback": 20, "rebalance_frequency": "ME"},
    )
    assert response.status_code == 422
    assert "exactly two" in response.json()["detail"]


def test_momentum_allocation_rejects_holding_every_sleeve(client: TestClient):
    """
    top_n == the number of sleeves means every sleeve is always held and the
    ranking does nothing — a silently pointless backtest.
    """
    response = run(
        client,
        strategy_id="momentum_allocation",
        params={"lookback": 20, "top_n": 2, "rebalance_frequency": "ME"},
    )
    assert response.status_code == 422
    assert "top_n" in response.json()["detail"]
