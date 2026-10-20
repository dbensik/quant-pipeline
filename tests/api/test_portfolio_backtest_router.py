"""
POST /api/v1/backtest/portfolio

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
PAIR = ["AAPL", "BTC-USD"]
#: Shorts are equities and ETFs only; AAPL and MSFT follow independent paths.
EQUITY_PAIR = ["AAPL", "MSFT"]
TERMS = {"borrow_rate": 0.01, "max_gross": 2.0, "maintenance": 0.3}


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
    response = run(client, strategy_id="pairs_trading", symbols=["AAPL", "MSFT", "AAPL"], **TERMS)
    assert response.status_code == 422
    assert "exactly 2" in response.json()["detail"]


def test_pairs_trading_runs_on_a_pair(client: TestClient):
    body = run(client, strategy_id="pairs_trading", symbols=EQUITY_PAIR, **TERMS).json()
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


def test_a_negative_weight_is_rejected(client: TestClient):
    """
    A negative weight reached the rebalance branch, which SOLD from flat: a
    short that spent its proceeds as cash, with no collateral, borrow or
    margin (decision 47). Shorts wait for an engine that accounts for them.
    """
    response = run(client, weights={"AAPL": 1.3, "MSFT": -0.3}, symbols=["AAPL", "MSFT"])
    assert response.status_code == 422
    assert "MSFT" in response.json()["detail"]
    assert "negative" in response.json()["detail"]


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
    response = run(
        client, strategy_id="pairs_trading", symbols=EQUITY_PAIR, params={"windwo": 5}, **TERMS
    )
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------
# These two dominate the metrics and were previously unreachable from the API,
# so a portfolio backtest reported the risk manager's behaviour rather than the
# strategy's: max_trade_risk_pct=0.02 scales every buy to 2% of equity, and
# max_portfolio_drawdown_pct=0.20 halts buying permanently once tripped.


def test_risk_limits_default_to_off(client: TestClient):
    """
    Decision 32 (2026-10-15): the 2% buy cap and 20% drawdown halt are opt-in.
    As defaults they left trend following on five ETFs 7.6% invested, so a
    default run reported the risk manager, not the strategy. They had been
    kept for backward compatibility; the one saved result in results/ is a
    weight optimization, not a portfolio backtest, so nothing stored changes.
    """
    body = run(client).json()
    assert body["max_trade_risk_pct"] == 1.0
    assert body["max_portfolio_drawdown_pct"] == 1.0


def test_a_default_run_is_sized_by_its_weights_not_the_risk_manager(client: TestClient):
    """Same result as the limits switched off explicitly; not the capped one."""
    trend = {"strategy_id": "asset_class_trend", "params": {"window": 50}}
    default = run(client, **trend).json()["metrics"]["Final Value"]
    off = run(client, **trend, max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=1.0)
    capped = run(client, **trend, max_trade_risk_pct=0.02, max_portfolio_drawdown_pct=0.20)
    assert default == off.json()["metrics"]["Final Value"]
    assert default != capped.json()["metrics"]["Final Value"]


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


def test_paired_switching_defaults_each_leg_to_full_weight(client: TestClient):
    """One leg is held at a time; equal weights left it 50% in cash (decision 32)."""
    body = run(client, strategy_id="paired_switching", params={"lookback": 20}).json()
    assert body["weights"] == {"AAPL": 1.0, "BTC-USD": 1.0}


def test_momentum_defaults_each_sleeve_to_one_over_top_n(client: TestClient):
    """It holds top_n at a time; equal weights left it 30-42% invested (decision 32)."""
    body = run(
        client,
        symbols=["AAPL", "BTC-USD", "MSFT"],
        strategy_id="momentum_allocation",
        params={"lookback": 20, "top_n": 2},
    ).json()
    assert body["weights"] == {"AAPL": 0.5, "BTC-USD": 0.5, "MSFT": 0.5}


def test_trend_defaults_to_equal_weights(client: TestClient):
    """Every sleeve may be held together, so 1/N is right for trend."""
    body = run(client, strategy_id="asset_class_trend", params={"window": 50}).json()
    assert body["weights"] == {"AAPL": 0.5, "BTC-USD": 0.5}


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


def test_transaction_cost_is_charged(client: TestClient):
    """The portfolio endpoint had no cost input at all before 2026-10-12."""
    free = run(client, transaction_cost=0.0, risk_free_rate=0.0).json()
    dear = run(client, transaction_cost=0.01, risk_free_rate=0.0).json()
    assert dear["transaction_cost"] == 0.01
    assert dear["metrics"]["Final Value"] < free["metrics"]["Final Value"]


def test_a_basket_holds_a_fraction_of_a_coin(client: TestClient):
    """
    AAPL and BTC-USD share one return path in this fixture, so the basket's
    return should barely depend on its capital. The risk manager caps each
    buy at 2% of equity: on $20,000 that is $400, about 0.013 of a ~$30,000
    BTC, which whole units rounded to 0, so BTC was never bought and the
    basket earned 0.44 of a $2,000,000 one's return. Now ~0.89; the rest is
    the $1-a-fill fee, 100x heavier on the smaller basket.
    """
    small = run(client, initial_capital=20_000, risk_free_rate=0.0).json()["metrics"]
    large = run(client, initial_capital=2_000_000, risk_free_rate=0.0).json()["metrics"]
    assert small["Total Return"] > 0.8 * large["Total Return"] > 0


# ---------------------------------------------------------------------------
# Shorts (decision 48)
# ---------------------------------------------------------------------------
# pairs_trading is the strategy that shorts. These parameters make it trade
# on the fixture pair within the year: a short formation, frequent refits and
# a loose p-value, since AAPL and MSFT follow unrelated paths.
TRADING = {"formation": 60, "refit": 5, "entry": 1.0, "max_pvalue": 0.5}


def test_a_pairs_run_reports_its_shorts_and_the_terms_it_used(client: TestClient):
    body = run(
        client, strategy_id="pairs_trading", symbols=EQUITY_PAIR, params=TRADING, **TERMS
    ).json()
    assert body["short_terms"] == {**TERMS, "source": "request"}
    exposure = body["short_exposure"]
    assert exposure["time_short"] > 0
    assert exposure["borrow_paid"] > 0
    assert 0 < exposure["max_gross_exposure"] <= TERMS["max_gross"]
    assert body["weights"] == {}


def test_terms_fall_back_to_the_settings(client: TestClient, monkeypatch):
    from config import settings

    monkeypatch.setattr(settings, "SHORT_BORROW_RATE", 0.02)
    monkeypatch.setattr(settings, "SHORT_MAX_GROSS", 1.5)
    monkeypatch.setattr(settings, "SHORT_MAINTENANCE", 0.35)
    body = run(client, strategy_id="pairs_trading", symbols=EQUITY_PAIR).json()
    assert body["short_terms"] == {
        "borrow_rate": 0.02,
        "max_gross": 1.5,
        "maintenance": 0.35,
        "source": "settings",
    }
    mixed = run(client, strategy_id="pairs_trading", symbols=EQUITY_PAIR, borrow_rate=0.05).json()
    assert mixed["short_terms"]["borrow_rate"] == 0.05
    assert mixed["short_terms"]["source"] == "mixed"


def test_a_short_run_without_terms_is_refused(client: TestClient):
    response = run(client, strategy_id="pairs_trading", symbols=EQUITY_PAIR, borrow_rate=0.01)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "max_gross" in detail and "maintenance" in detail
    assert "borrow_rate" not in detail.split("no ")[1].split(" is set")[0]


def test_terms_that_cannot_hold_are_refused(client: TestClient):
    response = run(
        client,
        strategy_id="pairs_trading",
        symbols=EQUITY_PAIR,
        **{**TERMS, "max_gross": 4.0},
    )
    assert response.status_code == 422
    assert "maintenance" in response.json()["detail"]


def test_crypto_cannot_be_shorted(client: TestClient):
    response = run(client, strategy_id="pairs_trading", symbols=["AAPL", "BTC-USD"], **TERMS)
    assert response.status_code == 422
    assert "BTC-USD is crypto" in response.json()["detail"]


def test_terms_on_a_strategy_that_does_not_short_are_refused(client: TestClient):
    response = run(client, borrow_rate=0.01)
    assert response.status_code == 422
    assert "does not" in response.json()["detail"]


def test_weights_on_a_strategy_that_sizes_its_legs_are_refused(client: TestClient):
    response = run(
        client,
        strategy_id="pairs_trading",
        symbols=EQUITY_PAIR,
        weights={"AAPL": 0.5, "MSFT": 0.5},
        **TERMS,
    )
    assert response.status_code == 422
    assert "weights" in response.json()["detail"]


def test_an_event_run_reports_no_short_terms(client: TestClient):
    body = run(client).json()
    assert body["short_terms"] is None
    assert body["short_exposure"] is None
