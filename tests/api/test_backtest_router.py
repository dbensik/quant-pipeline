"""
POST /api/v1/backtest

Phase 3/4 — API router tests
"""

from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}


def run(client: TestClient, **overrides):
    body = {"symbol": "AAPL", "strategy_id": "buy_and_hold", **WINDOW, **overrides}
    return client.post("/api/v1/backtest", json=body)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_returns_metrics_equity_curve_and_trades(client: TestClient):
    body = run(client, strategy_id="ma_crossover",
               params={"short_window": 10, "long_window": 30}).json()
    assert body["symbol"] == "AAPL"
    assert body["strategy_name"] == "Moving Average Crossover"
    assert body["bars"] > 0
    assert body["metrics"]["Final Value"] is not None
    assert len(body["equity_curve"]) == body["bars"]
    assert body["params"] == {"short_window": 10, "long_window": 30}


def test_omitted_params_fall_back_to_registry_defaults(client: TestClient):
    body = run(client, strategy_id="ma_crossover").json()
    assert body["params"] == {"short_window": 40, "long_window": 100}


def test_trade_count_metric_matches_the_trade_log(client: TestClient):
    """
    PerformanceAnalyzer used to read a "trades" column Backtester never
    produces, so Trade Count was always 0 no matter how much a strategy traded.

    The strategy and parameters here are chosen to TRADE — mean reversion with
    a tight window and low threshold. An earlier version of this test used
    ma_crossover 10/30, which makes zero trades on this fixture, so `0 == 0`
    passed even with the bug reintroduced. The non-zero assertion below is what
    stops that recurring.
    """
    body = run(client, strategy_id="mean_reversion",
               params={"window": 10, "threshold": 0.5}).json()
    assert len(body["trades"]) > 0, "fixture must produce trades or this proves nothing"
    assert body["metrics"]["Trade Count"] == len(body["trades"])


def test_metrics_only_response_omits_curve_and_trades(client: TestClient):
    body = run(client, include_equity_curve=False, include_trades=False).json()
    assert body["equity_curve"] == []
    assert body["trades"] == []
    assert body["metrics"]


def test_unsound_strategy_returns_its_caveat(client: TestClient):
    body = run(client, strategy_id="ml_random_forest",
               params={"n_estimators": 5, "lookback_window": 3}).json()
    assert body["caveat"] and "look-ahead" in body["caveat"].lower()


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_same_seed_gives_identical_results(client: TestClient):
    """
    Slippage used the unseeded module-level `random`, so identical requests
    returned different numbers and saved results could not be reproduced.
    An HTTP API that answers the same question differently each call is broken
    for caching, comparison and every downstream consumer.
    """
    params = {"window": 10, "threshold": 0.5}
    first = run(client, strategy_id="mean_reversion", params=params, seed=42).json()
    second = run(client, strategy_id="mean_reversion", params=params, seed=42).json()
    # Must trade, or slippage never applies and identical results prove nothing.
    assert len(first["trades"]) > 0
    assert first["metrics"] == second["metrics"]
    assert len(first["trades"]) == len(second["trades"])


def test_seed_defaults_to_42_so_repeat_requests_agree(client: TestClient):
    params = {"window": 10, "threshold": 0.5}
    first = run(client, strategy_id="mean_reversion", params=params).json()
    second = run(client, strategy_id="mean_reversion", params=params).json()
    assert first["seed"] == 42
    assert len(first["trades"]) > 0
    assert first["metrics"] == second["metrics"]


def test_different_seeds_can_differ(client: TestClient):
    """Seeding must not accidentally make slippage constant."""
    finals = {
        run(client, strategy_id="mean_reversion",
            params={"window": 10, "threshold": 0.5}, seed=seed)
        .json()["metrics"]["Final Value"]
        for seed in (1, 2, 3, 4, 5)
    }
    assert len(finals) > 1


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_unknown_symbol_is_404(client: TestClient):
    assert run(client, symbol="NOPE").status_code == 404


def test_unknown_strategy_is_404(client: TestClient):
    assert run(client, strategy_id="not_a_strategy").status_code == 404


def test_multi_asset_strategy_is_rejected(client: TestClient):
    """
    Handing a multi-asset strategy a single-symbol frame produces nonsense
    rather than an error, so the router must refuse it outright.
    """
    response = run(client, strategy_id="pairs_trading")
    assert response.status_code == 422
    assert "multi-symbol" in response.json()["detail"]


def test_unknown_param_name_is_rejected_not_ignored(client: TestClient):
    """A typo'd parameter must fail loudly, not silently run with defaults."""
    response = run(client, strategy_id="ma_crossover", params={"shrot_window": 10})
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


def test_strategy_self_validation_surfaces_as_422(client: TestClient):
    """Strategies raise ValueError for impossible parameters — a client error."""
    response = run(client, strategy_id="ma_crossover",
                   params={"short_window": 100, "long_window": 50})
    assert response.status_code == 422
    assert "smaller than the long window" in response.json()["detail"]


def test_start_after_end_is_422(client: TestClient):
    assert run(client, start="2024-12-31", end="2024-01-01").status_code == 422


def test_no_bars_in_range_is_422(client: TestClient):
    """
    Unlike /ohlcv, an empty range IS an error here: there is nothing to
    backtest, and returning zeroed metrics would look like a real result.
    """
    response = run(client, start="1990-01-01", end="1990-12-31")
    assert response.status_code == 422
    assert "No bars stored" in response.json()["detail"]


def test_registered_symbol_with_zero_bars_is_422(client: TestClient):
    assert run(client, symbol="EMPTY-USD").status_code == 422
