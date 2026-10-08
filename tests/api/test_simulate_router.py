"""
POST /api/v1/simulate

Every assertion is on something the router COMPUTES — bands, percentiles,
probabilities, the caveat — never on a field that merely echoes the request.
Phase 2 of research/monte-carlo-plan-2026-10-07.md.
"""

import pytest
from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
# mean_reversion 10/0.5 trades on this fixture (see test_backtest_router.py);
# buy_and_hold makes one trade; ma_crossover 10/30 makes none.
TRADING = {"strategy_id": "mean_reversion", "params": {"window": 10, "threshold": 0.5}}


def run(client: TestClient, **overrides):
    body = {"symbol": "AAPL", "strategy_id": "buy_and_hold", **WINDOW, "n_paths": 200, **overrides}
    return client.post("/api/v1/simulate", json=body)


def band_values(body, key):
    return [b[key] for b in body["bands"]]


# ---------------------------------------------------------------------------
# Output shape and internal consistency
# ---------------------------------------------------------------------------

def test_bands_start_at_initial_capital_and_are_ordered(client: TestClient):
    body = run(client, initial_capital=50_000.0).json()
    first = body["bands"][0]
    assert first["step"] == 0
    assert {first["p05"], first["p25"], first["p50"], first["p75"], first["p95"]} == {50_000.0}
    assert len(body["bands"]) == body["horizon_days"] + 1
    for b in body["bands"]:
        assert b["p05"] <= b["p25"] <= b["p50"] <= b["p75"] <= b["p95"]


def test_default_horizon_is_the_resampled_history(client: TestClient):
    body = run(client).json()
    assert body["horizon_days"] == body["resampled_from"]
    assert 0 < body["resampled_from"] <= body["bars"]


def test_explicit_horizon_sets_band_count(client: TestClient):
    body = run(client, horizon_days=30).json()
    assert len(body["bands"]) == 31
    assert [r["horizon_days"] for r in body["risk"]["rows"]] == [1, 10, 21]


def test_risk_rows_drop_horizons_longer_than_the_simulation(client: TestClient):
    body = run(client, horizon_days=5).json()
    assert [r["horizon_days"] for r in body["risk"]["rows"]] == [1]


def test_cvar_is_at_least_var_and_99_at_least_95(client: TestClient):
    body = run(client, **TRADING, mode="returns").json()
    for row in body["risk"]["rows"]:
        assert row["cvar_95"] >= row["var_95"]
        assert row["var_99"] >= row["var_95"]
        assert row["cvar_99"] >= row["cvar_95"]


def test_terminal_and_drawdown_summaries_are_coherent(client: TestClient):
    body = run(client).json()
    t = body["terminal"]
    assert t["wealth"]["p05"] <= t["wealth"]["p50"] <= t["wealth"]["p95"]
    assert t["total_return"]["p50"] == pytest.approx(t["wealth"]["p50"] / 100_000.0 - 1.0)
    assert 0.0 <= t["prob_loss"] <= 1.0
    d = body["drawdown"]
    assert d["depth"]["p95"] <= 0.0
    assert d["depth"]["p05"] <= d["depth"]["p50"]
    assert d["historical_depth"] is not None and d["historical_depth"] <= 0.0
    assert 0.0 <= d["prob_worse_than_historical"] <= 1.0
    assert 0.0 <= body["risk"]["prob_ruin"] <= 1.0


def test_historical_metrics_are_the_real_backtest(client: TestClient):
    sim = run(client, **TRADING).json()
    bt = client.post(
        "/api/v1/backtest",
        json={"symbol": "AAPL", **WINDOW, **TRADING, "include_equity_curve": False, "include_trades": False},
    ).json()
    assert sim["historical"] == bt["metrics"]
    assert sim["drawdown"]["historical_depth"] == bt["metrics"]["Max Drawdown"]


def test_risk_rows_come_from_the_simulated_equity(client: TestClient):
    """
    VaR at one day is the 5th percentile of the first step's return, and the
    step-1 p05 band is start equity compounded by that same percentile — so the
    two must agree in BOTH modes. In prices mode this is what proves the risk
    table is built from the strategy's equity, not from the raw price draws.
    """
    for mode in ("returns", "prices"):
        body = run(client, **TRADING, mode=mode, n_paths=50).json()
        one_day = next(r for r in body["risk"]["rows"] if r["horizon_days"] == 1)
        assert body["bands"][1]["p05"] == pytest.approx(100_000.0 * (1.0 - one_day["var_95"]), rel=1e-9), mode


def test_returns_mode_resamples_from_the_first_invested_bar(client: TestClient):
    """
    A strategy that waits before its first trade must not bootstrap its flat
    cash prefix. mean_reversion needs a full window before it can signal;
    buy_and_hold is invested from bar 0.
    """
    waiting = run(client, **TRADING).json()
    immediate = run(client, strategy_id="buy_and_hold").json()
    assert waiting["resampled_from"] < waiting["bars"]
    assert immediate["resampled_from"] == immediate["bars"]


def test_prob_loss_counts_strictly_losing_paths(client: TestClient):
    """
    ma_crossover 10/30 trades rarely on bootstrapped fixture paths, so some
    paths end EXACTLY at start equity. Those are not losses. Cross-check
    prob_loss against the returned sample paths — the two are computed
    separately, so they can only agree if both use a strict comparison.
    """
    body = run(client, strategy_id="ma_crossover", params={"short_window": 10, "long_window": 30},
               mode="prices", n_paths=40, horizon_days=45, include_paths=40).json()
    finals = [p[-1] for p in body["sample_paths"]]
    assert any(f == 100_000.0 for f in finals), "fixture must produce a flat path or this proves nothing"
    assert body["terminal"]["prob_loss"] == pytest.approx(sum(f < 100_000.0 for f in finals) / 40)


def test_sample_paths_are_equity_curves(client: TestClient):
    body = run(client, include_paths=3, horizon_days=20).json()
    assert len(body["sample_paths"]) == 3
    for path in body["sample_paths"]:
        assert len(path) == 21
        assert path[0] == 100_000.0
        assert all(v > 0 for v in path)


def test_no_sample_paths_by_default(client: TestClient):
    assert run(client).json()["sample_paths"] == []


# ---------------------------------------------------------------------------
# Determinism and the knobs that must matter
# ---------------------------------------------------------------------------

def test_seed_is_two_sided(client: TestClient):
    a = run(client, seed=1).json()
    b = run(client, seed=1).json()
    c = run(client, seed=2).json()
    assert band_values(a, "p05") == band_values(b, "p05")
    assert band_values(a, "p05") != band_values(c, "p05")


def test_method_changes_the_bands(client: TestClient):
    stationary = run(client, method="stationary").json()
    iid = run(client, method="iid").json()
    gbm = run(client, method="gbm").json()
    assert band_values(stationary, "p05") != band_values(iid, "p05")
    assert band_values(iid, "p05") != band_values(gbm, "p05")


def test_block_length_changes_the_bands(client: TestClient):
    short = run(client, block_length_days=2).json()
    long = run(client, block_length_days=60).json()
    assert band_values(short, "p05") != band_values(long, "p05")


def test_prices_mode_reruns_the_strategy(client: TestClient):
    returns_mode = run(client, **TRADING, mode="returns", n_paths=20).json()
    prices_mode = run(client, **TRADING, mode="prices", n_paths=20).json()
    assert prices_mode["n_paths"] == 20
    assert len(prices_mode["bands"]) == prices_mode["horizon_days"] + 1
    assert prices_mode["bands"][0]["p50"] == 100_000.0
    assert band_values(prices_mode, "p50") != band_values(returns_mode, "p50")
    # In prices mode the draws come from PRICE returns: one per bar after the first.
    assert prices_mode["resampled_from"] == prices_mode["bars"] - 1


def test_prices_mode_default_path_count_is_the_rerun_default(client: TestClient):
    from config import settings
    body = {"symbol": "AAPL", **WINDOW, **TRADING, "mode": "prices", "horizon_days": 10}
    assert client.post("/api/v1/simulate", json=body).json()["n_paths"] == settings.SIM_DEFAULT_RERUN_PATHS


# ---------------------------------------------------------------------------
# Caveats
# ---------------------------------------------------------------------------

def test_returns_mode_on_a_trading_strategy_carries_the_path_caveat(client: TestClient):
    body = run(client, **TRADING, mode="returns").json()
    assert body["caveat"] and "depend on the path" in body["caveat"]


def test_buy_and_hold_in_returns_mode_has_no_caveat(client: TestClient):
    assert run(client, strategy_id="buy_and_hold", mode="returns").json()["caveat"] is None


def test_prices_mode_has_no_path_caveat(client: TestClient):
    body = run(client, **TRADING, mode="prices", n_paths=5).json()
    assert body["caveat"] is None


def test_strategy_caveat_is_passed_through(client: TestClient):
    body = run(client, strategy_id="ml_random_forest", mode="prices", n_paths=2,
               params={"n_estimators": 5, "lookback_window": 3}).json()
    assert "look-ahead" in body["caveat"].lower()


# ---------------------------------------------------------------------------
# Gates and errors
# ---------------------------------------------------------------------------

def test_unverified_asset_is_refused_by_default(client: TestClient):
    response = run(client, symbol="BTC-USD")
    assert response.status_code == 422
    assert "allow_unverified" in response.json()["detail"]


def test_unverified_asset_runs_with_the_flag_and_says_so(client: TestClient):
    response = run(client, symbol="BTC-USD", allow_unverified=True)
    assert response.status_code == 200
    assert "not read-time adjusted" in response.json()["caveat"]


def test_path_cap_is_per_mode(client: TestClient):
    from config import settings
    assert run(client, n_paths=settings.SIM_MAX_PATHS + 1).status_code == 422
    assert run(client, mode="prices", n_paths=settings.SIM_MAX_RERUN_PATHS + 1).status_code == 422
    assert run(client, mode="prices", n_paths=settings.SIM_MAX_RERUN_PATHS + 1,
               horizon_days=5).status_code == 422


def test_horizon_cap(client: TestClient):
    from config import settings
    assert run(client, horizon_days=settings.SIM_MAX_HORIZON_DAYS + 1).status_code == 422


def test_never_invested_strategy_cannot_be_resampled_in_returns_mode(client: TestClient):
    # ma_crossover 10/30 makes zero trades on this fixture.
    response = run(client, strategy_id="ma_crossover", params={"short_window": 10, "long_window": 30})
    assert response.status_code == 422
    assert "never invested" in response.json()["detail"]


def test_unknown_symbol_and_strategy(client: TestClient):
    assert run(client, symbol="NOPE").status_code == 404
    assert run(client, strategy_id="nope").status_code == 404


def test_multi_asset_strategy_rejected(client: TestClient):
    assert run(client, strategy_id="pairs_trading").status_code == 422


def test_start_after_end(client: TestClient):
    assert run(client, start="2024-12-31", end="2024-01-01").status_code == 422


def test_empty_window(client: TestClient):
    assert run(client, start="1990-01-01", end="1990-01-02").status_code == 422


# ---------------------------------------------------------------------------
# Worker contract that the response cannot show
# ---------------------------------------------------------------------------

def test_prices_mode_seeds_each_path_from_its_index(monkeypatch):
    """
    Path i's slippage RNG is seeded `seed + i`, so a result never depends on
    the order paths ran in. Not visible in the response, so the worker is
    observed directly.
    """
    from datetime import datetime

    import api.routers.simulate as simulate
    from alpha_models import registry
    from api.frames import records_to_frame
    from tests.api.conftest import _series

    seeds = []
    real = simulate.Backtester

    class Recording(real):
        def __init__(self, *args, **kwargs):
            seeds.append(kwargs.get("seed"))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(simulate, "Backtester", Recording)
    frame = records_to_frame(_series("AAPL"))
    request = simulate.SimulationRequest(
        symbol="AAPL", strategy_id="buy_and_hold", start=datetime(2024, 1, 1),
        end=datetime(2024, 12, 31), mode="prices", n_paths=4, horizon_days=10, seed=7,
    )
    simulate._simulate_sync(frame, registry.get("buy_and_hold"), request, 4)
    # The historical run is seeded with the request seed via _run_backtest_sync
    # (which builds its own Backtester, not patched here); the four paths follow.
    assert seeds[-4:] == [7, 8, 9, 10]
