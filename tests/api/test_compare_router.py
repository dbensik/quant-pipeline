"""
POST /api/v1/backtest/compare

Ported from the Streamlit comparison tab.

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

BASE = "/api/v1/backtest/compare"
WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}
# Tuning is walk-forward (decision 41) and needs a validate year. The fixture
# runs 400 daily bars from 2024-01-01, so 2024 trains and early 2025 validates.
TUNE = {"optimize": True, "min_train_years": 1, "end": "2025-12-31"}


def compare(client: TestClient, **overrides):
    body = {
        "symbol": "AAPL",
        "strategies": [
            {"strategy_id": "ma_crossover"},
            {"strategy_id": "mean_reversion"},
        ],
        **WINDOW,
        **overrides,
    }
    return client.post(BASE, json=body)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_runs_every_requested_strategy(client: TestClient):
    body = compare(client).json()
    assert {r["strategy_id"] for r in body["results"]} == {
        "ma_crossover",
        "mean_reversion",
    }
    assert body["bars"] > 0


def test_results_are_ranked_best_first(client: TestClient):
    body = compare(client).json()
    sharpes = [r["metrics"]["Sharpe Ratio"] for r in body["results"]]
    assert sharpes == sorted(sharpes, reverse=True)


def test_ranking_respects_metric_direction(client: TestClient):
    """
    Ranking descending for every metric would report the MOST volatile
    strategy as best — the same defect fixed in ParameterOptimizer and the
    Streamlit optimization tab.
    """
    body = compare(client, metric="Annualized Volatility").json()
    vols = [r["metrics"]["Annualized Volatility"] for r in body["results"]]
    assert vols == sorted(vols)


def test_each_result_carries_the_params_used(client: TestClient):
    body = compare(client).json()
    row = next(r for r in body["results"] if r["strategy_id"] == "ma_crossover")
    assert set(row["params"]) == {"short_window", "long_window"}
    assert row["tuned"] is False


def test_equity_curves_are_returned_per_strategy(client: TestClient):
    body = compare(client).json()
    for row in body["results"]:
        assert len(row["equity_curve"]) == body["bars"]
        assert set(row["equity_curve"][0]) == {"time", "total"}


def test_curves_can_be_omitted(client: TestClient):
    body = compare(client, include_equity_curves=False).json()
    assert all(r["equity_curve"] == [] for r in body["results"])
    assert all(r["metrics"] for r in body["results"])


def test_explicit_params_are_used(client: TestClient):
    body = compare(
        client,
        strategies=[
            {"strategy_id": "ma_crossover", "params": {"short_window": 5, "long_window": 20}}
        ],
    ).json()
    assert body["results"][0]["params"] == {"short_window": 5, "long_window": 20}


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def test_benchmark_is_buy_and_hold_on_a_symbol(client: TestClient):
    body = compare(client, benchmark_symbol="MSFT").json()
    assert body["benchmark"]["symbol"] == "MSFT"
    assert body["benchmark"]["metrics"]["Total Return"] is not None


def test_benchmark_is_normalised_to_initial_capital(client: TestClient):
    """
    PerformanceAnalyzer expects `total` to be portfolio value, not raw price.
    Feeding it prices would make every capital-relative figure wrong.
    """
    body = compare(client, benchmark_symbol="MSFT", initial_capital=250_000.0).json()
    assert body["benchmark"]["equity_curve"][0]["total"] == 250_000.0


def test_benchmark_is_absent_when_not_requested(client: TestClient):
    assert compare(client).json()["benchmark"] is None


def test_unknown_benchmark_is_404(client: TestClient):
    assert compare(client, benchmark_symbol="NOPE").status_code == 404


def test_benchmark_without_bars_is_422_not_a_live_fetch(client: TestClient):
    """
    The controller fell back to live yfinance here. Prices come from the
    migrated database everywhere else, and a benchmark sourced elsewhere would
    be measured against a different price series than the strategies.
    """
    response = compare(client, benchmark_symbol="EMPTY-USD")
    assert response.status_code == 422
    assert "No bars stored" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Auto-tuning
# ---------------------------------------------------------------------------


def test_optimize_tunes_from_the_registry_default_grid(client: TestClient):
    """
    The grids moved from the Streamlit controller's _OPTIMIZATION_GRIDS into
    the registry, where every other strategy fact lives.
    """
    body = compare(client, **TUNE).json()
    assert body["optimized"] is True
    for row in body["results"]:
        assert row["tuned"] is True
        assert row["combinations_evaluated"] > 0


def test_tuning_actually_changes_the_parameters(client: TestClient):
    """
    Guards against `optimize` being cosmetic. If the tuned run returned the
    same defaults, the flag would be doing nothing.
    """
    untuned = compare(client).json()["results"]
    tuned = compare(client, **TUNE).json()["results"]

    untuned_params = {r["strategy_id"]: r["params"] for r in untuned}
    tuned_params = {r["strategy_id"]: r["params"] for r in tuned}
    assert any(untuned_params[k] != tuned_params[k] for k in untuned_params)


def test_an_explicit_grid_overrides_the_default(client: TestClient):
    body = compare(
        client,
        **TUNE,
        strategies=[
            {
                "strategy_id": "ma_crossover",
                "grid": {"short_window": [5], "long_window": [25]},
            }
        ],
    ).json()
    assert body["results"][0]["params"] == {"short_window": 5, "long_window": 25}


def test_a_strategy_without_a_grid_runs_on_its_params(client: TestClient):
    """`optimize` must not fail a strategy the registry has no sweep for."""
    body = compare(client, **TUNE, strategies=[{"strategy_id": "trend_following"}]).json()
    assert body["results"][0]["tuned"] is False
    assert body["results"][0]["metrics"]


def test_oversized_grid_is_skipped_with_a_reason(client: TestClient):
    body = compare(
        client,
        **TUNE,
        strategies=[
            {
                "strategy_id": "ma_crossover",
                "grid": {"short_window": list(range(1, 30)), "long_window": list(range(30, 60))},
            },
            {"strategy_id": "mean_reversion"},
        ],
    ).json()
    assert [s["strategy_id"] for s in body["skipped"]] == ["ma_crossover"]
    assert "limit is 500" in body["skipped"][0]["reason"]


def test_tuning_without_a_validate_year_is_422_not_hindsight(client: TestClient):
    """Decision 41: a window too short to walk forward is refused, never tuned in hindsight."""
    response = compare(client, optimize=True)
    assert response.status_code == 422
    assert "walk-forward" in response.json()["detail"]
    assert "3 whole calendar year" in response.json()["detail"]


def test_tuned_rows_carry_each_years_pick(client: TestClient):
    body = compare(client, **TUNE).json()
    assert body["evaluated_from"].startswith("2025-01-01")
    for row in body["results"]:
        assert len(row["picks"]) == 1
        assert row["picks"][0]["validate_start"].startswith("2025-01-01")
        effective = {k: row["params"][k] for k in row["picks"][0]["params"]}
        assert effective == row["picks"][0]["params"]


def test_with_tuning_every_row_and_the_benchmark_share_one_span(client: TestClient):
    """
    Tuned rows are out of sample from the first validate year only, so an
    untuned row and the benchmark are measured over the same span, each
    worth initial_capital at its start; otherwise the ranking compares a
    year of 2025 with two years of 2024-25.
    """
    body = compare(
        client,
        **TUNE,
        strategies=[{"strategy_id": "ma_crossover"}, {"strategy_id": "trend_following"}],
        benchmark_symbol="MSFT",
        initial_capital=50_000,
    ).json()
    curves = [r["equity_curve"] for r in body["results"]] + [body["benchmark"]["equity_curve"]]
    for curve in curves:
        assert curve[0]["time"].startswith("2024-12-31")
        assert curve[0]["total"] == 50_000
        assert curve[-1]["time"].startswith("2025-02-03")
    untuned = next(r for r in body["results"] if r["strategy_id"] == "trend_following")
    assert untuned["tuned"] is False
    assert untuned["metrics"]["Final Value"] == curves[1][-1]["total"]
    assert body["benchmark"]["metrics"]["Final Value"] == curves[-1][-1]["total"]


def test_without_tuning_the_whole_window_is_measured(client: TestClient):
    body = compare(client).json()
    assert body["evaluated_from"] is None
    assert body["results"][0]["equity_curve"][0]["time"].startswith("2024-01-01")
    assert all(r["picks"] == [] for r in body["results"])


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_same_seed_gives_identical_comparisons(client: TestClient):
    first = compare(client, include_equity_curves=False, seed=42).json()
    second = compare(client, include_equity_curves=False, seed=42).json()
    assert first["results"] == second["results"]


# ---------------------------------------------------------------------------
# Errors and partial failure
# ---------------------------------------------------------------------------


def test_unknown_strategy_is_skipped_with_a_reason_not_dropped(client: TestClient):
    """A comparison missing an entry must say why."""
    body = compare(
        client,
        strategies=[{"strategy_id": "ma_crossover"}, {"strategy_id": "not_a_strategy"}],
    ).json()
    assert [r["strategy_id"] for r in body["results"]] == ["ma_crossover"]
    assert body["skipped"][0]["strategy_id"] == "not_a_strategy"


def test_multi_asset_strategy_is_skipped_with_a_reason(client: TestClient):
    body = compare(
        client,
        strategies=[{"strategy_id": "ma_crossover"}, {"strategy_id": "pairs_trading"}],
    ).json()
    assert any(s["strategy_id"] == "pairs_trading" for s in body["skipped"])
    assert "multi-asset" in body["skipped"][0]["reason"]


def test_no_runnable_strategy_is_422_not_an_empty_comparison(client: TestClient):
    response = compare(client, strategies=[{"strategy_id": "not_a_strategy"}])
    assert response.status_code == 422
    assert "No strategy could be compared" in response.json()["detail"]


def test_duplicate_strategies_are_rejected(client: TestClient):
    """Two rows with the same name cannot be told apart in a results table."""
    response = compare(
        client,
        strategies=[{"strategy_id": "ma_crossover"}, {"strategy_id": "ma_crossover"}],
    )
    assert response.status_code == 422
    assert "Duplicate" in response.json()["detail"]


def test_unknown_metric_is_422(client: TestClient):
    assert compare(client, metric="Shrape Ratio").status_code == 422


def test_unknown_symbol_is_404(client: TestClient):
    assert compare(client, symbol="NOPE").status_code == 404


def test_symbol_with_no_bars_is_422(client: TestClient):
    assert compare(client, symbol="EMPTY-USD").status_code == 422


def test_start_after_end_is_422(client: TestClient):
    assert compare(client, start="2024-12-31", end="2024-01-01").status_code == 422


def test_too_many_strategies_is_422(client: TestClient):
    response = compare(client, strategies=[{"strategy_id": f"s{i}"} for i in range(11)])
    assert response.status_code == 422
    assert "limit is 10" in response.json()["detail"]


def test_empty_strategy_list_is_rejected(client: TestClient):
    assert compare(client, strategies=[]).status_code == 422


def test_crypto_rows_trade_below_one_coin(client: TestClient):
    """Each compared strategy on BTC traded whole coins only, so none traded."""
    body = compare(
        client,
        symbol="BTC-USD",
        initial_capital=20_000,
        strategies=[{"strategy_id": "buy_and_hold"}],
    ).json()
    assert body["results"][0]["metrics"]["Trade Count"] > 0
