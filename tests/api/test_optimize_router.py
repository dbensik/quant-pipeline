"""
POST /api/v1/optimize/strategy
POST /api/v1/optimize/portfolio

Phase 5 — decommissioning Streamlit

Several of these are regressions on defects in the code being ported. Each
such test was checked against the OLD behaviour before being committed, per
the rule in CLAUDE.md — a test that passes against the bug it names is worse
than no test.
"""

import pytest
from fastapi.testclient import TestClient

WINDOW = {"start": "2024-01-01", "end": "2024-12-31"}


def optimize(client: TestClient, **overrides):
    body = {
        "symbol": "AAPL",
        "strategy_id": "ma_crossover",
        "grid": {"short_window": [5, 10, 15], "long_window": [30, 40]},
        **WINDOW,
        **overrides,
    }
    return client.post("/api/v1/optimize/strategy", json=body)


def allocate(client: TestClient, **overrides):
    # AAPL + MSFT, not AAPL + BTC-USD: the first two fixture symbols share one
    # price formula and differ only in scale, so their returns are identical
    # and every weighting of them has the same volatility — a "frontier" over
    # that pair is a single point, and every allocation assertion below would
    # hold no matter what the optimizer did. MSFT follows an independent path.
    body = {
        "symbols": ["AAPL", "MSFT"],
        "num_portfolios": 200,
        **WINDOW,
        **overrides,
    }
    return client.post("/api/v1/optimize/portfolio", json=body)


# ---------------------------------------------------------------------------
# Strategy grid search — happy path
# ---------------------------------------------------------------------------

def test_searches_the_whole_grid(client: TestClient):
    body = optimize(client).json()
    assert body["combinations_requested"] == 6
    assert body["combinations_evaluated"] == 6
    assert len(body["results"]) == 6


def test_reports_the_winning_parameters(client: TestClient):
    body = optimize(client).json()
    assert set(body["best_params"]) == {"short_window", "long_window"}
    assert body["best_params"]["short_window"] in (5, 10, 15)


def test_results_are_ranked_best_first(client: TestClient):
    body = optimize(client).json()
    sharpes = [row["Sharpe Ratio"] for row in body["results"]]
    assert sharpes == sorted(sharpes, reverse=True)
    assert body["best_metrics"]["Sharpe Ratio"] == sharpes[0]


def test_top_n_truncates_the_ranking_not_the_search(client: TestClient):
    body = optimize(client, top_n=2).json()
    assert len(body["results"]) == 2
    assert body["combinations_evaluated"] == 6


def test_a_range_axis_expands_inclusively(client: TestClient):
    body = optimize(
        client,
        grid={
            "short_window": {"min": 5, "max": 25, "step": 5},  # 5,10,15,20,25
            "long_window": [40],
        },
    ).json()
    assert body["combinations_requested"] == 5


def test_int_parameters_come_back_as_ints(client: TestClient):
    """
    Reading params back out of the results DataFrame upcast them: .loc[i]
    returns one Series spanning params AND float metrics, so short_window
    arrived as 5.0. Round-tripping a "best" result into another request then
    sent floats where the registry declares ints.
    """
    best = optimize(client).json()["best_params"]
    assert isinstance(best["short_window"], int)
    assert isinstance(best["long_window"], int)


def test_ranked_rows_also_carry_int_parameters(client: TestClient):
    """
    Same upcast, one layer out. A results table is what a UI renders and what
    a "re-run this row" control would post back, so a row reading 20.0 sends a
    float where the registry declares an int.
    """
    for row in optimize(client).json()["results"]:
        assert isinstance(row["short_window"], int)
        assert isinstance(row["long_window"], int)


# ---------------------------------------------------------------------------
# Params must not be contaminated by metrics
# ---------------------------------------------------------------------------

def test_best_params_contains_no_metrics(client: TestClient):
    """
    THE regression on get_best_parameters. It recovered parameters by
    subtracting a hardcoded list of six metric names from the result row, but
    PerformanceAnalyzer returns ten — so "Final Value", "Calmar Ratio",
    "Max Drawdown Duration (Days)" and "Trade Count" were every one of them
    reported as a tuned parameter.
    """
    best = optimize(client).json()["best_params"]
    for metric in (
        "Final Value",
        "Calmar Ratio",
        "Max Drawdown Duration (Days)",
        "Trade Count",
        "Sharpe Ratio",
    ):
        assert metric not in best


def test_best_metrics_carries_the_full_metric_set(client: TestClient):
    metrics = optimize(client).json()["best_metrics"]
    for key in ("Sharpe Ratio", "Final Value", "Max Drawdown", "Trade Count"):
        assert key in metrics
    assert "short_window" not in metrics


# ---------------------------------------------------------------------------
# Metric direction
# ---------------------------------------------------------------------------

def test_minimized_metric_is_not_maximized(client: TestClient):
    """
    Ranking was an unconditional idxmax, so asking to optimize for
    "Annualized Volatility" returned the MOST volatile parameter set while
    labelling it best.
    """
    grid = {"short_window": [5, 10, 15, 20, 25], "long_window": [30, 40]}
    least = optimize(client, grid=grid, metric="Annualized Volatility").json()
    most = optimize(client, grid=grid, metric="Sharpe Ratio").json()
    assert (
        least["best_metrics"]["Annualized Volatility"]
        <= most["best_metrics"]["Annualized Volatility"]
    )
    assert least["best_params"] != most["best_params"]


def test_minimized_metric_ranks_ascending(client: TestClient):
    body = optimize(client, metric="Annualized Volatility").json()
    vols = [row["Annualized Volatility"] for row in body["results"]]
    assert vols == sorted(vols)


def test_unknown_metric_is_422_not_a_silent_empty_result(client: TestClient):
    """
    get_best_parameters returned {} when the metric was not a column, so a
    typo produced a 200 with no winner rather than an error.
    """
    response = optimize(client, metric="Shrape Ratio")
    assert response.status_code == 422
    assert "Cannot optimize" in response.json()["detail"]


def test_trade_count_is_rejected_as_a_target(client: TestClient):
    """Reported, but neither more nor fewer trades is better on its own."""
    assert optimize(client, metric="Trade Count").status_code == 422


# ---------------------------------------------------------------------------
# Invalid combinations
# ---------------------------------------------------------------------------

def test_invalid_combinations_are_skipped_and_reported(client: TestClient):
    """
    MACrossoverParameterGenerator hardcoded `if s >= l: continue`, so the
    constraint lived in the generator rather than the strategy and silently
    vanished from the accounting. The strategy's own ValueError is now what
    rejects the combo, and the rejection is reported.
    """
    body = optimize(
        client, grid={"short_window": [10, 20, 30], "long_window": [20, 30]}
    ).json()
    assert body["combinations_requested"] == 6
    assert body["combinations_evaluated"] == 3
    assert len(body["skipped"]) == 3
    assert "short window must be smaller" in body["skipped"][0]["reason"]


def test_an_entirely_invalid_grid_is_422_not_an_empty_ranking(client: TestClient):
    """
    Zero valid combinations means nothing was searched. A 200 with
    best_params={} would read as "searched and found nothing".
    """
    response = optimize(client, grid={"short_window": [50], "long_window": [10]})
    assert response.status_code == 422
    assert "rejected" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Grid validation
# ---------------------------------------------------------------------------

def test_unknown_grid_parameter_is_rejected_not_skipped(client: TestClient):
    """
    The optimizer treats a ValueError from spec.build() as "invalid
    combination, skip it" — and an unknown parameter name raises exactly that.
    Without an up-front key check, a typo would skip every combination and
    return 422 "all rejected", or worse read as a legitimately empty search,
    while the same typo on /api/v1/backtest is a clear 422.
    """
    response = optimize(client, grid={"short_windwo": [5, 10]})
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]
    assert "short_windwo" in response.json()["detail"]


def test_unknown_fixed_parameter_is_rejected(client: TestClient):
    response = optimize(client, params={"windwo": 5})
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


def test_empty_grid_is_422(client: TestClient):
    response = optimize(client, grid={})
    assert response.status_code == 422
    assert "nothing to search" in response.json()["detail"]


def test_oversized_grid_is_rejected_before_running(client: TestClient):
    response = optimize(
        client,
        grid={
            "short_window": {"min": 1, "max": 100, "step": 1},
            "long_window": {"min": 101, "max": 200, "step": 1},
        },
    )
    assert response.status_code == 422
    assert "10000 combinations" in response.json()["detail"]


def test_the_ported_streamlit_default_grid_still_fits(client: TestClient):
    """
    Streamlit's MA-crossover defaults were short 10-30 and long 40-60 — a
    21x21 grid, 441 combinations. The cap must not regress the case being
    ported.
    """
    body = optimize(
        client,
        grid={
            "short_window": {"min": 10, "max": 30, "step": 1},
            "long_window": {"min": 40, "max": 60, "step": 1},
        },
        top_n=1,
    ).json()
    assert body["combinations_requested"] == 441


def test_inverted_range_is_422(client: TestClient):
    response = optimize(
        client, grid={"short_window": {"min": 30, "max": 10}, "long_window": [40]}
    )
    assert response.status_code == 422
    assert "below min" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_same_seed_gives_identical_results(client: TestClient):
    """
    ParameterOptimizer shared ONE unseeded Backtester across every
    combination, so each candidate faced a different slippage draw and the
    ranking mixed parameter effect with noise.

    The assertion is on metrics, not on best_params. Checked against the
    unseeded implementation first: across four runs of this grid the winning
    PARAMS never moved while the winning Sharpe ranged 5.20-5.42, so a
    best_params assertion would have passed against the very bug it names.
    """
    first = optimize(client, seed=42).json()
    second = optimize(client, seed=42).json()
    assert first["best_metrics"] == second["best_metrics"]
    assert first["results"] == second["results"]


def test_different_seeds_can_pick_different_winners(client: TestClient):
    """
    Establishes that the ranking really is seed-sensitive — which is what
    makes the seed worth threading, and what makes the test above meaningful.
    """
    winners = {
        tuple(sorted(optimize(client, seed=seed).json()["best_params"].items()))
        for seed in range(1, 9)
    }
    assert len(winners) > 1


def test_every_combination_faces_the_same_draws(client: TestClient):
    """
    Common random numbers: a fresh Backtester carrying the same seed per
    combination. Re-running one combination alone must reproduce the score it
    got inside the grid — which is false if a single RNG advances across
    combinations, since combo #6 would then see a different stream than it
    does when run first.
    """
    grid_body = optimize(
        client, grid={"short_window": [5, 10, 15], "long_window": [30, 40]}, seed=42
    ).json()
    last = grid_body["results"][-1]
    alone = optimize(
        client,
        grid={
            "short_window": [last["short_window"]],
            "long_window": [last["long_window"]],
        },
        seed=42,
    ).json()
    assert alone["best_metrics"]["Sharpe Ratio"] == last["Sharpe Ratio"]


# ---------------------------------------------------------------------------
# Strategy contract
# ---------------------------------------------------------------------------

def test_a_strategy_the_old_factory_never_knew_can_be_optimized(client: TestClient):
    """
    _create_model hardcoded two display names and returned None for anything
    else, which run_optimization silently skipped — so optimizing any other
    strategy produced an empty DataFrame rather than an error. Construction
    now goes through the registry.
    """
    body = optimize(
        client, strategy_id="trend_following", grid={"window": [10, 20, 30]}
    ).json()
    assert body["combinations_evaluated"] == 3
    assert set(body["best_params"]) == {"window"}


def test_multi_asset_strategy_is_rejected(client: TestClient):
    response = optimize(client, strategy_id="pairs_trading", grid={"window": [10, 20]})
    assert response.status_code == 422
    assert "multi-asset" in response.json()["detail"]


def test_unknown_strategy_is_404(client: TestClient):
    assert optimize(client, strategy_id="not_a_strategy").status_code == 404


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_unknown_symbol_is_404(client: TestClient):
    assert optimize(client, symbol="NOPE").status_code == 404


def test_symbol_with_no_bars_is_422(client: TestClient):
    response = optimize(client, symbol="EMPTY-USD")
    assert response.status_code == 422
    assert "No bars stored" in response.json()["detail"]


def test_start_after_end_is_422(client: TestClient):
    assert optimize(client, start="2024-12-31", end="2024-01-01").status_code == 422


# ---------------------------------------------------------------------------
# Portfolio weight optimization
# ---------------------------------------------------------------------------

def test_returns_both_optimal_allocations(client: TestClient):
    body = allocate(client).json()
    for key in ("max_sharpe", "min_volatility"):
        assert set(body[key]["weights"]) == {"AAPL", "MSFT"}


def test_weights_sum_to_one(client: TestClient):
    body = allocate(client).json()
    assert sum(body["max_sharpe"]["weights"].values()) == pytest.approx(1.0)
    assert sum(body["min_volatility"]["weights"].values()) == pytest.approx(1.0)


def test_min_volatility_is_not_more_volatile_than_max_sharpe(client: TestClient):
    body = allocate(client).json()
    assert (
        body["min_volatility"]["annualized_volatility"]
        <= body["max_sharpe"]["annualized_volatility"]
    )


def test_same_seed_gives_the_same_allocation(client: TestClient):
    """
    simulate_random_portfolios drew weights from the GLOBAL numpy RNG, so the
    same universe and date range returned a different "optimal" allocation on
    every call and a saved allocation could not be reproduced.

    Verified non-vacuous: with seed=None, two 300-trial runs of this exact
    universe returned AAPL weights of 0.009674 and 0.008979.
    """
    first = allocate(client, seed=7).json()
    second = allocate(client, seed=7).json()
    assert first["max_sharpe"] == second["max_sharpe"]
    assert first["min_volatility"] == second["min_volatility"]


def test_different_seeds_give_different_allocations(client: TestClient):
    weights = {
        round(allocate(client, seed=seed).json()["max_sharpe"]["weights"]["AAPL"], 6)
        for seed in (1, 2, 3, 4, 5)
    }
    assert len(weights) > 1


def test_frontier_is_omitted_by_default(client: TestClient):
    assert allocate(client).json()["frontier"] == []


def test_frontier_is_returned_when_requested(client: TestClient):
    body = allocate(client, include_frontier=True, num_portfolios=200).json()
    assert len(body["frontier"]) == 200
    assert set(body["frontier"][0]) == {
        "annualized_return",
        "annualized_volatility",
        "sharpe_ratio",
        "weights",
    }


def test_frontier_is_downsampled_not_truncated(client: TestClient):
    """
    5,000 rows each carrying a weights mapping is a large response, but the
    frontier's shape comes from the whole cloud — head(N) would return only
    the first draws, which is not a sample of it.
    """
    body = allocate(
        client, include_frontier=True, num_portfolios=500, frontier_points=50
    ).json()
    assert len(body["frontier"]) <= 50

    full = allocate(client, include_frontier=True, num_portfolios=500).json()
    sampled_vols = {round(p["annualized_volatility"], 9) for p in body["frontier"]}
    head_vols = {
        round(p["annualized_volatility"], 9) for p in full["frontier"][:50]
    }
    assert sampled_vols != head_vols


def test_one_symbol_is_422(client: TestClient):
    response = allocate(client, symbols=["AAPL"])
    assert response.status_code == 422
    assert "at least 2" in response.json()["detail"]


def test_unknown_symbol_is_404_for_portfolio(client: TestClient):
    assert allocate(client, symbols=["AAPL", "NOPE"]).status_code == 404


def test_symbol_with_no_bars_is_422_for_portfolio(client: TestClient):
    assert allocate(client, symbols=["AAPL", "EMPTY-USD"]).status_code == 422


def test_too_few_common_dates_is_422(client: TestClient):
    response = allocate(client, start="2024-01-01", end="2024-01-02")
    assert response.status_code == 422
    assert "covariance" in response.json()["detail"]
