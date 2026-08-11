"""
Registry, determinism and metric-correctness tests.

Covers three defects found while building the Phase 3 routers, each of which
was silent — nothing failed, the numbers were just wrong:

  1. Backtests were non-reproducible (unseeded global RNG for slippage).
  2. "Trade Count" always reported 0.
  3. Strategy identity was duplicated across three files and had drifted.
"""

import pandas as pd
import pytest

from alpha_models import registry
from backtesting.backtester import Backtester
from execution.simulated_handler import SimulatedExecutionHandler


@pytest.fixture(scope="module")
def price_frame() -> pd.DataFrame:
    """A deterministic trending-then-reverting frame with real OHLCV columns."""
    idx = pd.bdate_range("2021-01-04", periods=400)
    base = pd.Series(range(400), index=idx, dtype=float)
    close = 100 + (base * 0.15) + (base % 40) * 1.5
    return pd.DataFrame(
        {
            "Open": close * 0.995,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": 1_000_000.0,
        },
        index=idx,
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_every_single_asset_strategy_is_constructible():
    """A strategy that cannot be built from its own declared defaults is broken."""
    for spec in registry.single_asset_strategies():
        model = spec.build()
        assert hasattr(model, "generate_signals"), spec.id


def test_registry_ids_are_unique():
    ids = [s.id for s in registry.all_strategies()]
    assert len(ids) == len(set(ids))


def test_param_defaults_match_declared_types():
    for spec in registry.all_strategies():
        for p in spec.params:
            expected = {"int": int, "float": float, "str": str}[p.type]
            assert isinstance(p.default, expected), f"{spec.id}.{p.name}"


def test_unknown_param_is_rejected_not_ignored():
    """A typo'd parameter must fail loudly rather than silently using defaults."""
    with pytest.raises(ValueError, match="Unknown parameter"):
        registry.build("ma_crossover", {"shrot_window": 10})


def test_unknown_strategy_lists_valid_ids():
    with pytest.raises(KeyError, match="Valid ids"):
        registry.get("does_not_exist")


def test_contract_test_cases_cover_all_single_asset_strategies():
    """
    Guards the registry/test-suite link: tests/test_strategy_contract.py
    parametrises over contract_test_cases(), so a strategy missing from it
    would silently escape the no-look-ahead contract.
    """
    covered = {sid for sid, _ in registry.contract_test_cases()}
    expected = {s.id for s in registry.single_asset_strategies()}
    assert covered == expected


def test_multi_asset_strategies_are_flagged():
    """
    The backtest/signals routers refuse multi-asset strategies on this flag.
    If one were mislabelled 'single' it would receive a single-symbol frame and
    silently produce nonsense instead of erroring.
    """
    multi = {s.id for s in registry.multi_asset_strategies()}
    assert multi == {
        "pairs_trading",
        "cointegrated_mean_reversion",
        "basket_trading",
        "index_rebalancing",
        # Asset allocation — these choose which sleeves to hold, so they must
        # receive a wide frame. Mislabelled 'single' they would be handed one
        # symbol at a time and could not compare anything.
        "paired_switching",
        "asset_class_trend",
        "momentum_allocation",
    }


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_same_seed_gives_identical_results(price_frame):
    """
    Identical inputs must give identical outputs.

    Slippage previously used the unseeded module-level `random`, so re-running a
    backtest produced different KPIs — saved results could not be reproduced and
    parameter comparisons were partly comparing random draws.
    """
    runs = []
    for _ in range(3):
        bt = Backtester(seed=42)
        bt.run(price_data=price_frame, model=registry.build("ma_crossover", {"short_window": 10, "long_window": 30}))
        runs.append((bt.get_performance_metrics(), len(bt.get_trade_log())))

    first_metrics, first_trades = runs[0]
    for metrics, trades in runs[1:]:
        assert trades == first_trades
        for key, value in first_metrics.items():
            assert metrics[key] == pytest.approx(value, abs=1e-12), key


def test_different_seeds_give_different_draws(price_frame):
    """Seeding must not accidentally make slippage constant."""
    finals = []
    for seed in (1, 2, 3):
        bt = Backtester(seed=seed)
        bt.run(price_data=price_frame, model=registry.build("ma_crossover", {"short_window": 10, "long_window": 30}))
        finals.append(bt.get_performance_metrics()["Final Value"])
    assert len(set(finals)) > 1


def test_handler_rng_is_per_instance_not_global():
    """
    Two handlers with the same seed must produce the same sequence independently.

    A module-level RNG would make concurrent backtests consume each other's
    draws — the API runs them in a threadpool, so this is a real concurrency
    concern, not a theoretical one.
    """
    a = SimulatedExecutionHandler(seed=7)
    b = SimulatedExecutionHandler(seed=7)
    seq_a = [a._rng.uniform(-1, 1) for _ in range(5)]
    seq_b = [b._rng.uniform(-1, 1) for _ in range(5)]
    assert seq_a == seq_b


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("strategy_id,params", [
    ("ma_crossover", {"short_window": 10, "long_window": 30}),
    ("buy_and_hold", {}),
    ("rsi", {}),
])
def test_trade_count_matches_trade_log(price_frame, strategy_id, params):
    """
    "Trade Count" must equal the number of executions.

    PerformanceAnalyzer read a "trades" column that Backtester.run() never
    produces, so the default-of-zeros always won and every backtest reported 0
    trades no matter how much it traded.
    """
    bt = Backtester(seed=42)
    bt.run(price_data=price_frame, model=registry.build(strategy_id, params))
    assert bt.get_performance_metrics()["Trade Count"] == len(bt.get_trade_log())


def test_trade_count_honours_explicit_trades_column(price_frame):
    """A caller supplying its own 'trades' column keeps control of the count."""
    from analysis.performance_analyzer import PerformanceAnalyzer

    bt = Backtester(seed=42)
    results = bt.run(price_data=price_frame, model=registry.build("ma_crossover", {"short_window": 10, "long_window": 30}))
    results = results.copy()
    results["trades"] = 0
    results.iloc[0, results.columns.get_loc("trades")] = 1
    results.iloc[5, results.columns.get_loc("trades")] = 1

    assert PerformanceAnalyzer(results, initial_capital=100000.0).calculate_all_metrics()["Trade Count"] == 2
