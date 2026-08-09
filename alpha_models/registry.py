"""
alpha_models/registry.py

The single source of truth for "what strategies exist and what do they take".

Before this module, strategy identity was duplicated in three places:
  * tests/test_strategy_contract.py — id -> factory pairs
  * dashboard_app/ui_components/sidebar.py — display names and param widgets
  * dashboard_app/controllers/analysis_controller.py — factory logic

Adding a strategy meant editing all three, and they had already drifted. Every
consumer should now read from here instead. `tests/test_strategy_contract.py`
imports SINGLE_ASSET_STRATEGIES from this module, which makes the contract
suite double as the registry's correctness check.

Registering a new strategy here is all that's required for it to appear in the
API's /api/v1/strategies listing — and therefore in any UI driven by it, with
its own parameter controls.

Phase 3 — FastAPI routers for the React UI
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Type

from alpha_models.base_model import BaseAlphaModel
from alpha_models.atr_breakout import ATRBreakoutStrategy
from alpha_models.basket_trading import BasketTradingStrategy
from alpha_models.buy_and_hold import BuyAndHoldStrategy
from alpha_models.cointegrated_mean_reversion import CointegratedMeanReversionStrategy
from alpha_models.index_rebalancing import IndexRebalancingStrategy
from alpha_models.mean_reversion import MeanReversionStrategy
from alpha_models.ml_random_forest import RandomForestStrategy
from alpha_models.moving_average_crossover import MovingAverageCrossoverStrategy
from alpha_models.pairs_trading import PairsTradingStrategy
from alpha_models.push_response_strategy import PushResponseStrategy
from alpha_models.rsi_strategy import RSIStrategy
from alpha_models.trend_following import TrendFollowingStrategy


ParamType = Literal["int", "float", "str"]


@dataclass(frozen=True)
class ParamSpec:
    """
    One tunable strategy parameter.

    `minimum`/`maximum` are advisory bounds for UI controls and API validation.
    They are not enforced by the strategy classes themselves — several validate
    their own inputs and raise ValueError, which the API surfaces as a 422.
    """

    name: str
    type: ParamType
    default: Any
    label: str
    description: str = ""
    minimum: Optional[float] = None
    maximum: Optional[float] = None


@dataclass(frozen=True)
class StrategySpec:
    """Everything a consumer needs to list, configure and instantiate a strategy."""

    id: str
    display_name: str
    cls: Type[BaseAlphaModel]
    description: str
    params: List[ParamSpec] = field(default_factory=list)

    # 'single' strategies take one symbol's OHLCV frame. 'multi' strategies take
    # a wide frame of several symbols and have a different input contract — the
    # backtest router must not hand them a single-symbol DataFrame.
    input_contract: Literal["single", "multi"] = "single"

    # Non-empty when a strategy is known to be unsound. Surfaced through the API
    # so a consumer can warn rather than silently trusting the numbers.
    caveat: Optional[str] = None

    def build(self, params: Optional[Dict[str, Any]] = None) -> BaseAlphaModel:
        """
        Instantiate the strategy, filling unspecified parameters with defaults.

        Unknown parameter names are rejected rather than ignored — a typo in a
        param name should fail loudly, not silently run with defaults.
        """
        supplied = dict(params or {})
        known = {p.name for p in self.params}
        unknown = set(supplied) - known
        if unknown:
            raise ValueError(
                f"Unknown parameter(s) for strategy '{self.id}': "
                f"{sorted(unknown)}. Valid: {sorted(known) or '(none)'}"
            )

        kwargs = {p.name: supplied.get(p.name, p.default) for p in self.params}
        # Coerce to the declared type — query strings and JSON both arrive loosely typed.
        for p in self.params:
            if kwargs[p.name] is None:
                continue
            if p.type == "int":
                kwargs[p.name] = int(kwargs[p.name])
            elif p.type == "float":
                kwargs[p.name] = float(kwargs[p.name])
        return self.cls(**kwargs)


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------

_SPECS: List[StrategySpec] = [
    StrategySpec(
        id="buy_and_hold",
        display_name="Buy and Hold",
        cls=BuyAndHoldStrategy,
        description="Enter on the first bar and hold. The baseline every strategy is measured against.",
        params=[],
    ),
    StrategySpec(
        id="mean_reversion",
        display_name="Mean Reversion",
        cls=MeanReversionStrategy,
        description="Fade moves beyond `threshold` standard deviations of a rolling mean.",
        params=[
            ParamSpec("window", "int", 20, "Lookback window", "Bars in the rolling mean", 2, 500),
            ParamSpec("threshold", "float", 1.5, "Z-score threshold", "Std devs from the mean to trigger", 0.1, 10.0),
        ],
    ),
    StrategySpec(
        id="ma_crossover",
        display_name="Moving Average Crossover",
        cls=MovingAverageCrossoverStrategy,
        description="Long when the short moving average is above the long one.",
        params=[
            ParamSpec("short_window", "int", 40, "Short window", "Must be smaller than the long window", 1, 500),
            ParamSpec("long_window", "int", 100, "Long window", "Must be larger than the short window", 2, 1000),
        ],
    ),
    StrategySpec(
        id="trend_following",
        display_name="Trend Following",
        cls=TrendFollowingStrategy,
        description="Follow the prevailing direction over a rolling window.",
        params=[
            ParamSpec("window", "int", 50, "Lookback window", "Bars used to measure trend", 2, 500),
        ],
    ),
    StrategySpec(
        id="rsi",
        display_name="RSI",
        cls=RSIStrategy,
        description="Buy oversold and sell overbought on the Relative Strength Index.",
        params=[
            ParamSpec("window", "int", 14, "RSI window", "Bars in the RSI calculation", 2, 200),
            ParamSpec("buy_threshold", "int", 30, "Buy below", "RSI level considered oversold", 1, 99),
            ParamSpec("sell_threshold", "int", 70, "Sell above", "RSI level considered overbought", 1, 99),
        ],
    ),
    StrategySpec(
        id="atr_breakout",
        display_name="ATR Breakout",
        cls=ATRBreakoutStrategy,
        description="Trade breakouts sized by Average True Range. Requires High/Low columns.",
        params=[
            ParamSpec("window", "int", 20, "ATR window", "Bars in the ATR calculation", 2, 200),
            ParamSpec("multiplier", "float", 2.0, "ATR multiplier", "Breakout distance in ATRs", 0.1, 10.0),
        ],
    ),
    StrategySpec(
        id="push_response",
        display_name="Push / Response",
        cls=PushResponseStrategy,
        description="Bins historical pushes and trades the conditional response.",
        params=[
            ParamSpec("tau", "int", 5, "Tau", "Bar interval for push and response", 1, 100),
            ParamSpec("training_window", "int", 100, "Training window", "Rolling window used to fit", 10, 2000),
            ParamSpec("num_bins", "int", 50, "Bins", "Number of push bins", 2, 200),
            ParamSpec("threshold", "float", 0.0, "Threshold", "Minimum response to act on", -1.0, 1.0),
        ],
    ),
    StrategySpec(
        id="ml_random_forest",
        display_name="Random Forest (ML)",
        cls=RandomForestStrategy,
        description="Random-forest classifier over lagged returns.",
        params=[
            ParamSpec("n_estimators", "int", 100, "Trees", "Number of trees in the forest", 1, 1000),
            ParamSpec("lookback_window", "int", 5, "Lookback", "Lagged returns used as features", 1, 100),
        ],
        caveat=(
            "Known look-ahead bias: the model trains on the full history before "
            "generating signals, so backtest results are optimistic and not "
            "achievable live. Covered by two xfail cases in the contract suite."
        ),
    ),
    # --- multi-asset: different input contract, NOT single-symbol frames ----
    StrategySpec(
        id="pairs_trading",
        display_name="Pairs Trading",
        cls=PairsTradingStrategy,
        description="Trade the spread between two cointegrated symbols.",
        params=[
            ParamSpec("window", "int", 20, "Lookback window", "Bars in the spread's rolling stats", 2, 500),
            ParamSpec("threshold", "float", 2.0, "Z-score threshold", "Spread std devs to trigger", 0.1, 10.0),
        ],
        input_contract="multi",
    ),
    StrategySpec(
        id="cointegrated_mean_reversion",
        display_name="Cointegrated Mean Reversion",
        cls=CointegratedMeanReversionStrategy,
        description="Mean-reversion on a weighted basket. Requires an explicit `weights` mapping.",
        params=[
            ParamSpec("window", "int", 20, "Lookback window", "Bars in the rolling stats", 2, 500),
            ParamSpec("threshold", "float", 2.0, "Z-score threshold", "Std devs to trigger", 0.1, 10.0),
        ],
        input_contract="multi",
        caveat="Requires a `weights` dict that this registry cannot default; not constructible from the API yet.",
    ),
    StrategySpec(
        id="basket_trading",
        display_name="Basket Trading",
        cls=BasketTradingStrategy,
        description="Periodically rebalance an equally weighted basket.",
        params=[
            ParamSpec(
                "rebalance_frequency", "str", "ME", "Rebalance frequency",
                # 'ME'/'QE', not 'M'/'Q': pandas 2.2 deprecated the bare
                # month/quarter aliases for resample and removes them in 3.0.
                # They are pure renames — identical period counts — so this
                # changes nothing except the FutureWarning it was emitting on
                # every rebalance. 'W' is unaffected.
                "Pandas offset alias: ME month-end, QE quarter-end, W weekly",
            ),
        ],
        input_contract="multi",
    ),
    StrategySpec(
        id="index_rebalancing",
        display_name="Index Rebalancing",
        cls=IndexRebalancingStrategy,
        description="Trade around scheduled index rebalance dates.",
        params=[
            ParamSpec(
                "rebalance_frequency", "str", "ME", "Rebalance frequency",
                "Pandas offset alias: ME month-end, QE quarter-end, W weekly",
            ),
        ],
        input_contract="multi",
    ),
]

REGISTRY: Dict[str, StrategySpec] = {s.id: s for s in _SPECS}


# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------

def all_strategies() -> List[StrategySpec]:
    """Every registered strategy, in registration order."""
    return list(_SPECS)


def single_asset_strategies() -> List[StrategySpec]:
    """Strategies that accept one symbol's OHLCV frame."""
    return [s for s in _SPECS if s.input_contract == "single"]


def multi_asset_strategies() -> List[StrategySpec]:
    """Strategies that accept a wide multi-symbol frame."""
    return [s for s in _SPECS if s.input_contract == "multi"]


def get(strategy_id: str) -> StrategySpec:
    """Look up a strategy, raising KeyError with the valid ids if unknown."""
    try:
        return REGISTRY[strategy_id]
    except KeyError:
        raise KeyError(
            f"Unknown strategy '{strategy_id}'. Valid ids: {sorted(REGISTRY)}"
        ) from None


def build(strategy_id: str, params: Optional[Dict[str, Any]] = None) -> BaseAlphaModel:
    """Instantiate a registered strategy by id."""
    return get(strategy_id).build(params)


# ---------------------------------------------------------------------------
# Test-suite compatibility
# ---------------------------------------------------------------------------
# tests/test_strategy_contract.py consumes these so the contract suite doubles
# as this registry's correctness check. The params below are deliberately small
# so the suite's 300-bar synthetic fixtures are long enough to exercise them.

_CONTRACT_TEST_PARAMS: Dict[str, Dict[str, Any]] = {
    "mean_reversion": {"window": 20, "threshold": 1.0},
    "ma_crossover": {"short_window": 10, "long_window": 30},
    "trend_following": {"window": 20},
    "rsi": {"window": 14, "buy_threshold": 30, "sell_threshold": 70},
    "atr_breakout": {"window": 14, "multiplier": 2.0},
    "push_response": {"tau": 5, "training_window": 100, "num_bins": 10},
    "ml_random_forest": {"n_estimators": 10, "lookback_window": 5},
}


def contract_test_cases() -> List[tuple[str, Callable[[], BaseAlphaModel]]]:
    """
    (id, factory) pairs for every single-asset strategy — the input
    tests/test_strategy_contract.py parametrises over.
    """
    return [
        (spec.id, (lambda s=spec: s.build(_CONTRACT_TEST_PARAMS.get(s.id))))
        for spec in single_asset_strategies()
    ]
