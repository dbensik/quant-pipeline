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

from alpha_models.asset_class_trend import AssetClassTrendFollowingStrategy
from alpha_models.atr_breakout import ATRBreakoutStrategy
from alpha_models.base_model import BaseAlphaModel
from alpha_models.basket_trading import BasketTradingStrategy
from alpha_models.buy_and_hold import BuyAndHoldStrategy
from alpha_models.cointegrated_mean_reversion import CointegratedMeanReversionStrategy
from alpha_models.index_rebalancing import IndexRebalancingStrategy
from alpha_models.mean_reversion import MeanReversionStrategy
from alpha_models.ml_random_forest import RandomForestStrategy
from alpha_models.momentum_allocation import MomentumAssetAllocationStrategy
from alpha_models.moving_average_crossover import MovingAverageCrossoverStrategy
from alpha_models.paired_switching import PairedSwitchingStrategy
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

    # HOW this strategy's output is assembled into the per-ticker dict the
    # PortfolioBacktester consumes. `input_contract` says whether a wide frame
    # goes IN; this says what shape comes OUT, and they are not the same
    # question — all four multi-asset strategies share input_contract="multi"
    # while returning three different output shapes.
    #
    # This was previously hardcoded as sets of strategy ids inside
    # api/routers/portfolio_backtest.py::_build_signals. Declaring it here is
    # what lets a new multi-asset strategy work without editing a router — and
    # the old arrangement failed UNSAFELY: an unlisted id fell through to the
    # per_symbol fallback and was handed each symbol's own frame in isolation,
    # so a cross-asset strategy silently compared nothing and still produced
    # plausible numbers.
    #
    #   per_symbol       one frame per symbol in, one 'signal' column out each.
    #                    Every single-asset strategy.
    #   wide_per_asset   wide frame in, ONE POSITION COLUMN PER ASSET out, no
    #                    'signal' column. Split into the per-ticker dict.
    #                    The shape allocation strategies want.
    #   wide_portfolio   wide frame in, ONE 'signal' column for the basket as a
    #                    single unit. Filed under the "Portfolio" key.
    #   calendar_shared  only the DatetimeIndex is read; emits a rebalance
    #                    schedule every symbol shares. Uses signal == 2.
    #   target           wide frame in, one TARGET column per asset out: the
    #                    position wanted as a signed fraction of equity, NaN =
    #                    unchanged. The only shape that can SHORT, so a run
    #                    needs borrow and margin terms (decision 48).
    #                    pairs_trading.
    signal_shape: Literal[
        "per_symbol", "wide_per_asset", "wide_portfolio", "calendar_shared", "target"
    ] = "per_symbol"

    # Non-empty when a strategy is known to be unsound, or when what it was
    # measured to do should be read before trusting a run (decisions 25, 28:
    # every single-asset strategy carries its 2010-2026 evidence, `measured()`).
    # Surfaced through the API so a consumer can warn rather than silently
    # trusting the numbers.
    caveat: Optional[str] = None

    # A sensible parameter sweep, used when a caller asks to optimize without
    # supplying a grid — chiefly the comparison endpoint, which tunes each
    # strategy before comparing so the ranking is not just measuring whose
    # DEFAULTS happened to suit the window.
    #
    # It lives here because it is a fact about the strategy, alongside its
    # params and input contract. It was previously `_OPTIMIZATION_GRIDS` inside
    # the Streamlit AnalysisController — a fifth copy of grid-search knowledge,
    # which also re-hardcoded the short<long constraint that
    # MovingAverageCrossoverStrategy already enforces.
    #
    # None means "no default sweep"; pass a grid explicitly. Deliberately not
    # invented for strategies where a sensible range is a guess.
    default_grid: Optional[Dict[str, List[Any]]] = None

    # How a multi-asset run weights each symbol when the caller passes no
    # weights (decision 32). The PortfolioBacktester buys weight x equity for a
    # symbol when it is first held, so the right default depends on how many
    # symbols the rule holds at once:
    #   equal      1/N each — every symbol may be held together (trend,
    #              rebalancing): in three of five sleeves is 60% invested.
    #   per_top_n  1/top_n each — the rule holds top_n at a time (momentum).
    #              Equal weights left it 30-42% invested (decision 29).
    #   full       1.0 each — one leg held at a time (paired switching).
    #              Equal weights left it 50% in cash.
    held_weight: Literal["equal", "per_top_n", "full"] = "equal"

    def default_weights(self, symbols: List[str], params: Dict[str, Any]) -> Dict[str, float]:
        """The weights a run uses when the caller supplies none."""
        if self.held_weight == "full":
            return {s: 1.0 for s in symbols}
        if self.held_weight == "per_top_n":
            effective = {p.name: params.get(p.name, p.default) for p in self.params}
            top_n = int(effective["top_n"])
            if top_n < 1:
                raise ValueError(f"top_n must be at least 1; got {top_n}.")
            return {s: 1.0 / min(top_n, len(symbols)) for s in symbols}
        return {s: 1.0 / len(symbols) for s in symbols}

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


def measured(finding: str) -> str:
    """What the 2010-2026 evaluation found at a strategy's defaults (decision 28).

    research/history-2010-backfill-2026-10-14.md, section 4: the 428 current
    index members and ETFs with bars from 2010, 2010-01 to 2026-10, through the
    API with costs and interest on cash; the 2020-2026 figure (decision 25,
    research/strategy-evaluation-2026-10-14.md) beside it. Medians; survivors
    only, so not 428 independent trials. Restated 2026-10-15 at next-open
    fills (decision 31, research/fill-timing-2026-10-15.md).
    """
    return f"Measured at its default parameters on 428 names, 2010-2026, with costs: {finding}"


def measured_crypto(finding: str) -> str:
    """What decision 35 found on crypto at a strategy's defaults.

    research/crypto-strategies-2026-10-15.md: the 14 coins with bars from 2018
    (survivors only, one correlated market), 2018-01 to 2026-10, next-open
    fills, costs and interest on cash. The edge it found is crash avoidance —
    2018 and 2022 — so it depends on the window containing such crashes.
    """
    return f"On crypto, 14 coins 2018-2026: {finding}"


def measured_allocation(finding: str) -> str:
    """What decision 29 found for a multi-asset allocation rule at its defaults.

    research/multi-asset-strategies-2026-10-15.md: ETF baskets fixed in advance
    (Faber's five: SPY EFA IEF VNQ DBC; seven: + GLD TLT) and SPY with TLT, IEF
    or AGG, 2011-01 to 2026-10, sized by weights (not the 2% buy cap), with
    costs and interest on cash, against the same basket equal-weighted.
    Restated 2026-10-15 at next-open fills (decision 31,
    research/fill-timing-2026-10-15.md), and 2026-10-20 with each fill in its
    own bar's equity (decision 52, research/portfolio-equity-lag-2026-10-20.md).
    """
    return f"Measured at its defaults on fixed ETF baskets, 2011-2026, with costs: {finding}"


def measured_short(finding: str) -> str:
    """What decision 47 found when a strategy's -1 exit was read as "go short".

    research/short-side-2026-10-19.md: the same 428 names (2010-2026) and 14
    coins (2018-2026) as decisions 28 and 35, in a prototype engine that
    reproduces Backtester to the cent with shorts off, borrow at 1% a year.
    Survivors only, which biases against shorts: the names a short wins on
    most are the ones Yahoo no longer serves.
    """
    return f"With its exit read as a short instead of cash: {finding}"


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
            ParamSpec(
                "threshold",
                "float",
                1.5,
                "Z-score threshold",
                "Std devs from the mean to trigger",
                0.1,
                10.0,
            ),
        ],
        default_grid={
            "window": [10, 20, 30, 40, 50],
            "threshold": [1.0, 1.5, 2.0, 2.5],
        },
        caveat=measured(
            "better Sharpe than buy and hold on 11% of names (18% in 2010-2019, 25% in "
            "2020-2026), 7.8 pp a year less return. Its shallower drawdown is time in cash "
            "(in the market 28% of the time); parameters fitted on 2010-2019 lost to buy "
            "and hold on 62% of names in 2020-2026."
        ),
    ),
    StrategySpec(
        id="ma_crossover",
        display_name="Moving Average Crossover",
        cls=MovingAverageCrossoverStrategy,
        description="Long when the short moving average is above the long one.",
        params=[
            ParamSpec(
                "short_window",
                "int",
                40,
                "Short window",
                "Must be smaller than the long window",
                1,
                500,
            ),
            ParamSpec(
                "long_window",
                "int",
                100,
                "Long window",
                "Must be larger than the short window",
                2,
                1000,
            ),
        ],
        # Combinations where short >= long are rejected by the strategy's own
        # ValueError and reported as skipped — no constraint is restated here.
        default_grid={
            "short_window": [10, 20, 30],
            "long_window": [50, 100, 200],
        },
        caveat=measured(
            "better Sharpe than buy and hold on 12% of names (14% in 2010-2019, 25% in "
            "2020-2026), 6.8 pp a year less return. On ETFs 6 of 11 (1 of 11 in 2010-2019, "
            "8 in 2020-2026). Parameters fitted on 2010-2019 lost to buy and hold on 72% of "
            "names in 2020-2026."
        )
        + " "
        + measured_short(
            "a better Sharpe than long-only on 1 of 428 names and none of the 14 coins (decision 47)."
        ),
    ),
    StrategySpec(
        id="trend_following",
        display_name="Trend Following",
        cls=TrendFollowingStrategy,
        description="Follow the prevailing direction over a rolling window.",
        params=[
            ParamSpec("window", "int", 50, "Lookback window", "Bars used to measure trend", 2, 500),
        ],
        caveat=measured(
            "better Sharpe than buy and hold on 6% of names (7% in 2010-2019, 20% in "
            "2020-2026), 9.4 pp a year less return."
        )
        + " "
        + measured_crypto(
            "beat buy and hold on 13 by sitting out the 2018 and 2022 crashes (median "
            "+23 pp a year); it lost in most rising years."
        )
        + " "
        + measured_short(
            "a better Sharpe than long-only on no equity and 2 of 14 coins (7 in 2018-2021, 3 in "
            "2022-2026; decision 47)."
        ),
    ),
    StrategySpec(
        id="rsi",
        display_name="RSI",
        cls=RSIStrategy,
        description="Buy oversold and sell overbought on the Relative Strength Index.",
        params=[
            ParamSpec("window", "int", 14, "RSI window", "Bars in the RSI calculation", 2, 200),
            ParamSpec(
                "buy_threshold", "int", 30, "Buy below", "RSI level considered oversold", 1, 99
            ),
            ParamSpec(
                "sell_threshold", "int", 70, "Sell above", "RSI level considered overbought", 1, 99
            ),
        ],
        caveat=measured(
            "better Sharpe than buy and hold on 26% of names (23% in 2010-2019, 49% in "
            "2020-2026), 5.3 pp a year less return, with a shallower drawdown on 97%. Which "
            "names it beats does not persist from one decade to the next."
        )
        + " "
        + measured_short(
            "it lost the whole account on 209 of 428 names and all 14 coins, shorting "
            "at overbought and holding to oversold (decision 47)."
        ),
    ),
    StrategySpec(
        id="atr_breakout",
        display_name="ATR Breakout",
        cls=ATRBreakoutStrategy,
        description="Trade breakouts sized by Average True Range. Requires High/Low columns.",
        params=[
            ParamSpec("window", "int", 20, "ATR window", "Bars in the ATR calculation", 2, 200),
            ParamSpec(
                "multiplier", "float", 2.0, "ATR multiplier", "Breakout distance in ATRs", 0.1, 10.0
            ),
        ],
        caveat=measured(
            "better Sharpe than buy and hold on 7% of names (8% in 2010-2019, 20% in "
            "2020-2026), 10.8 pp a year less return. Its drawdown (-37% against -53%) is "
            "time in cash: in the market 31% of the time."
        )
        + " "
        + measured_crypto(
            "beat buy and hold on 12 by sitting out the 2018 and 2022 crashes (median "
            "+25 pp a year, worst drawdown -62% against -95%); it lost in most rising years."
        )
        + " "
        + measured_short(
            "a better Sharpe than long-only on no equity and none of the 14 coins (decision 47)."
        ),
    ),
    StrategySpec(
        id="push_response",
        display_name="Push / Response",
        cls=PushResponseStrategy,
        description="Bins historical pushes and trades the conditional response.",
        params=[
            ParamSpec("tau", "int", 5, "Tau", "Bar interval for push and response", 1, 100),
            ParamSpec(
                "training_window",
                "int",
                100,
                "Training window",
                "Rolling window used to fit",
                10,
                2000,
            ),
            ParamSpec("num_bins", "int", 50, "Bins", "Number of push bins", 2, 200),
            ParamSpec(
                "threshold", "float", 0.0, "Threshold", "Minimum response to act on", -1.0, 1.0
            ),
        ],
        caveat=measured(
            "loses to buy and hold on every name (96% in 2020-2026), 17.7 pp a year less "
            "return, with a deeper drawdown (-76% against -53%)."
        )
        + " "
        + measured_short(
            "its -1 is a prediction of a fall, and acted on it had a better Sharpe than long-only "
            "on no equity and no coin (decision 47)."
        ),
    ),
    StrategySpec(
        id="ml_random_forest",
        display_name="Random Forest (ML)",
        cls=RandomForestStrategy,
        description=(
            "Random-forest classifier over lagged returns, walk-forward: retrained "
            "every `retrain_every` bars on the rows whose outcome is already known."
        ),
        params=[
            ParamSpec(
                "n_estimators", "int", 100, "Trees", "Number of trees in the forest", 1, 1000
            ),
            ParamSpec(
                "lookback_window", "int", 5, "Lookback", "Lagged returns used as features", 1, 100
            ),
            ParamSpec(
                "train_window",
                "int",
                252,
                "First training",
                "Rows before the first prediction (flat until then)",
                2,
                5000,
            ),
            ParamSpec("retrain_every", "int", 63, "Retrain every", "Bars between refits", 1, 1000),
        ],
        caveat=(
            "Flat for the first `train_window` bars while it gathers training data. "
            "Until 2026-10-14 it trained on the full history (look-ahead). "
            + measured(
                "loses to buy and hold on 427 of 428 names (96% in 2020-2026), 16.4 pp a year less "
                "return."
            )
        ),
    ),
    # --- multi-asset: different input contract, NOT single-symbol frames ----
    StrategySpec(
        id="pairs_trading",
        display_name="Pairs Trading",
        cls=PairsTradingStrategy,
        description=(
            "Long one leg and short the other when a cointegrated spread diverges; out "
            "when it reverts. Refits the hedge ratio and re-tests cointegration on the "
            "trailing window."
        ),
        params=[
            ParamSpec(
                "formation",
                "int",
                252,
                "Formation window",
                "Bars each fit and test reads",
                30,
                2000,
            ),
            ParamSpec("refit", "int", 21, "Refit every", "Bars between refits", 1, 500),
            ParamSpec("entry", "float", 2.0, "Entry z", "Spread std devs to enter", 0.1, 10.0),
            ParamSpec("exit", "float", 0.0, "Exit z", "Out when z crosses back past it", 0.0, 10.0),
            ParamSpec("stop", "float", 4.0, "Stop z", "Out when z passes it", 0.2, 20.0),
            ParamSpec(
                "max_pvalue",
                "float",
                0.05,
                "Max p-value",
                "Engle-Granger p-value below which the pair is traded",
                0.001,
                0.5,
            ),
        ],
        input_contract="multi",
        signal_shape="target",
        caveat=(
            "Shorts one leg: borrow and margin are charged as the run's terms say, at one "
            "flat rate (no hard-to-borrow names). Measured at its defaults (decision 48): on "
            "33 ETFs, 2013-2026, the 5 most cointegrated pairs of the prior three years, "
            "re-chosen yearly, with costs and 1% borrow, had a Sharpe of -0.27 against cash "
            "with a beta of 0.003. That beat 50 of 100 random pair sets: the selection added "
            "nothing. In a trade 10% of the time; 42 of the 70 chosen pairs never traded."
        ),
    ),
    StrategySpec(
        id="cointegrated_mean_reversion",
        display_name="Cointegrated Mean Reversion",
        cls=CointegratedMeanReversionStrategy,
        description="Mean-reversion on a weighted basket. Requires an explicit `weights` mapping.",
        params=[
            ParamSpec("window", "int", 20, "Lookback window", "Bars in the rolling stats", 2, 500),
            ParamSpec(
                "threshold", "float", 2.0, "Z-score threshold", "Std devs to trigger", 0.1, 10.0
            ),
        ],
        input_contract="multi",
        signal_shape="wide_portfolio",
        caveat="Requires a `weights` dict that this registry cannot default; not constructible from the API yet.",
    ),
    StrategySpec(
        id="basket_trading",
        display_name="Basket Trading",
        cls=BasketTradingStrategy,
        description="Periodically rebalance an equally weighted basket.",
        params=[
            ParamSpec(
                "rebalance_frequency",
                "str",
                "ME",
                "Rebalance frequency",
                # 'ME'/'QE', not 'M'/'Q': pandas 2.2 deprecated the bare
                # month/quarter aliases for resample and removes them in 3.0.
                # They are pure renames — identical period counts — so this
                # changes nothing except the FutureWarning it was emitting on
                # every rebalance. 'W' is unaffected.
                "Pandas offset alias: ME month-end, QE quarter-end, W weekly",
            ),
        ],
        input_contract="multi",
        signal_shape="calendar_shared",
    ),
    StrategySpec(
        id="index_rebalancing",
        display_name="Index Rebalancing",
        cls=IndexRebalancingStrategy,
        description="Trade around scheduled index rebalance dates.",
        params=[
            ParamSpec(
                "rebalance_frequency",
                "str",
                "ME",
                "Rebalance frequency",
                "Pandas offset alias: ME month-end, QE quarter-end, W weekly",
            ),
        ],
        input_contract="multi",
        signal_shape="calendar_shared",
    ),
    # --- Asset allocation ---------------------------------------------------
    # These three CHOOSE which sleeves to hold, which is what separates them
    # from basket_trading: that rebalances to weights the caller fixed, these
    # decide membership. All emit `wide_per_asset`, so they need no router
    # change — the point of declaring signal_shape in Phase 2.
    StrategySpec(
        id="paired_switching",
        display_name="Paired Switching",
        cls=PairedSwitchingStrategy,
        description=(
            "Hold whichever of two assets had the better trailing return, "
            "reassessed each quarter. Classically equities vs bonds."
        ),
        params=[
            ParamSpec(
                "lookback",
                "int",
                63,
                "Lookback window",
                "Bars in the trailing-return comparison; 63 ≈ one quarter",
                2,
                756,
            ),
            ParamSpec(
                "rebalance_frequency",
                "str",
                "QE",
                "Rebalance frequency",
                "W, ME (month end), QE (quarter end) or YE",
            ),
        ],
        held_weight="full",
        input_contract="multi",
        signal_shape="wide_per_asset",
        caveat=(
            "Trades exactly two symbols, and only one leg is ever held, so each "
            "leg defaults to a weight of 1.0 (fully invested in whichever is held). "
            + measured_allocation(
                "on SPY with TLT, IEF or AGG it lost to a static 50/50 of the same pair in both "
                "decades (SPY/TLT Sharpe 0.73 against 1.41 in 2011-2019, -0.18 against 0.17 in "
                "2020-2026) and took the full 2020 crash (-34%)."
            )
        ),
        default_grid={"lookback": [21, 63, 126, 252]},
    ),
    StrategySpec(
        id="asset_class_trend",
        display_name="Asset Class Trend Following",
        cls=AssetClassTrendFollowingStrategy,
        description=(
            "Hold each sleeve while it trades above its own long moving "
            "average, otherwise hold cash. Judged per sleeve, not ranked."
        ),
        params=[
            ParamSpec(
                "window",
                "int",
                200,
                "Moving average window",
                "Bars per sleeve's average; 200 ≈ the classic 10-month rule",
                2,
                756,
            ),
            ParamSpec(
                "rebalance_frequency",
                "str",
                "ME",
                "Rebalance frequency",
                "W, ME (month end), QE (quarter end) or YE",
            ),
        ],
        input_contract="multi",
        signal_shape="wide_per_asset",
        caveat=measured_allocation(
            "on Faber's five and seven ETFs it cut the basket's worst drawdown (-15% "
            "against -26%; -8% against -20%) at 1-2 pp a year of return, with a Sharpe "
            "level with the basket (0.45 against 0.46; 0.52 against 0.52)."
        )
        + " On BTC/ETH 2018-2026 it cut the worst drawdown from -87% to -59% and was flat in "
        "2018, when holding both lost 75%.",
        default_grid={"window": [50, 100, 200, 300]},
    ),
    StrategySpec(
        id="momentum_allocation",
        display_name="Momentum Asset Allocation",
        cls=MomentumAssetAllocationStrategy,
        description=(
            "Hold the strongest `top_n` sleeves by trailing return, reassessed "
            "monthly. Rotates between asset classes rather than de-risking."
        ),
        params=[
            ParamSpec(
                "lookback",
                "int",
                126,
                "Lookback window",
                "Bars in the trailing-return ranking; 126 ≈ six months",
                2,
                756,
            ),
            ParamSpec(
                "top_n",
                "int",
                2,
                "Sleeves held",
                "How many of the supplied sleeves to hold; must be fewer than the number supplied",
                1,
                20,
            ),
            ParamSpec(
                "rebalance_frequency",
                "str",
                "ME",
                "Rebalance frequency",
                "W, ME (month end), QE (quarter end) or YE",
            ),
        ],
        held_weight="per_top_n",
        input_contract="multi",
        signal_shape="wide_per_asset",
        caveat=measured_allocation(
            "beat its basket on Faber's five ETFs (Sharpe 0.62 against 0.46) but not on "
            "seven (0.49 against 0.52), with no drawdown protection (2022: -12%); its "
            "2020-2026 Sharpe ranged 0.22-0.88 across its grid."
        ),
        default_grid={"lookback": [63, 126, 252], "top_n": [1, 2, 3]},
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
        raise KeyError(f"Unknown strategy '{strategy_id}'. Valid ids: {sorted(REGISTRY)}") from None


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
    "ml_random_forest": {
        "n_estimators": 10,
        "lookback_window": 5,
        "train_window": 60,
        "retrain_every": 20,
    },
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
