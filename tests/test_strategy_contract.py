"""
Strategy contract test harness.

Runs EVERY single-asset BaseAlphaModel strategy over synthetic price fixtures
and asserts the signal contract:

  1. generate_signals() returns a DataFrame with a 'signal' column
  2. The signal index matches the price index (no rows invented or dropped)
  3. Signal values are only -1, 0, or +1 (NaN tolerated only because the
     Backtester ffill().fillna(0)'s them — but they must not be *all* NaN)
  4. NO LOOK-AHEAD: the signal at time t must not change when future bars
     are removed. This is the single most important invariant in the
     framework — a strategy that peeks at the future produces beautiful,
     meaningless backtests.

Multi-asset strategies (pairs, cointegrated, basket, index rebalancing) have
a different input contract and are exercised separately at the bottom.

Any new BaseAlphaModel subclass with a default-constructible signature is
picked up automatically via SINGLE_ASSET_STRATEGIES.

FIXTURES ARE TIMEZONE-AWARE, DELIBERATELY. TimescaleDB returns tz-aware
timestamps and `api/frames.py` builds strategy input straight from them, so a
tz-naive fixture tests a frame shape production never produces. That gap was
not theoretical: rebalance-date arithmetic that built its result via `.values`
silently dropped the tz, and the resulting naive timestamps raised KeyError
against the real index. It passed all 665 tests and failed on the first real
backtest. With UTC fixtures the same bug fails six tests here.

Keep new fixtures tz-aware; `test_fixtures_are_timezone_aware` enforces it.
"""

import numpy as np
import pandas as pd
import pytest

from alpha_models import registry

N = 300
TRUNCATE = 30          # bars removed for the look-ahead test
WARMUP = 150           # ignore early bars where rolling windows are filling

# (id, factory) pairs come from alpha_models/registry.py, the single source of
# truth for strategy identity — so registering a strategy there automatically
# subjects it to this contract, and this suite doubles as the registry's own
# correctness check. Test-sized parameters live in the registry alongside the
# production defaults (_CONTRACT_TEST_PARAMS).
SINGLE_ASSET_STRATEGIES = registry.contract_test_cases()

STRATEGY_IDS = [s[0] for s in SINGLE_ASSET_STRATEGIES]


def _make_ohlcv(close: np.ndarray) -> pd.DataFrame:
    """Build a plausible OHLCV frame around a close series."""
    idx = pd.bdate_range("2022-01-03", periods=len(close), tz="UTC")
    close = pd.Series(close, index=idx)
    rng = np.random.default_rng(7)
    spread = np.abs(rng.normal(0, 0.005, len(close))) * close
    df = pd.DataFrame(
        {
            "Open": close.shift(1).fillna(close.iloc[0]),
            "High": close + spread,
            "Low": close - spread,
            "Close": close,
            "Volume": rng.integers(1_000_000, 5_000_000, len(close)).astype(float),
        }
    )
    return df


def _fixtures() -> dict:
    rng = np.random.default_rng(42)
    t = np.arange(N)
    fixtures = {
        # steady uptrend + noise
        "trend": 100 * np.exp(0.001 * t + rng.normal(0, 0.01, N).cumsum() * 0.3),
        # oscillates around 100
        "mean_reverting": 100 + 5 * np.sin(t / 10.0) + rng.normal(0, 0.5, N),
        # flat with tiny noise
        "flat": 100 + rng.normal(0, 0.05, N),
        # trend with a -15% single-bar gap in the middle
        "gap": np.concatenate(
            [
                100 * np.exp(0.001 * np.arange(N // 2)),
                100 * np.exp(0.001 * (N // 2)) * 0.85 * np.exp(0.001 * np.arange(N - N // 2)),
            ]
        ),
    }
    return {name: _make_ohlcv(vals) for name, vals in fixtures.items()}


FIXTURES = _fixtures()
FIXTURE_IDS = list(FIXTURES.keys())


@pytest.fixture(params=SINGLE_ASSET_STRATEGIES, ids=STRATEGY_IDS)
def strategy_factory(request):
    return request.param[1]


@pytest.mark.parametrize("fixture_name", FIXTURE_IDS)
class TestSignalContract:
    def test_returns_signal_frame_aligned_to_input(self, strategy_factory, fixture_name):
        df = FIXTURES[fixture_name]
        signals = strategy_factory().generate_signals(price_data=df.copy())
        assert isinstance(signals, pd.DataFrame), "must return a DataFrame"
        assert "signal" in signals.columns, "must contain a 'signal' column"
        assert signals.index.equals(df.index), "signal index must match price index"

    def test_signal_values_are_valid(self, strategy_factory, fixture_name):
        df = FIXTURES[fixture_name]
        signals = strategy_factory().generate_signals(price_data=df.copy())["signal"]
        non_nan = signals.dropna()
        assert len(non_nan) > 0, "signals must not be all-NaN"
        invalid = set(non_nan.unique()) - {-1, 0, 1, -1.0, 0.0, 1.0}
        assert not invalid, f"invalid signal values: {invalid}"


LOOK_AHEAD_XFAILS = {
    # Trains one RandomForest on the ENTIRE history then predicts historically —
    # admitted in a code comment ("should use Walk-Forward execution to avoid
    # look-ahead bias") but never fixed. Its backtests are invalid until it is
    # rewritten walk-forward. Tracked in the vault Tasks database (2026-07-31).
    # strict=True: if someone fixes it, this xfail fails and must be removed.
    "ml_random_forest",
}


@pytest.mark.parametrize("fixture_name", ["trend", "mean_reverting"])
def test_no_look_ahead(strategy_factory, fixture_name, request):
    strategy_id = request.node.callspec.id.split("-")[0]
    if strategy_id in LOOK_AHEAD_XFAILS:
        request.node.add_marker(
            pytest.mark.xfail(
                strict=True,
                reason="known look-ahead: trains on full history (see LOOK_AHEAD_XFAILS)",
            )
        )
    """
    Signals up to time t must be identical whether or not bars after t exist.
    Strategies that train on the full history and then emit historical
    signals (look-ahead) fail here — and their backtests are meaningless.
    """
    df = FIXTURES[fixture_name]
    model_full = strategy_factory()
    model_trunc = strategy_factory()

    full = model_full.generate_signals(price_data=df.copy())["signal"]
    trunc = model_trunc.generate_signals(price_data=df.iloc[:-TRUNCATE].copy())["signal"]

    # compare on the overlap, past the warmup zone
    overlap = trunc.index[WARMUP:]
    a = full.loc[overlap].fillna(0)
    b = trunc.loc[overlap].fillna(0)
    diff = (a != b)
    assert not diff.any(), (
        f"LOOK-AHEAD: removing the last {TRUNCATE} bars changed "
        f"{int(diff.sum())} earlier signals (first at {diff.idxmax()})"
    )


# ---------------------------------------------------------------------------
# Multi-asset strategies — separate input contract (wide close-price frames)
# ---------------------------------------------------------------------------

def _make_wide_frame(n_assets: int = 3) -> pd.DataFrame:
    rng = np.random.default_rng(11)
    idx = pd.bdate_range("2022-01-03", periods=N, tz="UTC")
    data = {}
    for i in range(n_assets):
        drift = 0.0005 * (i + 1)
        data[f"ASSET{i}"] = 100 * np.exp(
            drift * np.arange(N) + rng.normal(0, 0.01, N).cumsum()
        )
    return pd.DataFrame(data, index=idx)


def test_cointegrated_mean_reversion_contract():
    from alpha_models.cointegrated_mean_reversion import CointegratedMeanReversionStrategy

    wide = _make_wide_frame(3)
    weights = {c: 1.0 / 3 for c in wide.columns}
    model = CointegratedMeanReversionStrategy(weights=weights, window=20, threshold=1.0)
    signals = model.generate_signals(price_data=wide)
    assert "signal" in signals.columns
    non_nan = signals["signal"].dropna()
    assert len(non_nan) > 0
    assert set(non_nan.unique()) <= {-1, 0, 1, -1.0, 0.0, 1.0}


def test_pairs_trading_contract():
    """
    NOTE: PairsTradingStrategy has a DIFFERENT output contract from the
    single-asset strategies — it returns one position column per leg
    (consumed by the portfolio backtester), not a 'signal' column.
    This is an interface inconsistency worth unifying eventually; until
    then, this test pins the actual behavior.
    """
    from alpha_models.pairs_trading import PairsTradingStrategy

    wide = _make_wide_frame(2)
    model = PairsTradingStrategy(window=20, threshold=1.5)
    signals = model.generate_signals(price_data=wide)
    assert list(signals.columns) == list(wide.columns), "one position column per leg"
    assert signals.index.equals(wide.index)
    vals = set(pd.unique(signals.values.ravel())) - {np.nan}
    assert vals <= {-1, 0, 1, -1.0, 0.0, 1.0}, f"invalid position values: {vals}"


# ---------------------------------------------------------------------------
# Look-ahead, multi-asset
# ---------------------------------------------------------------------------
# Until now test_no_look_ahead parametrised over SINGLE_ASSET_STRATEGIES only,
# so NONE of the four multi-asset strategies had ever been checked — and
# look-ahead is the defect that makes a backtest meaningless rather than merely
# wrong. `ml_random_forest` is in LOOK_AHEAD_XFAILS precisely because this check
# catches it; the multi-asset strategies had no equivalent guard at all.
#
# Factories are explicit rather than spec.build() because
# cointegrated_mean_reversion takes a `weights` dict that is not a ParamSpec and
# so cannot be constructed from the registry — the caveat the registry records.

def _multi_wide_cases():
    """
    (id, factory) for strategies whose input is a WIDE close frame.

    Parameters are deliberately smaller than the production defaults so the
    300-bar synthetic fixtures actually exercise them — asset_class_trend
    defaults to a 200-bar average, which over 300 bars would leave almost no
    decided rebalance dates and make these tests look green while asserting
    nothing.
    """
    from alpha_models.asset_class_trend import AssetClassTrendFollowingStrategy
    from alpha_models.cointegrated_mean_reversion import CointegratedMeanReversionStrategy
    from alpha_models.momentum_allocation import MomentumAssetAllocationStrategy
    from alpha_models.paired_switching import PairedSwitchingStrategy
    from alpha_models.pairs_trading import PairsTradingStrategy

    return [
        ("pairs_trading", lambda: PairsTradingStrategy(window=20, threshold=1.5)),
        (
            "cointegrated_mean_reversion",
            lambda: CointegratedMeanReversionStrategy(
                weights={"ASSET0": 0.5, "ASSET1": 0.5}, window=20, threshold=1.0
            ),
        ),
        (
            "paired_switching",
            lambda: PairedSwitchingStrategy(lookback=20, rebalance_frequency="ME"),
        ),
        (
            "asset_class_trend",
            lambda: AssetClassTrendFollowingStrategy(
                window=50, rebalance_frequency="ME"
            ),
        ),
        (
            "momentum_allocation",
            lambda: MomentumAssetAllocationStrategy(
                lookback=20, top_n=1, rebalance_frequency="ME"
            ),
        ),
    ]


def _multi_calendar_cases():
    """(id, factory) for strategies that read only the DatetimeIndex."""
    from alpha_models.basket_trading import BasketTradingStrategy
    from alpha_models.index_rebalancing import IndexRebalancingStrategy

    return [
        ("basket_trading", lambda: BasketTradingStrategy(rebalance_frequency="ME")),
        ("index_rebalancing", lambda: IndexRebalancingStrategy(rebalance_frequency="ME")),
    ]


@pytest.mark.parametrize(
    "strategy_id,factory", _multi_wide_cases(), ids=[c[0] for c in _multi_wide_cases()]
)
def test_no_look_ahead_multi_asset_wide(strategy_id, factory):
    """
    Signals up to time t must not change when bars after t are removed —
    the same rule the single-asset suite enforces, on the wide contract.
    """
    wide = _make_wide_frame(2)

    full = factory().generate_signals(price_data=wide.copy())
    trunc = factory().generate_signals(price_data=wide.iloc[:-TRUNCATE].copy())

    overlap = trunc.index[WARMUP:]
    # wide_per_asset returns one column per asset; wide_portfolio returns
    # 'signal'. Compare every column either way.
    a = full.loc[overlap].fillna(0)
    b = trunc.loc[overlap].fillna(0)
    assert list(a.columns) == list(b.columns)
    diff = (a != b)
    assert not diff.to_numpy().any(), (
        f"LOOK-AHEAD in {strategy_id}: removing the last {TRUNCATE} bars "
        f"changed {int(diff.to_numpy().sum())} earlier signals"
    )


@pytest.mark.parametrize(
    "strategy_id,factory",
    _multi_calendar_cases(),
    ids=[c[0] for c in _multi_calendar_cases()],
)
def test_no_look_ahead_multi_asset_calendar(strategy_id, factory):
    """
    A rebalance schedule is derived from the calendar, so truncating must not
    move any rebalance date within the surviving window.

    Compared over the FULL truncated index, with no warmup or final-period
    carve-out — a calendar schedule has no rolling window to warm up, and both
    strategies agree exactly over the whole overlap, so a carve-out would only
    weaken the assertion. Verified non-vacuous against a strategy that anchors
    its schedule to `price_data.index[-1]` instead of to calendar boundaries:
    that one shifts 26 dates here and is caught.
    """
    df = FIXTURES["trend"]

    full = factory().generate_signals(price_data=df.copy())["signal"]
    trunc = factory().generate_signals(price_data=df.iloc[:-TRUNCATE].copy())["signal"]

    overlap = trunc.index
    a = full.loc[overlap].fillna(0)
    b = trunc.loc[overlap].fillna(0)
    diff = (a != b)
    assert not diff.any(), (
        f"LOOK-AHEAD in {strategy_id}: removing the last {TRUNCATE} bars "
        f"changed {int(diff.sum())} earlier rebalance dates "
        f"(first at {diff.idxmax()})"
    )


# ---------------------------------------------------------------------------
# Signal VALUES per declared shape
# ---------------------------------------------------------------------------
# test_signal_values_are_valid asserts {-1, 0, 1}, but only over
# SINGLE_ASSET_STRATEGIES. basket_trading and index_rebalancing emit 2.0
# ("rebalance to target weights"), which is outside that set and had therefore
# never been asserted anywhere. These pin what each shape is allowed to emit,
# so 2 is sanctioned deliberately rather than by omission.

@pytest.mark.parametrize(
    "strategy_id,factory", _multi_wide_cases(), ids=[c[0] for c in _multi_wide_cases()]
)
def test_wide_shapes_emit_only_directional_values(strategy_id, factory):
    signals = factory().generate_signals(price_data=_make_wide_frame(2))
    values = set(pd.unique(signals.values.ravel())) - {np.nan}
    assert values <= {-1, 0, 1, -1.0, 0.0, 1.0}, (
        f"{strategy_id} emitted {values - {-1, 0, 1, -1.0, 0.0, 1.0}}"
    )


@pytest.mark.parametrize(
    "strategy_id,factory",
    _multi_calendar_cases(),
    ids=[c[0] for c in _multi_calendar_cases()],
)
def test_calendar_shape_emits_the_rebalance_code(strategy_id, factory):
    """
    2.0 means "rebalance to target weights" and is read as such by
    PortfolioBacktester.run. It is NOT a directional signal, and must never be
    mixed with -1/1 — the backtester's `signal == 2` branch sizes to
    target_weights while `signal == 1` opens a new position instead.
    """
    signals = factory().generate_signals(price_data=FIXTURES["trend"])["signal"]
    values = set(signals.dropna().unique())
    assert values <= {0, 2, 0.0, 2.0}, f"{strategy_id} emitted {values}"
    assert 2.0 in values, "a rebalance schedule that never rebalances is broken"


def test_every_registered_strategy_declares_a_wired_shape():
    """
    The router dispatches on signal_shape. A spec carrying a shape the router
    does not implement would silently fall through to the per-symbol branch —
    the exact failure the id-dispatch version had.
    """
    wired = {"per_symbol", "wide_per_asset", "wide_portfolio", "calendar_shared"}
    for spec in registry.all_strategies():
        assert spec.signal_shape in wired, f"{spec.id}: unwired shape"
        if spec.input_contract == "single":
            assert spec.signal_shape == "per_symbol", (
                f"{spec.id} is single-asset but declares {spec.signal_shape}"
            )
        else:
            assert spec.signal_shape != "per_symbol", (
                f"{spec.id} is multi-asset but declares per_symbol, which would "
                "hand it each symbol's frame in isolation"
            )


# ---------------------------------------------------------------------------
# Fixture convention
# ---------------------------------------------------------------------------

def test_fixtures_are_timezone_aware():
    """
    Guards the convention rather than a strategy.

    Strategy input in production comes from TimescaleDB via api/frames.py and is
    tz-aware. A tz-naive fixture exercises a frame shape that never occurs, and
    that is exactly how a rebalance-date bug survived the whole suite and then
    raised KeyError on the first real backtest. If someone drops the tz from
    these builders, the suite goes quietly back to testing the wrong thing —
    so it fails here instead.
    """
    for name, frame in FIXTURES.items():
        assert frame.index.tz is not None, f"OHLCV fixture {name!r} is tz-naive"

    wide = _make_wide_frame(2)
    assert wide.index.tz is not None, "wide fixture is tz-naive"


# ---------------------------------------------------------------------------
# Calendar strategies must not skip weekend period-ends
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "strategy_id,factory",
    _multi_calendar_cases(),
    ids=[c[0] for c in _multi_calendar_cases()],
)
def test_calendar_strategies_rebalance_every_month(strategy_id, factory):
    """
    THE REGRESSION. Both strategies used
    `index.intersection(resample("ME").last().index)`, which labels each group
    with the CALENDAR month end. That is a Saturday or Sunday about a third of
    the time, and a weekend label is not in a trading-day index, so the
    rebalance was silently dropped.

    Measured on SPY over 2015-2026 before the fix: 97 monthly rebalances fired
    where 139 were due — 42 missed. April, July and December 2022 are three
    such months.

    Asserting a rebalance in EVERY completed month, not just a count, because a
    count can be right while the wrong months are chosen.
    """
    index = pd.bdate_range("2022-01-03", "2022-12-30", tz="UTC")
    frame = pd.DataFrame({"Close": 100.0}, index=index)

    signals = factory().generate_signals(price_data=frame)["signal"]
    months = {d.month for d in signals.index[signals == 2.0]}

    # Twelve months of data, minus December: its period is the one the final
    # bar falls in, so it is not yet known to be complete.
    assert months == set(range(1, 12)), f"missing months: {set(range(1, 12)) - months}"
