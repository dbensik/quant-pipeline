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
    idx = pd.bdate_range("2022-01-03", periods=len(close))
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
    idx = pd.bdate_range("2022-01-03", periods=N)
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
