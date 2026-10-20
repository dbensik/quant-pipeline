"""
pairs_trading, rebuilt as targets (decision 48, research/short-engine-2026-10-19.md).

Fixtures: B is a seeded random walk and A = 10 + 2B + small noise, so the pair
is cointegrated with a hedge ratio near 2, with the noise nearly gone after
the formation window, so a shock added to A moves z by a known amount. `refit` is set past the end of
each frame, so one fit (at bar formation - 1) governs every bar after it and
z is predictable.
"""

import numpy as np
import pandas as pd
import pytest

from alpha_models.pairs_trading import LEG, PairsTradingStrategy

N = 200
FORMATION = 100


def pair(shock=None, seed=3, cointegrated=True):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2024-01-02", periods=N, tz="UTC")
    b = 50 + np.cumsum(rng.normal(0, 1, N))
    if cointegrated:
        # Noisy in the formation window, quiet after it: out of sample z
        # stays near 0 and moves only where a shock puts it.
        noise = rng.normal(0, 0.5, N)
        noise[FORMATION:] *= 0.05
        a = 10 + 2 * b + noise
    else:
        a = 100 + np.cumsum(rng.normal(0, 1, N))
    if shock is not None:
        a = a + shock
    return pd.DataFrame({"A": a, "B": b}, index=idx)


def strategy(**kw):
    return PairsTradingStrategy(**{"formation": FORMATION, "refit": 10_000, **kw})


def shock(at, size, length):
    s = np.zeros(N)
    s[at : at + length] = size
    return s


def residual_sd(frame):
    w = frame.iloc[:FORMATION]
    beta, alpha = np.polyfit(w["B"], w["A"], 1)
    return (w["A"] - alpha - beta * w["B"]).std(ddof=1)


def decisions(targets):
    return targets.dropna(how="all")


def test_a_spread_above_entry_shorts_a_and_buys_b_then_exits_at_the_mean():
    sd = residual_sd(pair())
    targets = strategy().generate_signals(pair(shock(150, 3 * sd, 5)))
    d = decisions(targets)
    assert list(d.index) == [targets.index[150], targets.index[155]]
    assert list(d.iloc[0]) == [-LEG, LEG]
    assert list(d.iloc[1]) == [0.0, 0.0]


def test_a_spread_below_entry_buys_a_and_shorts_b():
    sd = residual_sd(pair())
    d = decisions(strategy().generate_signals(pair(shock(150, -3 * sd, 5))))
    assert list(d.iloc[0]) == [LEG, -LEG]


@pytest.mark.parametrize("side", [1, -1])
def test_it_never_enters_beyond_the_stop(side):
    sd = residual_sd(pair())
    assert decisions(strategy().generate_signals(pair(shock(150, side * 6 * sd, 5)))).empty


@pytest.mark.parametrize("side", [1, -1])
def test_a_stop_exits_and_blocks_re_entry_until_z_is_back_inside_entry(side):
    sd = residual_sd(pair())
    s = np.zeros(N)
    s[150:152] = side * 3 * sd  # enter
    s[152:155] = side * 6 * sd  # past the stop: out
    s[155:158] = side * 3 * sd  # still beyond entry: no re-entry
    targets = strategy().generate_signals(pair(s))
    d = decisions(targets)
    assert list(d.index[:2]) == [targets.index[150], targets.index[152]]
    assert list(d.iloc[1]) == [0.0, 0.0]
    assert targets.index[155] not in d.index


def test_an_uncointegrated_pair_is_never_traded():
    targets = strategy().generate_signals(pair(cointegrated=False, seed=11))
    assert decisions(targets).empty


def test_a_refit_that_loses_cointegration_closes_the_position():
    class BreaksAtSecondFit(PairsTradingStrategy):
        fits = 0

        def fit(self, a, b):
            self.fits += 1
            return super().fit(a, b) if self.fits == 1 else None

    sd = residual_sd(pair())
    model = BreaksAtSecondFit(formation=FORMATION, refit=30)  # fits at 99, 129, 159
    targets = model.generate_signals(pair(shock(110, 3 * sd, 40)))
    d = decisions(targets)
    assert list(d.index) == [targets.index[110], targets.index[129]]
    assert list(d.iloc[1]) == [0.0, 0.0]


def test_nothing_is_decided_before_the_formation_window_is_full():
    targets = strategy().generate_signals(pair(shock(10, 50.0, 80)))
    assert targets.iloc[: FORMATION - 1].isna().all().all()


def test_no_decision_reads_a_later_bar():
    """Refitting every bar, cut at many points: the targets up to each cut are unchanged."""
    sd = residual_sd(pair())
    frame = pair(shock(150, 3 * sd, 5) + shock(170, -3 * sd, 5))
    model = PairsTradingStrategy(formation=FORMATION, refit=1)
    full = model.generate_signals(frame)
    assert not decisions(full).empty
    for cut in range(FORMATION, N, 7):
        part = model.generate_signals(frame.iloc[:cut])
        pd.testing.assert_frame_equal(part, full.iloc[:cut])


def test_requires_exactly_two_columns():
    with pytest.raises(ValueError, match="exactly two"):
        strategy().generate_signals(pair()[["A"]])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"formation": 10},
        {"refit": 0},
        {"entry": 2.0, "exit": 2.0},
        {"entry": 4.0, "stop": 4.0},
        {"max_pvalue": 0.0},
    ],
)
def test_parameters_that_cannot_work_are_refused(kwargs):
    with pytest.raises(ValueError):
        PairsTradingStrategy(**kwargs)
