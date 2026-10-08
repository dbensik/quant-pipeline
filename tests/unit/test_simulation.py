"""
Unit tests for simulation/ — the pure Monte Carlo kernels.

Each test asserts on an OUTPUT the kernel computes, never on an echoed input,
and the known-answer cases were checked by hand before the kernel existed.
Mutations tried against this file are listed in
research/monte-carlo-plan-2026-10-07.md (phase 1).
"""

from __future__ import annotations

import numpy as np
import pytest

from simulation import (
    METHODS,
    bands,
    cvar,
    draw,
    equity_from_returns,
    gbm,
    horizon_returns,
    iid_bootstrap,
    max_drawdown,
    max_drawdown_duration,
    prob_ruin,
    prob_worse_drawdown,
    stationary_bootstrap,
    terminal_wealth,
    var,
)


def _ar1(n: int, phi: float, sigma: float, seed: int) -> np.ndarray:
    """A strongly autocorrelated daily return series."""
    rng = np.random.default_rng(seed)
    eps = rng.normal(0.0, sigma, n)
    r = np.empty(n)
    r[0] = eps[0]
    for t in range(1, n):
        r[t] = phi * r[t - 1] + eps[t]
    return r


def _lag1_autocorr(paths: np.ndarray) -> np.ndarray:
    """Per-path lag-1 autocorrelation."""
    x = paths - paths.mean(axis=1, keepdims=True)
    num = (x[:, 1:] * x[:, :-1]).sum(axis=1)
    den = (x * x).sum(axis=1)
    return num / den


# ---------------------------------------------------------------------------
# resample
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("method", METHODS)
def test_draw_shape_and_values_come_from_the_series(method):
    r = np.array([0.01, -0.02, 0.03, 0.0, -0.01])
    out = draw(method, r, n_paths=7, horizon=11, rng=np.random.default_rng(1))
    assert out.shape == (7, 11)
    assert np.all(np.isfinite(out))
    if method != "gbm":
        assert set(np.unique(out)).issubset(set(r))


@pytest.mark.parametrize("method", METHODS)
def test_seed_is_two_sided(method):
    r = _ar1(500, 0.3, 0.01, seed=0)
    a = draw(method, r, 50, 100, np.random.default_rng(42))
    b = draw(method, r, 50, 100, np.random.default_rng(42))
    c = draw(method, r, 50, 100, np.random.default_rng(43))
    assert np.array_equal(a, b)
    assert not np.array_equal(a, c)


def test_stationary_bootstrap_keeps_autocorrelation_that_iid_destroys():
    # phi = 0.8 gives a lag-1 autocorrelation of ~0.8 in the source series.
    r = _ar1(2_000, 0.8, 0.01, seed=7)
    source = _lag1_autocorr(r[None, :])[0]
    assert source > 0.7

    rng = np.random.default_rng(3)
    blocks = _lag1_autocorr(stationary_bootstrap(r, 400, 1_000, 20.0, rng)).mean()
    singles = _lag1_autocorr(iid_bootstrap(r, 400, 1_000, rng)).mean()

    # The assertion is the GAP: blocks retain most of the structure, singles
    # retain none. A kernel that ignored its block length would fail this.
    assert blocks > 0.6, blocks
    assert abs(singles) < 0.1, singles
    assert blocks - singles > 0.5


def test_stationary_bootstrap_block_length_changes_the_draw():
    r = _ar1(300, 0.5, 0.01, seed=2)
    short = stationary_bootstrap(r, 20, 200, 2.0, np.random.default_rng(5))
    long = stationary_bootstrap(r, 20, 200, 50.0, np.random.default_rng(5))
    assert not np.array_equal(short, long)
    # Longer blocks -> more of the source's autocorrelation survives.
    assert _lag1_autocorr(long).mean() > _lag1_autocorr(short).mean()


def test_stationary_bootstrap_continues_blocks_in_source_order():
    # With a huge mean block, nearly every step is "continue": each path is a
    # contiguous (wrapped) slice of the source.
    r = np.arange(10, dtype=float) / 100.0
    out = stationary_bootstrap(r, 5, 6, 1e9, np.random.default_rng(0))
    for path in out:
        start = int(round(path[0] * 100))
        expected = r[(start + np.arange(6)) % 10]
        assert np.array_equal(path, expected)


def test_gbm_terminal_mean_matches_closed_form():
    rng = np.random.default_rng(11)
    r = np.expm1(rng.normal(0.0004, 0.012, 3_000))
    log_r = np.log1p(r)
    mu, sigma = log_r.mean(), log_r.std(ddof=1)
    horizon, n_paths = 252, 40_000

    wealth = terminal_wealth(equity_from_returns(gbm(r, n_paths, horizon, np.random.default_rng(1)), 1.0))
    expected = np.exp((mu + 0.5 * sigma**2) * horizon)
    # Lognormal terminal wealth: SE of the sample mean from its known variance.
    var_terminal = np.exp(2 * mu * horizon + sigma**2 * horizon) * (np.exp(sigma**2 * horizon) - 1)
    se = np.sqrt(var_terminal / n_paths)
    assert abs(wealth.mean() - expected) < 3 * se


@pytest.mark.parametrize(
    "bad",
    [
        np.array([0.01]),
        np.array([[0.01, 0.02]]),
        np.array([0.01, np.nan]),
        np.array([0.01, -1.0]),
    ],
)
def test_resample_rejects_unusable_series(bad):
    with pytest.raises(ValueError):
        iid_bootstrap(bad, 2, 2, np.random.default_rng(0))


def test_resample_rejects_bad_shape_and_method():
    r = np.array([0.01, 0.02, 0.03])
    rng = np.random.default_rng(0)
    with pytest.raises(ValueError):
        iid_bootstrap(r, 0, 5, rng)
    with pytest.raises(ValueError):
        stationary_bootstrap(r, 5, 0, 20.0, rng)
    with pytest.raises(ValueError):
        stationary_bootstrap(r, 5, 5, 0.5, rng)
    with pytest.raises(ValueError):
        draw("normal", r, 5, 5, rng)


# ---------------------------------------------------------------------------
# paths
# ---------------------------------------------------------------------------


def test_equity_starts_at_initial_and_compounds():
    eq = equity_from_returns(np.array([[0.1, -0.5], [0.0, 0.0]]), 200.0)
    assert eq.shape == (2, 3)
    np.testing.assert_allclose(eq[0], [200.0, 220.0, 110.0])
    np.testing.assert_allclose(eq[1], [200.0, 200.0, 200.0])


def test_max_drawdown_known_answer():
    eq = np.array(
        [
            [100.0, 120.0, 90.0, 95.0, 130.0, 100.0],  # 90/120 - 1
            [100.0, 110.0, 120.0, 130.0, 140.0, 150.0],  # never under water
            [100.0, 50.0, 60.0, 40.0, 45.0, 50.0],  # 40/100 - 1
        ]
    )
    np.testing.assert_allclose(max_drawdown(eq), [-0.25, 0.0, -0.6])


def test_max_drawdown_duration_known_answer():
    eq = np.array(
        [
            [100.0, 120.0, 90.0, 95.0, 130.0, 100.0],  # under at 2,3 then 5 -> 2
            [100.0, 110.0, 120.0, 130.0, 140.0, 150.0],  # 0
            [100.0, 50.0, 60.0, 40.0, 45.0, 50.0],  # under from 1 to 5 -> 5
            [100.0, 90.0, 100.0, 90.0, 80.0, 100.0],  # runs of 1 and 2; equal to peak is NOT under
        ]
    )
    np.testing.assert_array_equal(max_drawdown_duration(eq), [2, 0, 5, 2])


def test_drawdown_matches_performance_analyzer_on_a_random_curve():
    from itertools import groupby

    rng = np.random.default_rng(9)
    eq = 100 * np.cumprod(1 + rng.normal(0, 0.02, 500))[None, :]
    series = eq[0]
    peak = np.maximum.accumulate(series)
    dd = (series - peak) / peak
    flags = np.where(dd < 0, 1, 0)
    expected_duration = max(sum(1 for _ in g) for k, g in groupby(flags) if k == 1)
    assert max_drawdown(eq)[0] == pytest.approx(dd.min())
    assert max_drawdown_duration(eq)[0] == expected_duration


def test_bands_percentiles_are_ordered_and_start_at_initial():
    rng = np.random.default_rng(4)
    eq = equity_from_returns(rng.normal(0.0, 0.01, (500, 30)), 1_000.0)
    b = bands(eq, (5, 25, 50, 75, 95))
    assert b.shape == (31, 5)
    assert np.all(b[0] == 1_000.0)
    assert np.all(np.diff(b, axis=1) >= 0)
    assert b[10, 2] == pytest.approx(np.median(eq[:, 10]))


def test_bands_rejects_bad_percentiles():
    eq = np.ones((3, 3))
    with pytest.raises(ValueError):
        bands(eq, ())
    with pytest.raises(ValueError):
        bands(eq, (5, 101))


# ---------------------------------------------------------------------------
# risk
# ---------------------------------------------------------------------------


def test_horizon_returns_compound_the_first_days():
    paths = np.array([[0.1, 0.1, -0.5], [0.0, 0.0, 0.0]])
    np.testing.assert_allclose(horizon_returns(paths, 2), [0.21, 0.0])
    np.testing.assert_allclose(horizon_returns(paths, 3), [1.21 * 0.5 - 1, 0.0])
    with pytest.raises(ValueError):
        horizon_returns(paths, 4)


def test_var_and_cvar_known_answer():
    # 1,001 evenly spaced outcomes from -50% to +50%: the 5th percentile is
    # -45%, and the worst 5% average to -47.5%.
    r = np.linspace(-0.5, 0.5, 1_001)
    assert var(r, 0.95) == pytest.approx(0.45)
    assert cvar(r, 0.95) == pytest.approx(0.475, abs=1e-3)
    assert cvar(r, 0.95) > var(r, 0.95)
    with pytest.raises(ValueError):
        var(r, 1.0)


def test_prob_ruin_counts_paths_that_ever_cross_the_threshold():
    eq = np.array(
        [
            [100.0, 60.0, 120.0],  # dipped below 70 then recovered: ruin
            [100.0, 80.0, 90.0],  # never below 70
            [100.0, 100.0, 69.0],  # ends below
            [100.0, 70.0, 100.0],  # exactly 70 is not below
        ]
    )
    assert prob_ruin(eq, 0.7) == pytest.approx(0.5)
    with pytest.raises(ValueError):
        prob_ruin(eq, 1.0)


def test_prob_worse_drawdown():
    dds = np.array([-0.1, -0.3, -0.5, -0.2])
    assert prob_worse_drawdown(dds, -0.25) == pytest.approx(0.5)
    assert prob_worse_drawdown(dds, -0.6) == 0.0
