"""
pricing/svi.py and pricing/arbitrage.py (decision 56,
research/svi-surface-2026-10-20.md): the fit recovers known parameters, the
analytic derivatives match finite differences, and each arbitrage check
flags what it should, on the fit and on the quotes, and nothing else.
"""

from types import SimpleNamespace

import numpy as np
import pytest

from pricing import arbitrage as A
from pricing import svi
from pricing.svi import SVIParams

CLEAN = SVIParams(a=0.01, b=0.12, rho=-0.5, m=0.02, sigma=0.15)
#: Axel Vogt's raw SVI slice (T = 1), the textbook butterfly arbitrage:
#: g(k) < 0 between k = 0.64 and 1.26 (Gatheral and Jacquier 2014).
VOGT = SVIParams(a=-0.0410, b=0.1331, rho=0.3060, m=0.3586, sigma=0.4153)


# ---------------------------------------------------------------------------
# The fit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "true",
    [
        CLEAN,
        SVIParams(a=0.002, b=0.05, rho=-0.8, m=-0.05, sigma=0.08),
        SVIParams(a=0.03, b=0.3, rho=0.2, m=0.1, sigma=0.4),
    ],
)
def test_a_smile_from_known_parameters_is_fitted_back_to_them(true):
    T = 0.25
    k = np.linspace(-0.4, 0.3, 25)
    fit = svi.fit(k, true.iv(k, T), T)
    got = fit.params
    for name in ("a", "b", "rho", "m", "sigma"):
        assert getattr(got, name) == pytest.approx(getattr(true, name), abs=1e-6), name
    assert fit.rmse_iv < 1e-9
    assert fit.points == 25


def test_the_fit_reports_its_error_in_implied_vol():
    T = 0.5
    k = np.linspace(-0.3, 0.3, 21)
    noise = np.where(np.arange(21) % 2, 0.004, -0.004)  # +-0.4 vol points
    fit = svi.fit(k, CLEAN.iv(k, T) + noise, T)
    assert 0.001 < fit.rmse_iv < 0.005


def test_the_fit_never_has_negative_total_variance():
    # A smile that dips toward zero: the bottom is bounded at 0, not below.
    T = 0.1
    k = np.linspace(-0.2, 0.2, 15)
    iv = 0.02 + 0.8 * np.abs(k)
    fit = svi.fit(k, iv, T)
    assert fit.params.min_variance >= -1e-15
    assert np.all(fit.params.w(np.linspace(-1, 1, 201)) >= -1e-15)


def test_the_lowest_error_start_wins(monkeypatch):
    """Each start is solved; the fit keeps the one with the smallest cost, not the first."""
    costs = iter([5.0, 1.0, 3.0])
    vmins = iter([0.01, 0.02, 0.03])

    def fake(fun, x0, **kwargs):
        x = np.array(x0, dtype=float)
        x[0] = next(vmins)
        return SimpleNamespace(x=x, cost=next(costs))

    monkeypatch.setattr(svi, "least_squares", fake)
    k = np.linspace(-0.2, 0.2, 9)
    fit = svi.fit(k, CLEAN.iv(k, 0.25), 0.25, starts=3)
    assert fit.params.min_variance == pytest.approx(0.02)


def test_the_fit_refuses_too_few_points():
    with pytest.raises(ValueError):
        svi.fit([0.0, 0.1, 0.2, 0.3], [0.2, 0.2, 0.2, 0.2], 0.25)
    with pytest.raises(ValueError):
        svi.fit(np.linspace(-0.2, 0.2, 9), np.full(9, 0.2), 0.0)


# ---------------------------------------------------------------------------
# Analytic derivatives
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("p", [CLEAN, VOGT])
def test_derivatives_match_finite_differences(p):
    k = np.linspace(-0.8, 1.2, 41)
    h = 1e-5
    assert p.w1(k) == pytest.approx((p.w(k + h) - p.w(k - h)) / (2 * h), abs=1e-8)
    assert p.w2(k) == pytest.approx((p.w(k + h) - 2 * p.w(k) + p.w(k - h)) / h**2, abs=1e-4)
    w, w1 = p.w(k), (p.w(k + h) - p.w(k - h)) / (2 * h)
    w2 = (p.w(k + h) - 2 * p.w(k) + p.w(k - h)) / h**2
    g = (1 - k * w1 / (2 * w)) ** 2 - (w1**2 / 4) * (1 / w + 0.25) + w2 / 2
    assert p.g(k) == pytest.approx(g, abs=1e-4)


def test_wing_slopes_are_the_asymptotes():
    left, right = CLEAN.wing_slopes
    assert left == pytest.approx(CLEAN.w1(-1e6), rel=1e-9)
    assert right == pytest.approx(CLEAN.w1(1e6), rel=1e-9)


# ---------------------------------------------------------------------------
# Butterfly
# ---------------------------------------------------------------------------


def test_intervals_are_the_runs_of_flags():
    grid = np.arange(8.0)
    mask = np.array([1, 1, 0, 0, 1, 0, 1, 1], dtype=bool)
    assert A.intervals(grid, mask) == [(0.0, 1.0), (4.0, 4.0), (6.0, 7.0)]
    assert A.intervals(grid, np.zeros(8, dtype=bool)) == []


def test_vogts_slice_is_flagged_where_its_density_is_negative():
    fly = A.butterfly(VOGT, (-0.5, 1.2), [], margin=0.1, points=3001)
    assert fly.verdict == "fit_only"
    assert fly.min_g < -0.03
    [(lo, hi)] = fly.fit
    assert lo == pytest.approx(0.643, abs=2e-3)
    assert hi == pytest.approx(1.256, abs=2e-3)
    assert fly.checked == (-0.6, 1.3)


def test_an_arbitrage_free_slice_passes():
    fly = A.butterfly(CLEAN, (-0.5, 0.5), [])
    assert fly.fit == [] and fly.min_g > 0 and fly.verdict == "none"


def test_the_margin_reaches_past_the_quotes():
    # Vogt's bad region starts at 0.643: quoted to 0.6 it is found only
    # through the margin.
    assert A.butterfly(VOGT, (-0.5, 0.6), [], margin=0.0).fit == []
    assert A.butterfly(VOGT, (-0.5, 0.6), [], margin=0.1).fit != []


def test_a_butterfly_bought_for_less_than_nothing_is_flagged():
    F = 100.0
    # Body bid 3.10 against wing asks 5.00 and 1.10: 5.00 + 1.10 - 2 x 3.10 < 0.
    flagged = A.quoted_butterflies([95, 100, 105], [True] * 3, [4.9, 3.1, 1.0], [5.0, 3.2, 1.1], F)
    assert flagged == [pytest.approx((np.log(0.95), np.log(1.05)))]
    # One cent less on the body bid and it costs exactly nothing: not flagged.
    assert (
        A.quoted_butterflies([95, 100, 105], [True] * 3, [4.9, 3.05, 1.0], [5.0, 3.2, 1.1], F) == []
    )


def test_unequal_strike_gaps_are_weighted():
    # Strikes 100, 101, 105: the butterfly is 4 x K100 + 1 x K105 - 5 x K101,
    # so a fair body is 0.8 x 2.00 + 0.2 x 1.00 = 1.80 per unit.
    strikes, calls, ask = [100, 101, 105], [True] * 3, [2.00, 1.95, 1.00]
    assert A.quoted_butterflies(strikes, calls, [1.9, 1.70, 0.9], ask, 100.0) == []
    assert A.quoted_butterflies(strikes, calls, [1.9, 1.81, 0.9], ask, 100.0) != []
    # Equal weights (2.00 + 1.00 - 2 x 1.70 < 0) would have flagged the first.


def test_a_triple_across_puts_and_calls_is_not_tested():
    # Put at 95 and 100, call at 105: no forward is trusted to map them.
    assert (
        A.quoted_butterflies(
            [95, 100, 105], [False, False, True], [0.5, 9.0, 1.0], [0.6, 9.1, 1.1], 100.0
        )
        == []
    )


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def _iv(p, k, T):
    return p.iv(np.asarray(k), T)


def test_crossing_fits_are_flagged_on_their_shared_range_beside_the_quotes():
    near_T, far_T = 0.1, 0.2
    near = SVIParams(0.004, 0.03, -0.4, 0.0, 0.1)
    # The far fit lies above the near one for k < 0 and below it for k > 0.
    far = SVIParams(0.004 + 0.0, 0.03, -0.9, 0.0, 0.1)
    nk = np.linspace(-0.3, 0.2, 11)
    fk = np.linspace(-0.2, 0.3, 11)
    # Quotes: far total variance above near everywhere.
    near_iv = np.full(11, 0.20)
    far_iv = np.full(11, 0.20)
    c = A.calendar(near, nk, near_iv, near_T, far, fk, far_iv, far_T, points=501)
    assert c.shared == pytest.approx((-0.2, 0.2))
    [(lo, hi)] = c.fit
    assert lo == pytest.approx(0.0, abs=1e-3) and hi == pytest.approx(0.2)
    assert c.quotes == [] and c.verdict == "fit_only"


def test_quotes_crossing_where_the_fits_do_not_is_quotes_only():
    near = SVIParams(0.004, 0.03, -0.4, 0.0, 0.1)
    far = SVIParams(0.010, 0.03, -0.4, 0.0, 0.1)  # above everywhere
    k = np.linspace(-0.2, 0.2, 9)
    near_iv = np.full(9, 0.30)
    far_iv = np.where(k > 0.05, 0.20, 0.30)  # far total variance falls only at k > 0.05
    c = A.calendar(near, k, near_iv, 0.1, far, k, far_iv, 0.2, points=401)
    assert c.fit == []
    [(lo, hi)] = c.quotes
    assert 0.0 < lo < 0.1 and hi == pytest.approx(0.2)
    assert c.verdict == "quotes_only"


def test_expiries_with_no_shared_range_are_not_compared():
    k1, k2 = np.linspace(-0.3, -0.1, 5), np.linspace(0.0, 0.2, 5)
    c = A.calendar(CLEAN, k1, np.full(5, 0.2), 0.1, CLEAN, k2, np.full(5, 0.2), 0.2)
    assert c.shared is None and c.fit == [] and c.quotes == [] and c.verdict == "none"


@pytest.mark.parametrize(
    "fit,quotes,expected",
    [
        (False, False, "none"),
        (True, True, "both"),
        (True, False, "fit_only"),
        (False, True, "quotes_only"),
    ],
)
def test_verdicts(fit, quotes, expected):
    assert A.verdict(fit, quotes) == expected
