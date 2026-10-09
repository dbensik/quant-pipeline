"""
pricing/: known answers from a textbook, identities that must hold exactly,
cross-checks between independent implementations (closed form, tree, Monte
Carlo), and realised-vol estimators recovering a known sigma from simulated
intraday paths.

Tolerances are MEASURED, not guessed; the numbers are in the plan's phase 1
row (research/option-pricing-plan-2026-10-08.md).
"""

import numpy as np
import pytest

from pricing import black_scholes as bs
from pricing import volatility as V
from pricing.binomial import american_price, crr_price
from pricing.monte_carlo import european_mc

RIGHTS = ("call", "put")
# S, K, T, r, q, sigma — includes a dividend yield everywhere it can matter.
GRID = [
    (100, K, T, 0.04, 0.012, s)
    for K in (70, 90, 100, 110, 130)
    for T in (0.05, 0.25, 1.0)
    for s in (0.15, 0.4)
]


# --- Black-Scholes -------------------------------------------------------------

def test_hull_known_values():
    # Hull, Options Futures and Other Derivatives: S=42 K=40 r=10% sigma=20% T=0.5
    assert round(bs.price(42, 40, 0.5, 0.10, 0.0, 0.20, "call"), 2) == 4.76
    assert round(bs.price(42, 40, 0.5, 0.10, 0.0, 0.20, "put"), 2) == 0.81


@pytest.mark.parametrize("S,K,T,r,q,s", GRID)
def test_put_call_parity_with_dividend_yield(S, K, T, r, q, s):
    lhs = bs.price(S, K, T, r, q, s, "call") - bs.price(S, K, T, r, q, s, "put")
    assert lhs == pytest.approx(S * np.exp(-q * T) - K * np.exp(-r * T), abs=1e-10)


def _fd(fn, x, h):
    return (fn(x + h) - fn(x - h)) / (2 * h)


@pytest.mark.parametrize("right", RIGHTS)
@pytest.mark.parametrize("S,K,T,r,q,s", GRID[::3])
def test_greeks_match_central_differences(S, K, T, r, q, s, right):
    g = bs.greeks(S, K, T, r, q, s, right)
    P = lambda **kw: bs.price(**{**dict(S=S, K=K, T=T, r=r, q=q, sigma=s, right=right), **kw})
    assert g.delta == pytest.approx(_fd(lambda x: P(S=x), S, 1e-4 * S), abs=1e-6)
    # Gamma's bump is scaled to the option's own width, S sigma sqrt(T): a
    # fixed 1% of spot is a third of a standard deviation for a 0.05-year,
    # 15%-vol option, and the second difference's truncation error dominates.
    hg = 1e-3 * S * s * np.sqrt(T)
    gamma_fd = (P(S=S + hg) - 2 * P() + P(S=S - hg)) / hg**2
    assert g.gamma == pytest.approx(gamma_fd, abs=1e-5)
    assert g.vega == pytest.approx(_fd(lambda x: P(sigma=x), s, 1e-5), abs=1e-5)
    assert g.rho == pytest.approx(_fd(lambda x: P(r=x), r, 1e-5), abs=1e-5)
    # Theta is per year of time PASSING: minus the derivative in T.
    assert g.theta == pytest.approx(-_fd(lambda x: P(T=x), T, 1e-6), abs=1e-4)


def test_expiry_is_intrinsic_and_its_greeks_are_finite():
    assert bs.price(105, 100, 0.0, 0.04, 0.01, 0.2, "call") == 5.0
    assert bs.price(95, 100, 0.0, 0.04, 0.01, 0.2, "put") == 5.0
    assert bs.price(95, 100, 0.0, 0.04, 0.01, 0.2, "call") == 0.0
    g = bs.greeks(105, 100, 0.0, 0.04, 0.01, 0.2, "call")
    assert (g.delta, g.gamma, g.vega, g.theta, g.rho) == (1.0, 0.0, 0.0, 0.0, 0.0)


def test_zero_vol_is_the_discounted_forward_intrinsic():
    S, K, T, r, q = 100, 95, 0.5, 0.04, 0.01
    expected = max(S * np.exp(-q * T) - K * np.exp(-r * T), 0.0)
    assert bs.price(S, K, T, r, q, 0.0, "call") == pytest.approx(expected, abs=1e-12)


def test_invalid_inputs_raise():
    for bad in [dict(S=0), dict(K=-1), dict(T=-0.1), dict(sigma=-0.2)]:
        args = {**dict(S=100, K=100, T=0.25, r=0.04, q=0.0, sigma=0.2), **bad}
        with pytest.raises(ValueError):
            bs.price(**args)


def test_vectorised_over_strikes():
    K = np.array([90.0, 100.0, 110.0])
    out = bs.price(100, K, 0.25, 0.04, 0.01, 0.2, "call")
    assert out.shape == (3,)
    assert out[1] == pytest.approx(bs.price(100, 100, 0.25, 0.04, 0.01, 0.2, "call"))


# --- implied volatility -----------------------------------------------------------

@pytest.mark.parametrize("right", RIGHTS)
@pytest.mark.parametrize("S,K,T,r,q,s", GRID)
def test_iv_round_trips(S, K, T, r, q, s, right):
    p = bs.price(S, K, T, r, q, s, right)
    iv = bs.implied_vol(p, S, K, T, r, q, right)
    if bs.greeks(S, K, T, r, q, s, right).vega > 1e-2:
        assert iv.reason is None
        assert iv.sigma == pytest.approx(s, abs=1e-8)
    else:
        # Deep out of the money or nearly expired: the price barely moves with
        # sigma, so only the PRICE is pinned — sigma can be off and price right.
        if iv.reason is None:
            assert bs.price(S, K, T, r, q, iv.sigma, right) == pytest.approx(p, abs=1e-10)
        else:
            assert iv.reason in ("at_or_below_lower_bound", "outside_search_bounds")


@pytest.mark.parametrize(
    "right,price_of,reason",
    [
        ("call", lambda lo, hi: lo - 0.01, "at_or_below_lower_bound"),
        ("call", lambda lo, hi: hi + 0.01, "at_or_above_upper_bound"),
        ("put", lambda lo, hi: lo, "at_or_below_lower_bound"),
        ("put", lambda lo, hi: hi, "at_or_above_upper_bound"),
    ],
)
def test_iv_refuses_prices_outside_dividend_discounted_bounds(right, price_of, reason):
    S, K, T, r, q = 100, 90 if right == "call" else 110, 0.5, 0.04, 0.03
    lo, hi = bs.no_arbitrage_bounds(S, K, T, r, q, right)
    iv = bs.implied_vol(price_of(lo, hi), S, K, T, r, q, right)
    assert np.isnan(iv.sigma) and iv.reason == reason


def test_iv_bounds_use_the_dividend_yield():
    # With q = 3% the call lower bound is S e^{-qT} - K e^{-rT}, not S - K e^{-rT}.
    lo, hi = bs.no_arbitrage_bounds(100, 90, 0.5, 0.04, 0.03, "call")
    assert lo == pytest.approx(100 * np.exp(-0.015) - 90 * np.exp(-0.02))
    assert hi == pytest.approx(100 * np.exp(-0.015))


def test_iv_other_reasons():
    assert bs.implied_vol(1.0, 100, 100, 0.0, 0.04, 0.0).reason == "non_positive_time"
    assert bs.implied_vol(np.nan, 100, 100, 0.5, 0.04, 0.0).reason == "non_finite_price"
    # A price that needs sigma above the search ceiling of 5.0.
    p = bs.price(100, 100, 0.5, 0.04, 0.0, 6.0)
    assert bs.implied_vol(p, 100, 100, 0.5, 0.04, 0.0).reason == "outside_search_bounds"


def test_american_put_past_its_exercise_boundary_has_no_implied_vol():
    # S=100, K=130: immediate exercise is optimal, the tree prices at
    # intrinsic (30) for any sigma, so there is nothing to invert.
    p = crr_price(100, 130, 0.5, 0.05, 0.0, 0.25, "put", american=True)
    assert p == pytest.approx(30.0, abs=1e-9)
    iv = bs.implied_vol(p, 100, 130, 0.5, 0.05, 0.0, "put", style="american")
    assert np.isnan(iv.sigma) and iv.reason == "at_or_below_lower_bound"


def test_american_iv_inverts_the_tree_and_european_iv_overstates_it():
    S, K, T, r, q, s = 100, 110, 0.5, 0.05, 0.0, 0.25
    p = crr_price(S, K, T, r, q, s, "put", american=True)
    american = bs.implied_vol(p, S, K, T, r, q, "put", style="american")
    assert american.reason is None
    assert american.sigma == pytest.approx(s, abs=1e-6)
    # The defect style="american" exists for: BSM inverted on an American
    # deep-ITM put reads an early-exercise premium as extra volatility.
    european = bs.implied_vol(p, S, K, T, r, q, "put", style="european")
    assert european.reason is not None or european.sigma > s + 0.02


# --- binomial tree ----------------------------------------------------------------

@pytest.mark.parametrize("right", RIGHTS)
@pytest.mark.parametrize("S,K,T,r,q,s", GRID)
def test_european_tree_converges_to_black_scholes(S, K, T, r, q, s, right):
    # Measured worst on this grid: 7e-5 of S sigma sqrt(T) with N, N+1 averaged.
    tree = crr_price(S, K, T, r, q, s, right, american=False)
    assert abs(tree - bs.price(S, K, T, r, q, s, right)) <= 1e-4 * S * s * np.sqrt(T)


def test_smoothing_is_what_meets_the_tolerance_at_the_money():
    S, K, T, r, q, s = 100, 100, 0.25, 0.04, 0.012, 0.2
    exact = bs.price(S, K, T, r, q, s, "call")
    plain = abs(crr_price(S, K, T, r, q, s, "call", False, 500, smooth=False) - exact)
    smooth = abs(crr_price(S, K, T, r, q, s, "call", False, 500, smooth=True) - exact)
    assert plain > 1e-3 > smooth


def test_hull_american_put():
    # Hull: S=K=50, r=10%, sigma=40%, T=5 months. 5-step tree 4.49; converged
    # ~4.28; European (BSM) 4.08.
    assert round(crr_price(50, 50, 5 / 12, 0.10, 0.0, 0.40, "put", True, 5, smooth=False), 2) == 4.49
    a = american_price(50, 50, 5 / 12, 0.10, 0.0, 0.40, "put")
    assert round(a.american, 2) == 4.28
    assert round(bs.price(50, 50, 5 / 12, 0.10, 0.0, 0.40, "put"), 2) == 4.08
    assert a.early_exercise_premium == pytest.approx(a.american - a.european)


@pytest.mark.parametrize("K", (80, 100, 120))
def test_american_call_without_dividends_is_european(K):
    a = american_price(100, K, 0.5, 0.05, 0.0, 0.3, "call")
    assert a.early_exercise_premium == pytest.approx(0.0, abs=1e-12)


@pytest.mark.parametrize("S,K,T,r,q,s", GRID)
def test_american_put_dominates_european_and_intrinsic(S, K, T, r, q, s):
    a = american_price(S, K, T, r, q, s, "put")
    assert a.american >= a.european - 1e-12
    assert a.american >= max(K - S, 0.0) - 1e-12


def test_deep_itm_put_carries_an_early_exercise_premium():
    a = american_price(100, 130, 0.5, 0.05, 0.0, 0.25, "put")
    assert a.early_exercise_premium > 0.3


def test_tree_rejects_a_probability_outside_zero_one():
    with pytest.raises(ValueError, match="risk-neutral probability"):
        crr_price(100, 100, 1.0, 2.0, 0.0, 0.01, "call", steps=2, smooth=False)


# --- Monte Carlo ------------------------------------------------------------------

@pytest.mark.parametrize("right", RIGHTS)
@pytest.mark.parametrize("K", (90, 100, 115))
def test_mc_within_three_standard_errors_of_black_scholes(right, K):
    S, T, r, q, s = 100, 0.5, 0.04, 0.015, 0.25
    mc = european_mc(S, K, T, r, q, s, right, np.random.default_rng(7), paths=200_000)
    assert abs(mc.price - bs.price(S, K, T, r, q, s, right)) < 3 * mc.std_error
    assert abs(mc.delta - bs.greeks(S, K, T, r, q, s, right).delta) < 3 * mc.delta_std_error


@pytest.mark.parametrize("K", (70, 100))
def test_reported_standard_error_is_honest(K):
    # Across independent seeds, the spread of the estimates must match the SE
    # each run reports. Deep in the money the antithetic pairing removes most
    # of the variance; an SE taken over all 2N draws would overstate it there
    # several times over, which is what this catches.
    runs = [
        european_mc(100, K, 0.5, 0.04, 0.0, 0.25, "call", np.random.default_rng(seed), paths=20_000)
        for seed in range(30)
    ]
    spread = np.std([m.price for m in runs], ddof=1)
    reported = np.mean([m.std_error for m in runs])
    assert 0.6 < spread / reported < 1.5


def test_mc_seed_is_explicit_and_effective():
    args = (100, 100, 0.5, 0.04, 0.0, 0.25, "call")
    a = european_mc(*args, np.random.default_rng(1), paths=10_000)
    b = european_mc(*args, np.random.default_rng(1), paths=10_000)
    c = european_mc(*args, np.random.default_rng(2), paths=10_000)
    assert a == b
    assert a.price != c.price
    with pytest.raises(TypeError):
        european_mc(*args, 1, paths=10_000)


# --- realised volatility ------------------------------------------------------

def simulate_ohlc(rng, days, sigma, steps=390, gap_frac=0.0, drift_per_day=0.0):
    """
    Daily OHLC from an intraday GBM: `steps` log-normal steps a day plus an
    overnight gap carrying `gap_frac` of the daily variance. The high and low
    are the extremes of the discrete path, which biases range estimators a
    few percent low (measured: 3-5% at 390 steps).
    """
    sd = sigma / np.sqrt(252)
    intraday = rng.standard_normal((days, steps)) * sd * np.sqrt(1 - gap_frac) / np.sqrt(steps)
    intraday += drift_per_day / steps
    gaps = rng.standard_normal(days) * sd * np.sqrt(gap_frac)
    day_move = gaps + intraday.sum(axis=1)
    open_log = np.log(100.0) + np.concatenate([[0.0], np.cumsum(day_move)[:-1]]) + gaps
    path = open_log[:, None] + np.concatenate([np.zeros((days, 1)), np.cumsum(intraday, axis=1)], axis=1)
    return np.exp(open_log), np.exp(path.max(axis=1)), np.exp(path.min(axis=1)), np.exp(path[:, -1])


@pytest.fixture(scope="module")
def plain():
    return simulate_ohlc(np.random.default_rng(11), 4000, 0.2)


@pytest.fixture(scope="module")
def gapped():
    return simulate_ohlc(np.random.default_rng(12), 4000, 0.2, gap_frac=0.2)


@pytest.fixture(scope="module")
def trending():
    return simulate_ohlc(np.random.default_rng(11), 4000, 0.2, drift_per_day=0.01)


def ratios(ohlc):
    return {name: fn(*ohlc) / 0.2 for name, fn in V.ESTIMATORS.items()}


def test_every_estimator_recovers_sigma_without_gaps_or_drift(plain):
    r = ratios(plain)
    assert r["close_to_close"] == pytest.approx(1.0, abs=0.03)
    for name in ("parkinson", "garman_klass", "rogers_satchell", "yang_zhang"):
        assert 0.93 < r[name] < 1.005, name  # discrete high/low reads a few % low


def test_only_gap_aware_estimators_see_overnight_variance(gapped):
    r = ratios(gapped)
    assert r["close_to_close"] == pytest.approx(1.0, abs=0.03)
    assert 0.93 < r["yang_zhang"] < 1.005
    for name in ("parkinson", "garman_klass", "rogers_satchell"):
        # Intraday variance only: sqrt(0.8) = 0.894, less the sampling bias.
        assert 0.82 < r[name] < 0.90, name


def test_drift_biases_parkinson_but_not_rogers_satchell_or_yang_zhang(plain, trending):
    base, trend = ratios(plain), ratios(trending)
    assert trend["parkinson"] / base["parkinson"] > 1.06
    assert trend["garman_klass"] / base["garman_klass"] > 1.02
    for name in ("rogers_satchell", "yang_zhang"):
        assert trend[name] / base[name] == pytest.approx(1.0, abs=0.015), name


def test_parkinson_known_answer():
    # A constant daily range ln(H/L) = a gives variance a^2 / (4 ln 2).
    a = 0.02
    h, lo = np.full(10, 100 * np.exp(a)), np.full(10, 100.0)
    assert V.parkinson(h, lo) == pytest.approx(np.sqrt(a**2 / (4 * np.log(2)) * 252))


def test_yang_zhang_weight():
    assert V.yang_zhang_k(20) == pytest.approx(0.34 / (1.34 + 21 / 19))
    assert V.yang_zhang_k(20) == pytest.approx(0.139044, abs=1e-6)


def test_yang_zhang_known_answer_where_k_matters():
    # Three bars after a starting close, with no wicks (high = max(open,
    # close), low = min), so every Rogers-Satchell term is exactly zero and
    # YZ = V_overnight + k * V_open_close. The simulated fixtures cannot pin k:
    # without gaps YZ with k = 1 equals close-to-close to the bit.
    #   overnight log returns [0.01, -0.01, 0.02]  -> sample var 2.33333e-4
    #   open-to-close         [0.02, -0.01, 0.00]  -> sample var 2.33333e-4
    #   k(3) = 0.34 / (1.34 + 4/2) = 0.1017964
    #   var = 2.33333e-4 * 1.1017964 = 2.570858e-4; sqrt(var * 252) = 0.254530
    gaps, moves = [0.01, -0.01, 0.02], [0.02, -0.01, 0.00]
    o, c = [1.0], [1.0]
    for g, m in zip(gaps, moves):
        o.append(c[-1] * np.exp(g))
        c.append(o[-1] * np.exp(m))
    o, c = np.array(o), np.array(c)
    h, lo = np.maximum(o, c), np.minimum(o, c)
    assert V.rogers_satchell(o[1:], h[1:], lo[1:], c[1:]) == 0.0
    assert V.yang_zhang(o, h, lo, c) == pytest.approx(0.254530, abs=1e-6)


def test_rolling_aligns_to_the_window_end_and_cone_is_ordered(plain):
    o, h, lo, c = (x[:300] for x in plain)
    roll = V.rolling("parkinson", o, h, lo, c, 20)
    assert np.isnan(roll[18]) and np.isfinite(roll[19])
    assert roll[19] == pytest.approx(V.parkinson(h[:20], lo[:20]))
    roll_yz = V.rolling("yang_zhang", o, h, lo, c, 20)
    assert np.isnan(roll_yz[19]) and np.isfinite(roll_yz[20])
    cone = V.vol_cone(o, h, lo, c, windows=(10, 60))
    for stats in cone.values():
        values = [stats[k] for k in V.CONE_STATS]
        assert values == sorted(values)


def test_estimators_reject_bad_input():
    with pytest.raises(ValueError):
        V.parkinson([1.0, 2.0], [1.0])
    with pytest.raises(ValueError):
        V.close_to_close([100.0, 0.0, 101.0])
    with pytest.raises(ValueError):
        V.rolling("nope", [1.0] * 5, [1.0] * 5, [1.0] * 5, [1.0] * 5, 3)
