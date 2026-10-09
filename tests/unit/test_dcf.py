"""
modeling/dcf.py: known answers from an identity and from arithmetic done by
hand, the terminal-value cross-check, the reverse DCF, and the edges that
must return a reason rather than a number.
"""

import math

import numpy as np
import pytest

from modeling.dcf import (
    DCFInputs,
    gordon_implied_multiple,
    growth_path,
    regress_beta,
    reverse_dcf,
    run,
    sensitivity,
    wacc,
)


@pytest.mark.parametrize("years", [1, 3, 5, 10])
def test_constant_growth_collapses_to_gordon_for_any_horizon(years):
    # With g1 == g_T the projection is a growing perpetuity from year 1:
    # EV = FCF0 (1 + g) / (wacc - g), whatever N is. Independent of the code.
    x = DCFInputs(100.0, 0.03, 0.03, 0.09, net_debt=200.0, shares=50.0, years=years)
    ev = 100.0 * 1.03 / (0.09 - 0.03)
    r = run(x)
    assert r.enterprise_value == pytest.approx(ev, rel=1e-12)
    assert r.per_share == pytest.approx((ev - 200.0) / 50.0, rel=1e-12)


def test_three_year_case_by_hand():
    # FCF0 100, g fades 10% -> 6% -> 2% (terminal 2%), wacc 8%, net debt 50, 10 shares.
    # FCF: 110, 116.6, 118.932. PV: 110/1.08 = 101.851852; 116.6/1.1664 = 99.965706;
    # 118.932/1.259712 = 94.412056. TV = 118.932 x 1.02 / 0.06 = 2021.844;
    # PV(TV) = 2021.844 / 1.259712 = 1605.004954. EV = 1901.234568; per share 185.123457.
    r = run(DCFInputs(100.0, 0.10, 0.02, 0.08, 50.0, 10.0, years=3))
    assert r.growth_path == pytest.approx([0.10, 0.06, 0.02])
    assert r.fcf == pytest.approx([110.0, 116.6, 118.932])
    assert r.pv_fcf == pytest.approx([101.851852, 99.965706, 94.412056], abs=1e-6)
    assert r.terminal_value == pytest.approx(2021.844, abs=1e-6)
    assert r.pv_terminal == pytest.approx(1605.004954, abs=1e-6)
    assert r.per_share == pytest.approx(185.123457, abs=1e-6)
    assert r.terminal_share == pytest.approx(1605.004954 / 1901.234568, abs=1e-9)


def test_exit_multiple_agrees_with_gordon_at_the_gordon_implied_multiple():
    x = DCFInputs(100.0, 0.08, 0.025, 0.085, 0.0, 10.0, exit_metric_base=150.0, exit_multiple=1.0)
    m = gordon_implied_multiple(run(x))
    r = run(DCFInputs(**{**x.__dict__, "exit_multiple": m}))
    assert r.exit_enterprise_value == pytest.approx(r.enterprise_value, rel=1e-12)


def test_reverse_dcf_round_trips():
    x = DCFInputs(100.0, 0.07, 0.025, 0.09, 30.0, 10.0)
    price = run(x).per_share
    implied = reverse_dcf(DCFInputs(**{**x.__dict__, "growth_first": 0.0}), price)
    assert implied.reason is None
    assert implied.growth_first == pytest.approx(0.07, abs=1e-8)


def test_reverse_dcf_says_why_when_it_cannot():
    assert "not positive" in reverse_dcf(DCFInputs(-5.0, 0.05, 0.02, 0.09, 0.0, 10.0), 100.0).reason
    out = reverse_dcf(DCFInputs(100.0, 0.05, 0.02, 0.09, 0.0, 10.0), 1e9)
    assert math.isnan(out.growth_first) and "below" in out.reason


def test_wacc_at_or_below_terminal_growth_has_no_value_and_a_reason():
    r = run(DCFInputs(100.0, 0.05, 0.03, 0.03, 0.0, 10.0))
    assert r.per_share is None and "terminal growth" in r.reason


def test_sensitivity_grid_is_monotone_and_survives_impossible_cells():
    g = sensitivity(DCFInputs(100.0, 0.06, 0.025, 0.035, 0.0, 10.0), wacc_step=0.005, growth_step=0.005, points=5)
    grid = np.array(g["per_share"], dtype=float)
    assert np.isnan(grid).any()  # wacc 2.5% with growth 3.5%: no Gordon value, no exception
    for row in grid:  # higher terminal growth, higher value
        finite = row[np.isfinite(row)]
        assert np.all(np.diff(finite) > 0)
    for col in grid.T:  # higher wacc, lower value
        finite = col[np.isfinite(col)]
        assert np.all(np.diff(finite) < 0)


def test_terminal_heavy_valuations_are_flagged():
    r = run(DCFInputs(100.0, 0.02, 0.02, 0.05, 0.0, 10.0))
    assert r.terminal_share > 0.8 and any("terminal" in w for w in r.warnings)


def test_growth_path_fades_linearly():
    assert growth_path(0.10, 0.02, 5) == pytest.approx([0.10, 0.08, 0.06, 0.04, 0.02])
    assert growth_path(0.10, 0.02, 1) == [0.02]


def test_beta_recovers_a_known_slope():
    rng = np.random.default_rng(4)
    x = rng.normal(0, 0.01, 756)
    y = 1.3 * x + rng.normal(0, 0.008, 756)
    b = regress_beta(y, x)
    assert abs(b.beta - 1.3) < 3 * b.std_error
    assert 0 < b.std_error < 0.05 and b.observations == 756


def test_wacc_reported_and_estimated_cost_of_debt():
    w = wacc(0.04, 1.2, equity_market_value=900.0, debt=100.0, tax_rate=0.2, interest_expense=5.0)
    assert w.cost_of_equity == pytest.approx(0.04 + 1.2 * 0.045)
    assert w.cost_of_debt_pre_tax == pytest.approx(0.05) and not w.cost_of_debt_estimated
    assert w.wacc == pytest.approx(0.9 * w.cost_of_equity + 0.1 * 0.05 * 0.8)
    assert w.cost_of_debt_after_tax < w.wacc < w.cost_of_equity
    e = wacc(0.04, 1.2, 900.0, 100.0, 0.2, interest_expense=None)
    assert e.cost_of_debt_estimated and e.cost_of_debt_pre_tax == pytest.approx(0.05) and e.notes
