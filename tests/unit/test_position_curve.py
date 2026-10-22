"""
pricing/position.py: a multi-leg position before expiry (decision 55).

The Greeks are checked against finite differences of the curve the module
itself returns, under both volatility rules, so a sign slip on a short leg
or a missing smile term fails here.
"""

from datetime import date

import numpy as np
import pytest

from pricing import black_scholes as bs
from pricing.position import (
    Expiry,
    Leg,
    Smile,
    carry,
    leg_quotes,
    position_curve,
    time_left,
)

CAPTURE = date(2026, 10, 20)
NEAR, FAR = date(2026, 11, 20), date(2026, 12, 18)
S0, R = 100.0, 0.04
# A downward-sloping skew, like an equity index: puts dearer than calls.
SKEW_K = [-0.40, -0.17, -0.02, 0.09, 0.40]
SKEW_IV = [0.42, 0.27, 0.19, 0.16, 0.15]
EXPIRIES = {
    NEAR: Expiry(NEAR, 31.0 / 365, 100.25, Smile(SKEW_K, SKEW_IV)),
    FAR: Expiry(FAR, 59.0 / 365, 100.40, Smile(SKEW_K, [v + 0.01 for v in SKEW_IV])),
}
RULES = ("sticky_strike", "sticky_moneyness")


def call(k, side=1, qty=1, premium=0.0, expiry=NEAR):
    return Leg("call", side, qty, premium, k, expiry)


def put(k, side=1, qty=1, premium=0.0, expiry=NEAR):
    return Leg("put", side, qty, premium, k, expiry)


TEMPLATES = {
    "long straddle": [call(100, premium=4.0), put(100, premium=3.8)],
    "short strangle": [call(108, -1, premium=1.1), put(92, -1, premium=1.6)],
    "call butterfly": [call(95, premium=6.5), call(100, -1, 2, 3.6), call(105, premium=1.6)],
    "iron condor": [
        put(88, premium=0.9),
        put(94, -1, premium=1.9),
        call(106, -1, premium=1.4),
        call(112, premium=0.4),
    ],
    "collar": [Leg("stock", 1, 100, 100.0), put(93, premium=1.7), call(107, -1, premium=1.2)],
    "put ratio": [put(100, premium=3.8), put(92, -1, 2, 1.6)],
    "call calendar": [call(100, -1, premium=2.6), call(100, 1, premium=3.9, expiry=FAR)],
}


def curve(legs, spots, as_of=CAPTURE, rule="sticky_strike", **shift):
    return position_curve(legs, EXPIRIES, spots, S0, R, CAPTURE, as_of, rule, **shift)


# Spots whose log-moneyness stays clear of every smile node for every strike
# above, so the linear smile is differentiable where it is bumped.
SPOTS = [91.3, 97.1, 103.3, 110.7]


@pytest.mark.parametrize("rule", RULES)
@pytest.mark.parametrize("name", sorted(TEMPLATES))
@pytest.mark.parametrize("as_of", [CAPTURE, date(2026, 11, 5)])
def test_greeks_are_the_slopes_of_the_curve(name, rule, as_of):
    legs = TEMPLATES[name]
    c = curve(legs, SPOTS, as_of, rule)
    h = 1e-3
    up, down = (
        curve(legs, [s + h for s in SPOTS], as_of, rule),
        curve(legs, [s - h for s in SPOTS], as_of, rule),
    )
    np.testing.assert_allclose(c["delta"], (up["value"] - down["value"]) / (2 * h), atol=1e-4)
    gamma = (up["value"] - 2 * c["value"] + down["value"]) / h**2
    np.testing.assert_allclose(c["gamma"], gamma, atol=2e-3)

    def bumped(**kw):
        return curve(legs, SPOTS, as_of, rule, **kw)["value"]

    dv = 1e-5
    vega = (bumped(vol_shift=dv) - bumped(vol_shift=-dv)) / (2 * dv) / 100
    np.testing.assert_allclose(c["vega"], vega, atol=1e-4)
    dr = 1e-6
    rho = (bumped(rate_shift=dr) - bumped(rate_shift=-dr)) / (2 * dr) / 100
    np.testing.assert_allclose(c["rho"], rho, atol=1e-4)
    dt = 1e-3  # days
    theta = (bumped(day_shift=dt) - bumped(day_shift=-dt)) / (2 * dt)
    np.testing.assert_allclose(c["theta"], theta, atol=1e-4)


@pytest.mark.parametrize("rule", RULES)
@pytest.mark.parametrize("name", sorted(set(TEMPLATES) - {"call calendar"}))
def test_on_the_expiry_date_the_curve_is_the_payoff(name, rule):
    legs = TEMPLATES[name]
    spots = np.linspace(80, 120, 41)
    payoff = sum(
        leg.units
        * (
            (np.maximum(spots - leg.strike, 0) if leg.kind == "call" else 0)
            + (np.maximum(leg.strike - spots, 0) if leg.kind == "put" else 0)
            + (spots if leg.kind == "stock" else 0)
            - leg.premium
        )
        for leg in legs
    )
    np.testing.assert_allclose(curve(legs, spots, NEAR, rule)["profit"], payoff, atol=1e-9)


def test_the_time_left_reaches_zero_on_the_expiry_date():
    e = EXPIRIES[NEAR]
    assert time_left(e, CAPTURE, CAPTURE) == e.T0
    assert time_left(e, CAPTURE, date(2026, 11, 19)) == pytest.approx(e.T0 - 30 / 365)
    assert time_left(e, CAPTURE, NEAR) == 0.0
    assert time_left(e, CAPTURE, date(2026, 12, 1)) == 0.0


def test_the_carry_reproduces_the_parity_forward():
    for e in EXPIRIES.values():
        q = carry(e, S0, R)
        assert S0 * np.exp((R - q) * e.T0) == pytest.approx(e.forward, rel=1e-12)


def test_at_the_capture_a_leg_is_black_76_at_its_own_strikes_vol():
    e = EXPIRIES[NEAR]
    for leg in (put(85), put(97), call(100), call(115)):
        sigma, _, _ = e.smile.at(np.log(leg.strike / e.forward))
        black = np.exp(-R * e.T0) * bs.price(e.forward, leg.strike, e.T0, 0.0, 0.0, sigma, leg.kind)
        for rule in RULES:
            assert curve([leg], [S0], rule=rule)["value"][0] == pytest.approx(
                100 * black, rel=1e-10
            )


def test_the_two_rules_agree_at_the_capture_and_part_away_from_it():
    legs = TEMPLATES["put ratio"]
    a, b = curve(legs, [S0, 92.0]), curve(legs, [S0, 92.0], rule="sticky_moneyness")
    assert a["value"][0] == pytest.approx(b["value"][0], rel=1e-12)
    # At 92 the short 92 puts are at the money: moneyness gives them the
    # cheaper at-the-money vol, strike keeps their dearer skew vol.
    assert a["value"][1] - b["value"][1] < -50
    qa, qb = (
        leg_quotes(legs, EXPIRIES, S0, R, CAPTURE, CAPTURE, rule)
        for rule in ("sticky_strike", "sticky_moneyness")
    )
    assert [x.iv for x in qa] == pytest.approx([x.iv for x in qb], rel=1e-12)


def test_on_a_downward_skew_moneyness_adds_delta_to_a_long_put():
    # Sticky moneyness: spot up -> ln(K/F) down -> a dearer vol, so a long
    # option gains vega on the way up. The put's delta is less negative.
    leg = [put(90)]
    strike, mny = curve(leg, [S0]), curve(leg, [S0], rule="sticky_moneyness")
    assert mny["delta"][0] > strike["delta"][0]
    sigma, slope, _ = EXPIRIES[NEAR].smile.at(np.log(90 / EXPIRIES[NEAR].forward))
    assert slope < 0
    assert mny["delta"][0] - strike["delta"][0] == pytest.approx(
        strike["vega"][0] * 100 * -slope / S0, rel=1e-10
    )


def test_a_calendar_at_the_near_expiry_pays_the_near_leg_and_prices_the_far():
    near, far = TEMPLATES["call calendar"]
    e = EXPIRIES[FAR]
    T = e.T0 - 31 / 365
    q = carry(e, S0, R)
    for s in (92.0, 100.0, 108.0):
        got = curve([near, far], [s], NEAR)["profit"][0]
        far_value = bs.price(s, 100.0, T, R, q, e.smile.at(np.log(100 / e.forward))[0], "call")
        want = -100 * (max(s - 100, 0) - 2.6) + 100 * (far_value - 3.9)
        assert got == pytest.approx(want, rel=1e-12)


def test_profit_is_value_less_the_premiums_and_the_short_leg_counts_negative():
    legs = TEMPLATES["short strangle"]
    c = curve(legs, [S0])
    single = [curve([leg], [S0])["value"][0] for leg in legs]
    assert all(v < 0 for v in single)
    assert c["value"][0] == pytest.approx(sum(single))
    assert c["profit"][0] == pytest.approx(c["value"][0] + 100 * (1.1 + 1.6))
    assert c["vega"][0] < 0 and c["theta"][0] > 0 and c["gamma"][0] < 0


def test_the_smile_is_linear_inside_and_flat_outside():
    s = Smile([0.1, -0.1, np.nan, 0.0, 0.1], [0.2, 0.3, 0.5, 0.25, 0.9])
    assert s.k.tolist() == [-0.1, 0.0, 0.1]  # NaN dropped, the first 0.1 kept
    sigma, slope, inside = s.at([-0.5, -0.05, 0.05, 0.5])
    assert sigma.tolist() == pytest.approx([0.3, 0.275, 0.225, 0.2])
    assert slope.tolist() == pytest.approx([0.0, -0.5, -0.5, 0.0])
    assert inside.tolist() == [False, True, True, False]
    with pytest.raises(ValueError):
        Smile([0.0], [0.2])


def test_leg_quotes_say_where_the_vol_came_from():
    legs = [put(60), call(100), Leg("stock", 1, 100, 100.0)]
    quotes = leg_quotes(legs, EXPIRIES, S0, R, CAPTURE, CAPTURE)
    assert [q.vol_source for q in quotes] == ["extrapolated", "interpolated", None]
    assert quotes[0].iv == SKEW_IV[0]
    assert quotes[1].forward == pytest.approx(EXPIRIES[NEAR].forward)
