"""
pricing/chains.py, pricing/dividends.py and the vectorised pieces they use.

Synthetic chains are priced from a known sigma and forward, so the surface
must recover both. Each synthetic test also checks the fixture actually
exercises the behaviour (a raw parity forward that IS biased, a spot that IS
off), so it cannot pass vacuously. The real fixture is 198 rows of SPY
2026-10-07 (expiries 10-16 and 11-20, 50 strikes each around the money).
"""

from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pricing import black_scholes as bs
from pricing.binomial import crr_price, escrowed_prices
from pricing.chains import expiry_time, normalise, price_chain, term_structure
from pricing.dividends import Dividend, dividends_before, near_ex_date, project_dividends, pv_dividends

CAPTURED = datetime(2026, 10, 7, 19, 45, tzinfo=timezone.utc)  # 15:45 New York
FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "spy_chain_2026-10-07_slice.parquet"


def years_to(expiry: str) -> float:
    return (expiry_time(expiry) - CAPTURED).total_seconds() / (365 * 86400)


def synthetic_chain(prices_for, expiries, strikes, spot_recorded=100.0, half_spread=0.01):
    """
    A vendor-shaped frame. prices_for(expiry, T, K array, is_call) returns
    the TRUE prices; bid/ask straddle them by half_spread.
    """
    rows = []
    for e in expiries:
        T = years_to(e)
        for is_call in (True, False):
            p = prices_for(e, T, np.asarray(strikes, float), is_call)
            for k, v in zip(strikes, p):
                rows.append(
                    dict(
                        underlying="SYN", expiry=e, right="C" if is_call else "P", strike=float(k),
                        bid=v - half_spread, ask=v + half_spread, impliedVolatility=0.0,
                        lastTradeDate=pd.Timestamp(CAPTURED), captured_at_utc=pd.Timestamp(CAPTURED),
                        spot=spot_recorded, openInterest=1,
                    )
                )
    return pd.DataFrame(rows)


EXPIRIES = ["2026-10-30", "2026-11-20", "2026-12-31"]
STRIKES = np.arange(80.0, 121.0, 2.5)
SIGMA = 0.22


def european_prices(forwards, r):
    def f(e, T, K, is_call):
        return bs.price(forwards[e], K, T, r, r, SIGMA, "call" if is_call else "put")
    return f


# --- the forward measure: no recorded spot -----------------------------------

def test_european_chain_recovers_sigma_and_forward_and_ignores_recorded_spot():
    # r = 0 and no dividends: an American option is its European twin, so the
    # premium is zero and recovery must be exact up to the IV solver.
    forwards = {e: 100.0 for e in EXPIRIES}
    frame = synthetic_chain(european_prices(forwards, 0.0), EXPIRIES, STRIKES)
    skewed = frame.assign(spot=frame["spot"] * 1.002)  # 0.2% off, as AAPL was
    a, b = price_chain(frame, 0.0), price_chain(skewed, 0.0)
    pd.testing.assert_frame_equal(a.rows, b.rows)
    priced = a.rows[a.rows["iv_reason"] == ""]
    assert len(priced) > 0.9 * len(a.rows)
    assert np.abs(priced["iv"] - SIGMA).max() < 1e-8
    assert np.abs(a.expiries["forward"] - 100.0).max() < 1e-8


def test_each_expiry_is_priced_off_its_own_forward():
    # Quotes fetched seconds apart: each expiry implies a different spot.
    forwards = {"2026-10-30": 99.80, "2026-11-20": 100.00, "2026-12-31": 100.25}
    frame = synthetic_chain(european_prices(forwards, 0.0), EXPIRIES, STRIKES)
    result = price_chain(frame, 0.0)
    solved = result.expiries.set_index("expiry")["forward"]
    for e, f in forwards.items():
        assert solved[e] == pytest.approx(f, abs=1e-8)
    priced = result.rows[result.rows["iv_reason"] == ""]
    assert np.abs(priced["iv"] - SIGMA).max() < 1e-8
    # And a single spot would have been wrong by the size of the skew.
    assert result.quality["implied_spot_spread_bp"] == pytest.approx(1e4 * 0.45 / 100.0, rel=1e-3)


# --- American quotes: the joint forward/premium solve ----------------------

R = 0.05
DIV = [Dividend(date(2026, 12, 4), 1.0, projected=True)]


def american_prices(forwards, valuation=date(2026, 10, 7)):
    def f(e, T, K, is_call):
        schedule = dividends_before(DIV, valuation, date.fromisoformat(e))
        F = forwards[e]
        am, _ = escrowed_prices(F * np.exp(-R * T), K, T, R, SIGMA, is_call, divs=[schedule] * K.size)
        return am
    return f


@pytest.fixture(scope="module")
def american():
    forwards = {e: 100.0 * np.exp(R * years_to(e)) for e in EXPIRIES}
    frame = synthetic_chain(american_prices(forwards), EXPIRIES, STRIKES, half_spread=0.005)
    return forwards, price_chain(frame, R, DIV)


def test_raw_parity_on_american_quotes_is_biased(american):
    # The fixture must exercise the correction: raw parity is measurably off.
    forwards, result = american
    raw = result.expiries.set_index("expiry")["raw_parity_forward"]
    worst = max(abs(raw[e] / forwards[e] - 1) for e in EXPIRIES)
    assert worst > 2e-4  # 2 bp, far above the 1e-6 the solve achieves


def test_joint_solve_recovers_forward_and_sigma(american):
    forwards, result = american
    solved = result.expiries.set_index("expiry")
    for e in EXPIRIES:
        assert solved.loc[e, "forward"] == pytest.approx(forwards[e], rel=2e-6)
        assert solved.loc[e, "forward_converged"]
    otm = result.rows[result.rows["otm"] & (result.rows["iv_reason"] == "")]
    # Not exact: the premium is priced on the tree, the European price by
    # formula, and the two differ by the tree's own discretisation error.
    assert np.abs(otm["iv"] - SIGMA).max() < 2e-4


def test_without_the_correction_puts_read_high(american):
    # The defect the plan exists to fix: invert Black-76 on American put mids.
    forwards, result = american
    rows = result.rows[~result.rows["is_call"] & (result.rows["iv_reason"] == "") & (result.rows["T"] > 0.15)]
    naive, _ = bs.implied_vol_black76_many(rows["mid"], rows["F"], rows["strike"], rows["T"], R, False)
    assert np.nanmax(naive - SIGMA) > 0.005
    # Out of the money to 5% in: within 2e-4. Deeper in the money the
    # premium is most of the time value and the one-lattice-vs-formula
    # mismatch shows: measured 1.0e-3 at 17% ITM. The surface uses OTM only.
    near = rows["log_moneyness"] < 0.05
    assert np.abs(rows.loc[near, "iv"] - SIGMA).max() < 2e-4
    assert np.abs(rows.loc[~near, "iv"] - SIGMA).max() < 2e-3


# --- flags and the surface ---------------------------------------------------

def test_quality_flags():
    forwards = {e: 100.0 for e in EXPIRIES}
    frame = synthetic_chain(european_prices(forwards, 0.0), EXPIRIES, STRIKES)
    frame.loc[0, ["bid", "ask"]] = [0.0, 0.05]
    frame.loc[1, ["bid", "ask"]] = [1.10, 1.00]
    frame.loc[2, ["bid", "ask"]] = [1.00, 1.00]
    same_day = synthetic_chain(european_prices({"2026-10-07": 100.0}, 0.0), ["2026-10-07"], STRIKES[:3])
    n = normalise(pd.concat([frame, same_day], ignore_index=True))
    assert bool(n.loc[0, "zero_bid"]) and bool(n.loc[1, "crossed"]) and bool(n.loc[2, "locked"])
    assert n["zero_dte"].sum() == 6
    assert not n.loc[:2, "usable"].any()


def test_term_structure_on_a_flat_surface():
    forwards = {e: 100.0 for e in EXPIRIES}
    result = price_chain(synthetic_chain(european_prices(forwards, 0.0), EXPIRIES, STRIKES), 0.0)
    ts = term_structure(result)
    assert len(ts) == len(EXPIRIES)
    assert np.abs(ts["atm_iv"] - SIGMA).max() < 1e-8
    assert np.abs(ts["skew_25d"]).max() < 1e-8


# --- the real SPY slice ------------------------------------------------------

@pytest.fixture(scope="module")
def spy():
    frame = pd.read_parquet(FIXTURE)
    divs = [Dividend(date(2026, 12, 18), 1.889, projected=True)]
    return price_chain(frame, 0.041141, divs)


def test_spy_fixture_has_what_the_correction_needs(spy):
    rows = spy.rows
    long_puts = rows[~rows["is_call"] & (rows["T"] > 30 / 365) & (rows["log_moneyness"].abs() < 0.03)]
    assert len(long_puts) >= 20
    assert spy.expiries["forward_converged"].all()


def test_spy_same_strike_calls_and_puts_agree_after_correction(spy):
    ok = spy.rows[spy.rows["iv_reason"] == ""]
    c = ok[ok["is_call"]].set_index(["expiry", "strike"])
    p = ok[~ok["is_call"]].set_index(["expiry", "strike"])
    j = c.join(p, lsuffix="_c", rsuffix="_p", how="inner")
    j = j[(j["log_moneyness_c"].abs() < 0.03) & (j["T_c"] > 30 / 365)]
    K = j.index.get_level_values(1).to_numpy(float)
    T = j["T_c"].to_numpy()
    vega = bs.greeks(j["F_c"].to_numpy(), K, T, 0.041141, 0.041141, j["iv_c"].to_numpy(), "call").vega
    half_spread_vol = (((j["ask_c"] - j["bid_c"]) + (j["ask_p"] - j["bid_p"])) / 2).to_numpy() / vega
    fixed = np.abs(j["iv_c"] - j["iv_p"]).to_numpy()
    raw_f = spy.expiries.set_index("expiry")["raw_parity_forward"]
    Fr = np.asarray(j.index.get_level_values(0).map(raw_f), float)
    rc, _ = bs.implied_vol_black76_many(j["mid_c"], Fr, K, T, 0.041141, True)
    rp, _ = bs.implied_vol_black76_many(j["mid_p"], Fr, K, T, 0.041141, False)
    raw = np.abs(rc - rp)
    assert np.mean(raw > half_spread_vol) > 0.5   # uncorrected: most pairs disagree
    assert np.mean(fixed > half_spread_vol) < 0.15  # corrected: few do


# --- dividends -----------------------------------------------------------------

def test_projection_from_a_quarterly_history():
    hist = [(date(2025, 12, 19), 1.99), (date(2026, 3, 20), 1.80), (date(2026, 6, 18), 1.90), (date(2026, 9, 18), 1.89)]
    out = project_dividends(hist, date(2026, 10, 7), date(2027, 3, 31))
    assert [d.ex_date for d in out] == [date(2026, 12, 18), date(2027, 3, 19)]
    assert all(d.amount == 1.89 and d.projected for d in out)
    assert project_dividends(hist[:2], date(2026, 10, 7), date(2027, 3, 31)) == []
    assert near_ex_date(out, date(2026, 12, 16), 5) and not near_ex_date(out, date(2026, 11, 20), 5)
    # Both sides of the ex-date: an expiry a month AFTER it is not near it.
    assert near_ex_date(out, date(2026, 12, 21), 5) and not near_ex_date(out, date(2027, 1, 15), 5)


def test_dividends_before_counts_only_ex_dates_inside_the_life():
    divs = [Dividend(date(2026, 12, 18), 1.0, True), Dividend(date(2027, 3, 19), 1.0, True)]
    assert dividends_before(divs, date(2026, 10, 7), date(2026, 12, 17)) == []
    assert len(dividends_before(divs, date(2026, 10, 7), date(2026, 12, 18))) == 1
    assert pv_dividends([(0.5, 2.0)], 0.04) == pytest.approx(2.0 * np.exp(-0.02))


# --- vectorised pieces -----------------------------------------------------------

def test_escrowed_tree_without_dividends_is_the_scalar_tree():
    K = np.linspace(80, 120, 9)
    is_call = np.arange(9) % 2 == 0
    am, eu = escrowed_prices(100.0, K, 0.4, 0.05, 0.25, is_call, smooth=False)
    for i in range(9):
        right = "call" if is_call[i] else "put"
        assert am[i] == pytest.approx(crr_price(100, K[i], 0.4, 0.05, 0.0, 0.25, right, True, 500, False), abs=1e-12)
        assert eu[i] == pytest.approx(crr_price(100, K[i], 0.4, 0.05, 0.0, 0.25, right, False, 500, False), abs=1e-12)


def test_escrowed_european_is_black_scholes_on_the_risky_spot():
    S, K, T, r, s = 100.0, np.array([90.0, 100.0, 110.0]), 0.5, 0.04, 0.3
    divs = [[(0.25, 2.0)]] * 3
    s_risky = S - pv_dividends(divs[0], r)
    _, eu = escrowed_prices(s_risky, K, T, r, s, np.array([True, False, True]), divs=divs)
    exact = [bs.price(s_risky, K[0], T, r, 0, s, "call"), bs.price(s_risky, K[1], T, r, 0, s, "put"), bs.price(s_risky, K[2], T, r, 0, s, "call")]
    assert np.abs(eu - exact).max() <= 1e-4 * S * s * np.sqrt(T)


def test_a_dividend_before_expiry_makes_early_call_exercise_worth_something():
    K, T, r, s = np.array([80.0]), 0.5, 0.04, 0.2
    am0, eu0 = escrowed_prices(100.0, K, T, r, s, True)
    am1, eu1 = escrowed_prices(100.0 - 5.0 * np.exp(-r * 0.45), K, T, r, s, True, divs=[[(0.45, 5.0)]])
    assert am0 - eu0 == pytest.approx(0.0, abs=1e-12)
    assert am1 - eu1 > 0.5


def test_vectorised_black76_iv_matches_the_scalar_solver():
    rng = np.random.default_rng(3)
    K, T, s = rng.uniform(70, 130, 200), rng.uniform(0.02, 0.5, 200), rng.uniform(0.1, 0.8, 200)
    calls = rng.random(200) < 0.5
    prices = np.where(calls, bs.price(100, K, T, 0.04, 0.04, s, "call"), bs.price(100, K, T, 0.04, 0.04, s, "put"))
    sigma, why = bs.implied_vol_black76_many(prices, 100.0, K, T, 0.04, calls)
    for i in range(0, 200, 7):
        ref = bs.implied_vol(prices[i], 100, K[i], T[i], 0.04, 0.04, "call" if calls[i] else "put")
        if ref.reason is None:
            assert why[i] == "" and sigma[i] == pytest.approx(ref.sigma, abs=1e-9)
        else:
            assert why[i] == ref.reason


def test_deep_itm_call_before_a_big_dividend_respects_the_spot_bound():
    # Exercise just before the ex-date is worth about S - K. The add-back must
    # DISCOUNT the dividend to each node: compounding it instead lets the tree
    # value the call above the stock itself.
    r, t_div, D, K = 0.10, 0.9, 20.0, np.array([1.0])
    s_risky = 100.0
    spot = s_risky + D * np.exp(-r * t_div)
    am, _ = escrowed_prices(s_risky, K, 1.0, r, 0.2, True, divs=[[(t_div, D)]])
    assert spot - K[0] - 1e-9 <= am[0] <= spot


def test_vectorised_iv_has_no_vol_for_a_price_at_intrinsic_plus_float_noise():
    F, K, T, r = 100.0, np.array([130.0]), 0.5, 0.05
    lower = np.exp(-r * T) * (K - F)
    sigma, why = bs.implied_vol_black76_many(lower * (1 + 1e-14), F, K, T, r, False)
    assert np.isnan(sigma[0]) and why[0] == "at_or_below_lower_bound"


def test_one_row_below_the_tree_floor_does_not_fail_the_chain():
    # A quote implying sigma below |r| sqrt(dt) cannot go on the lattice. It
    # must get its own reason; before, escrowed_prices raised for the batch.
    # At the money (F = K = 100) a 0.08% vol still prices above one cent, so
    # the row is not flagged zero_bid and does reach the tree.
    e = "2026-12-31"
    forwards = {x: 100.0 * np.exp(R * years_to(x)) for x in EXPIRIES}
    forwards[e] = 100.0
    frame = synthetic_chain(american_prices(forwards), EXPIRIES, STRIKES, half_spread=0.005)
    T = years_to(e)
    floor = abs(R) * np.sqrt(T / 500)
    sigma_tiny = 0.0008
    assert sigma_tiny < floor
    tiny = bs.price(100.0, 100.0, T, R, R, sigma_tiny, "call")
    assert tiny > 0.01
    row = (frame["expiry"] == e) & (frame["strike"] == 100.0) & (frame["right"] == "C")
    frame.loc[row, ["bid", "ask"]] = [tiny, tiny + 0.002]
    result = price_chain(frame, R, DIV)
    hit = result.rows[(result.rows["expiry"] == e) & (result.rows["strike"] == 100.0) & result.rows["is_call"]]
    assert hit["iv_reason"].iloc[0] == "below_tree_floor"
    others = result.rows[result.rows["otm"] & (result.rows["iv_reason"] == "")]
    assert len(others) >= 40 and np.abs(others["iv"] - SIGMA).max() < 2e-4  # 42 OTM rows price
