"""
pricing/black_scholes.py

Black-Scholes-Merton with a continuous dividend yield: price, Greeks and
implied volatility. Vectorised over any broadcastable inputs.

UNITS
    price    per share
    delta    dV/dS
    gamma    d2V/dS2
    vega     dV/dsigma per 1.00 of vol (divide by 100 for "per vol point")
    theta    dV/dt per YEAR of calendar time passing (= -dV/dT); divide by
             365 for per day. Negative for a long vanilla option almost always.
    rho      dV/dr per 1.00 of rate

EDGES, DEFINED RATHER THAN LEFT TO NaN
    T == 0       price is intrinsic, max(S-K, 0) or max(K-S, 0). Delta is the
                 exercise indicator (0 at the strike); every other Greek is 0.
    sigma == 0   the forward is certain: price is the discounted forward
                 intrinsic, e^{-rT} max(F-K, 0). Greeks follow the same limit.
    T < 0, sigma < 0, S <= 0, K <= 0   ValueError.

IMPLIED VOLATILITY
    `implied_vol` inverts a pricer by brentq on OPTIONS_IV_BOUNDS and returns
    NaN WITH A REASON when no volatility can produce the price, instead of a
    wrong number. style="american" inverts the CRR tree: the archive's
    options are American, and inverting BSM on an American put's mid pushes
    deep in-the-money IV up — the vendor-IV defect the plan exists to fix.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
from scipy.optimize import brentq
from scipy.stats import norm

from config.settings import BINOMIAL_STEPS, OPTIONS_IV_BOUNDS

Right = Literal["call", "put"]
Style = Literal["european", "american"]


def _check_right(right: str) -> None:
    if right not in ("call", "put"):
        raise ValueError(f"right must be 'call' or 'put'; got {right!r}")


def _inputs(S, K, T, r, q, sigma):
    S, K, T, r, q, sigma = (np.asarray(x, dtype=float) for x in (S, K, T, r, q, sigma))
    if np.any(S <= 0) or np.any(K <= 0):
        raise ValueError("S and K must be positive")
    if np.any(T < 0):
        raise ValueError("T must be >= 0 (years)")
    if np.any(sigma < 0):
        raise ValueError("sigma must be >= 0")
    return np.broadcast_arrays(S, K, T, r, q, sigma)


def _d1_d2(S, K, T, r, q, sigma):
    """d1, d2 where sigma*sqrt(T) > 0; NaN elsewhere (handled by callers)."""
    vol = sigma * np.sqrt(T)
    live = vol > 0
    safe = np.where(live, vol, 1.0)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / safe
    d2 = d1 - safe
    return np.where(live, d1, np.nan), np.where(live, d2, np.nan), live


def _scalar_or_array(x):
    return float(x) if np.ndim(x) == 0 else x


def price(S, K, T, r, q, sigma, right: Right = "call"):
    _check_right(right)
    S, K, T, r, q, sigma = _inputs(S, K, T, r, q, sigma)
    d1, d2, live = _d1_d2(S, K, T, r, q, sigma)
    dq, dr = np.exp(-q * T), np.exp(-r * T)
    if right == "call":
        bs = S * dq * norm.cdf(d1) - K * dr * norm.cdf(d2)
        dead = np.maximum(S * dq - K * dr, 0.0)  # T == 0 gives max(S-K, 0)
    else:
        bs = K * dr * norm.cdf(-d2) - S * dq * norm.cdf(-d1)
        dead = np.maximum(K * dr - S * dq, 0.0)
    return _scalar_or_array(np.where(live, bs, dead))


@dataclass(frozen=True)
class Greeks:
    delta: object
    gamma: object
    vega: object
    theta: object
    rho: object


def greeks(S, K, T, r, q, sigma, right: Right = "call") -> Greeks:
    _check_right(right)
    S, K, T, r, q, sigma = _inputs(S, K, T, r, q, sigma)
    d1, d2, live = _d1_d2(S, K, T, r, q, sigma)
    dq, dr = np.exp(-q * T), np.exp(-r * T)
    root = np.sqrt(np.where(live, T, 1.0))
    pdf = norm.pdf(np.where(live, d1, 0.0))
    d1z, d2z = np.where(live, d1, 0.0), np.where(live, d2, 0.0)

    gamma = np.where(live, dq * pdf / (S * np.where(live, sigma, 1.0) * root), 0.0)
    vega = np.where(live, S * dq * pdf * root, 0.0)
    decay = -S * dq * pdf * sigma / (2 * root)

    # Dead (vol*sqrt(T) == 0) limits: the option is a discounted forward
    # contract when in the money and worth nothing when out.
    fwd_itm_call = S * dq > K * dr
    if right == "call":
        delta = np.where(live, dq * norm.cdf(d1z), np.where(fwd_itm_call, dq, 0.0))
        theta = np.where(
            live,
            decay - r * K * dr * norm.cdf(d2z) + q * S * dq * norm.cdf(d1z),
            np.where(fwd_itm_call & (T > 0), q * S * dq - r * K * dr, 0.0),
        )
        rho = np.where(live, K * T * dr * norm.cdf(d2z), np.where(fwd_itm_call, K * T * dr, 0.0))
    else:
        fwd_itm_put = K * dr > S * dq
        delta = np.where(live, -dq * norm.cdf(-d1z), np.where(fwd_itm_put, -dq, 0.0))
        theta = np.where(
            live,
            decay + r * K * dr * norm.cdf(-d2z) - q * S * dq * norm.cdf(-d1z),
            np.where(fwd_itm_put & (T > 0), r * K * dr - q * S * dq, 0.0),
        )
        rho = np.where(live, -K * T * dr * norm.cdf(-d2z), np.where(fwd_itm_put, -K * T * dr, 0.0))
    return Greeks(*(_scalar_or_array(g) for g in (delta, gamma, vega, theta, rho)))


# ---------------------------------------------------------------------------
# Implied volatility
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ImpliedVol:
    sigma: float
    #: None when sigma is a number; otherwise why there is none.
    reason: Optional[str]


def no_arbitrage_bounds(S, K, T, r, q, right: Right, style: Style = "european"):
    """(lower, upper) an option price must lie strictly between to have an IV."""
    dq, dr = np.exp(-q * T), np.exp(-r * T)
    if right == "call":
        lower = max(S * dq - K * dr, 0.0)
        upper = S * dq if style == "european" else S
        if style == "american":
            lower = max(lower, S - K)
    else:
        lower = max(K * dr - S * dq, 0.0)
        upper = K * dr if style == "european" else K
        if style == "american":
            lower = max(lower, K - S)
    return lower, upper


def implied_vol(
    option_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    q: float,
    right: Right = "call",
    style: Style = "european",
    bounds: tuple = OPTIONS_IV_BOUNDS,
    steps: int = BINOMIAL_STEPS,
    xtol: float = 1e-12,
) -> ImpliedVol:
    """
    The sigma at which the chosen pricer returns `option_price`.

    Reasons, in the order checked: 'non_positive_time', 'non_finite_price',
    'at_or_below_lower_bound' (no time value: intrinsic or less),
    'at_or_above_upper_bound', 'outside_search_bounds' (a sigma exists but
    not inside `bounds`).
    """
    _check_right(right)
    if style not in ("european", "american"):
        raise ValueError(f"style must be 'european' or 'american'; got {style!r}")
    if not T > 0:
        return ImpliedVol(np.nan, "non_positive_time")
    if not np.isfinite(option_price):
        return ImpliedVol(np.nan, "non_finite_price")
    lower, upper = no_arbitrage_bounds(S, K, T, r, q, right, style)
    # A price within float noise of the lower bound has no time value and so
    # no volatility in it: an American put past its exercise boundary prices
    # at intrinsic for every sigma, and brentq would return an arbitrary one.
    if option_price <= lower + 1e-10 * max(1.0, upper):
        return ImpliedVol(np.nan, "at_or_below_lower_bound")
    if option_price >= upper:
        return ImpliedVol(np.nan, "at_or_above_upper_bound")

    if style == "european":
        def model(sig: float) -> float:
            return price(S, K, T, r, q, sig, right)
    else:
        from pricing.binomial import crr_price

        def model(sig: float) -> float:
            return crr_price(S, K, T, r, q, sig, right, american=True, steps=steps)

    lo, hi = bounds
    if style == "american":
        # CRR needs sigma sqrt(dt) > |r - q| dt, or its probability leaves
        # (0, 1). Below that floor the tree has no price, so the search starts
        # just above it; a price only reachable lower is outside_search_bounds.
        lo = max(lo, 1.001 * abs(r - q) * np.sqrt(T / steps))
    f_lo, f_hi = model(lo) - option_price, model(hi) - option_price
    if f_lo > 0 or f_hi < 0:
        return ImpliedVol(np.nan, "outside_search_bounds")
    if f_lo == 0:
        return ImpliedVol(lo, None)
    if f_hi == 0:
        return ImpliedVol(hi, None)
    sigma = brentq(lambda s: model(s) - option_price, lo, hi, xtol=xtol, maxiter=200)
    return ImpliedVol(float(sigma), None)
