"""
pricing/binomial.py

Cox-Ross-Rubinstein tree with a continuous dividend yield, European and
American. Every option in the chain archive is American-style (the ETF
options included), so the European tree is the check against Black-Scholes
and the American tree is the price.

    u = exp(sigma * sqrt(dt)),  d = 1/u,  p = (exp((r - q) dt) - d) / (u - d)

Early exercise is checked at EVERY node, not only at expiry. The premium
over the European price is reported separately, because "how much of this
price is the right to exercise early" is the number that decides whether a
Black-Scholes IV is usable for that option.

CONVERGENCE: CRR's error is O(1/N) and oscillates with the parity of N,
largest at the money. `crr_price(..., smooth=True)` averages N and N+1,
which cancels most of the oscillation. The measured error at 500 steps is
in the plan's phase 1 row.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from config.settings import BINOMIAL_STEPS


def _tree_price(S, K, T, r, q, sigma, right, american, steps):
    if right not in ("call", "put"):
        raise ValueError(f"right must be 'call' or 'put'; got {right!r}")
    if S <= 0 or K <= 0:
        raise ValueError("S and K must be positive")
    if T < 0 or sigma < 0:
        raise ValueError("T and sigma must be >= 0")
    if steps < 1:
        raise ValueError("steps must be >= 1")
    if T == 0:
        return max(S - K, 0.0) if right == "call" else max(K - S, 0.0)
    if sigma == 0:
        # Degenerate tree (u == d): the forward is certain.
        from pricing.black_scholes import price as bs_price

        european = bs_price(S, K, T, r, q, 0.0, right)
        if not american:
            return european
        intrinsic = max(S - K, 0.0) if right == "call" else max(K - S, 0.0)
        return max(european, intrinsic)

    dt = T / steps
    u = np.exp(sigma * np.sqrt(dt))
    d = 1.0 / u
    growth = np.exp((r - q) * dt)
    p = (growth - d) / (u - d)
    if not 0.0 < p < 1.0:
        raise ValueError(
            f"risk-neutral probability {p:.6f} outside (0, 1): steps={steps} is too "
            "few for this rate and volatility"
        )
    disc = np.exp(-r * dt)
    sign = 1.0 if right == "call" else -1.0

    j = np.arange(steps + 1)
    spot = S * u ** (steps - 2 * j)  # node prices at expiry, highest first
    value = np.maximum(sign * (spot - K), 0.0)
    for n in range(steps - 1, -1, -1):
        value = disc * (p * value[:-1] + (1.0 - p) * value[1:])
        if american:
            spot = spot[:-1] / u  # one level back: S * u**(n - 2j)
            np.maximum(value, sign * (spot - K), out=value)
    return float(value[0])


def crr_price(
    S: float,
    K: float,
    T: float,
    r: float,
    q: float,
    sigma: float,
    right: str = "call",
    american: bool = True,
    steps: int = BINOMIAL_STEPS,
    smooth: bool = True,
) -> float:
    """
    CRR price. smooth=True averages `steps` and `steps + 1` to cancel the
    odd/even oscillation; smooth=False is the plain N-step tree.
    """
    one = _tree_price(S, K, T, r, q, sigma, right, american, steps)
    if not smooth:
        return one
    return 0.5 * (one + _tree_price(S, K, T, r, q, sigma, right, american, steps + 1))


@dataclass(frozen=True)
class AmericanPrice:
    american: float
    european: float
    #: american - european; zero for a call on a non-dividend payer.
    early_exercise_premium: float


def american_price(
    S: float,
    K: float,
    T: float,
    r: float,
    q: float,
    sigma: float,
    right: str = "call",
    steps: int = BINOMIAL_STEPS,
    smooth: bool = True,
) -> AmericanPrice:
    am = crr_price(S, K, T, r, q, sigma, right, True, steps, smooth)
    eu = crr_price(S, K, T, r, q, sigma, right, False, steps, smooth)
    return AmericanPrice(am, eu, am - eu)
