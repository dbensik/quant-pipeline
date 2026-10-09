"""
pricing/registry.py

The option pricers the API lists, described rather than hardcoded in the
router (the analysis/registry.py pattern). POST /api/v1/options/price runs
every model that supports the requested exercise style and returns them side
by side, so the closed form, the tree and Monte Carlo can be compared on one
set of inputs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from config.settings import BINOMIAL_STEPS, MC_PRICER_PATHS
from pricing import black_scholes as bs
from pricing.binomial import american_price, crr_price
from pricing.monte_carlo import european_mc


@dataclass(frozen=True)
class PricerInputs:
    S: float
    K: float
    T: float
    r: float
    q: float
    sigma: float
    right: str
    seed: int = 42
    mc_paths: int = MC_PRICER_PATHS
    steps: int = BINOMIAL_STEPS


@dataclass(frozen=True)
class Quote:
    price: float
    std_error: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None
    rho: Optional[float] = None
    early_exercise_premium: Optional[float] = None


@dataclass(frozen=True)
class PricerSpec:
    id: str
    display_name: str
    description: str
    styles: Tuple[str, ...]
    run: Callable[[PricerInputs, str], Quote]


def _black_scholes(x: PricerInputs, style: str) -> Quote:
    g = bs.greeks(x.S, x.K, x.T, x.r, x.q, x.sigma, x.right)
    return Quote(
        price=bs.price(x.S, x.K, x.T, x.r, x.q, x.sigma, x.right),
        delta=g.delta, gamma=g.gamma, vega=g.vega, theta=g.theta, rho=g.rho,
    )


def _crr(x: PricerInputs, style: str) -> Quote:
    if style == "american":
        a = american_price(x.S, x.K, x.T, x.r, x.q, x.sigma, x.right, steps=x.steps)
        price, premium = a.american, a.early_exercise_premium
    else:
        price, premium = crr_price(x.S, x.K, x.T, x.r, x.q, x.sigma, x.right, american=False, steps=x.steps), None
    # Delta and gamma by bumping spot on the same tree (1% of S, central).
    h = 0.01 * x.S
    american = style == "american"
    up = crr_price(x.S + h, x.K, x.T, x.r, x.q, x.sigma, x.right, american, x.steps)
    down = crr_price(x.S - h, x.K, x.T, x.r, x.q, x.sigma, x.right, american, x.steps)
    return Quote(
        price=price,
        delta=(up - down) / (2 * h),
        gamma=(up - 2 * price + down) / h**2,
        early_exercise_premium=premium,
    )


def _monte_carlo(x: PricerInputs, style: str) -> Quote:
    mc = european_mc(x.S, x.K, x.T, x.r, x.q, x.sigma, x.right, np.random.default_rng(x.seed), paths=x.mc_paths)
    return Quote(price=mc.price, std_error=mc.std_error, delta=mc.delta)


PRICERS: List[PricerSpec] = [
    PricerSpec(
        "black_scholes", "Black-Scholes-Merton",
        "Closed form with a continuous dividend yield. European exercise only.",
        ("european",), _black_scholes,
    ),
    PricerSpec(
        "crr_tree", "Cox-Ross-Rubinstein tree",
        f"{BINOMIAL_STEPS}-step binomial tree, N and N+1 averaged. American or European; "
        "the American price reports its early-exercise premium separately.",
        ("european", "american"), _crr,
    ),
    PricerSpec(
        "monte_carlo", "Monte Carlo (antithetic)",
        "Risk-neutral GBM, seeded, standard error over antithetic pairs. European only.",
        ("european",), _monte_carlo,
    ),
]

BY_ID: Dict[str, PricerSpec] = {p.id: p for p in PRICERS}
