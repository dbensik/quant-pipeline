"""
pricing/monte_carlo.py

Risk-neutral GBM pricing of a European payoff with antithetic variates.

This exists to (a) check the closed form and (b) be the base for anything
path-dependent later. It is NOT simulation/resample.gbm, which is fitted to a
series' realised drift; a pricer needs the risk-neutral drift r - q.

STANDARD ERROR WITH ANTITHETICS
    Draws come in pairs (Z, -Z), and the two halves of a pair are negatively
    correlated by construction. The standard error is therefore computed over
    the n/2 PAIR AVERAGES. Computed over all n draws it would treat
    correlated samples as independent and misstate the error.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from config.settings import MC_PRICER_PATHS


@dataclass(frozen=True)
class MCPrice:
    price: float
    std_error: float
    #: Pathwise delta estimate and its standard error (same pairing).
    delta: float
    delta_std_error: float
    paths: int


def european_mc(
    S: float,
    K: float,
    T: float,
    r: float,
    q: float,
    sigma: float,
    right: str,
    rng: np.random.Generator,
    paths: int = MC_PRICER_PATHS,
) -> MCPrice:
    if right not in ("call", "put"):
        raise ValueError(f"right must be 'call' or 'put'; got {right!r}")
    if S <= 0 or K <= 0 or T <= 0 or sigma < 0:
        raise ValueError("need S, K, T > 0 and sigma >= 0")
    if paths < 2 or paths % 2:
        raise ValueError("paths must be an even number >= 2 (antithetic pairs)")
    if not isinstance(rng, np.random.Generator):
        raise TypeError("rng must be a numpy Generator; pass one explicitly")

    half = paths // 2
    z = rng.standard_normal(half)
    z = np.concatenate([z, -z])
    drift = (r - q - 0.5 * sigma**2) * T
    terminal = S * np.exp(drift + sigma * np.sqrt(T) * z)
    disc = np.exp(-r * T)
    if right == "call":
        payoff = np.maximum(terminal - K, 0.0)
        # d/dS of max(S_T - K, 0), S_T linear in S: 1{S_T > K} S_T / S
        dpay = np.where(terminal > K, terminal / S, 0.0)
    else:
        payoff = np.maximum(K - terminal, 0.0)
        dpay = np.where(terminal < K, -terminal / S, 0.0)
    value = disc * payoff
    dvalue = disc * dpay

    pairs = 0.5 * (value[:half] + value[half:])
    dpairs = 0.5 * (dvalue[:half] + dvalue[half:])
    return MCPrice(
        price=float(pairs.mean()),
        std_error=float(pairs.std(ddof=1) / np.sqrt(half)),
        delta=float(dpairs.mean()),
        delta_std_error=float(dpairs.std(ddof=1) / np.sqrt(half)),
        paths=paths,
    )
