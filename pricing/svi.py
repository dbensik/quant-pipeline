"""
pricing/svi.py

Raw SVI fitted to one expiry's smile (decision 56,
research/svi-surface-2026-10-20.md).

    w(k) = a + b (rho (k - m) + sqrt((k - m)^2 + sigma^2))

w is TOTAL implied variance, iv^2 T, and k = ln(K/F) on the expiry's own
parity forward. Five parameters:

    a      level: the variance the smile is lifted by everywhere
    b      wings: how fast variance rises away from the bottom
    rho    tilt: negative makes the left (low strike, put) wing the steeper
    m      shift: where the bottom sits, in log-moneyness
    sigma  rounding: how wide and smooth the bottom is; near 0 is a sharp V

FIT
    Least squares on total variance against our implied volatilities (never
    Yahoo's), from SVI_STARTS starting points; the lowest error wins. The
    bottom of the smile, a + b sigma sqrt(1 - rho^2), is a parameter bounded
    at 0, so total variance is never negative. Nothing else is constrained:
    arbitrage is REPORTED (pricing/arbitrage.py), not fitted away. Measured,
    one start was never worse than nine; three keep a margin.

DERIVATIVES are analytic: w', w'' and Gatheral's density function

    g(k) = (1 - k w'/(2w))^2 - (w'^2/4)(1/w + 1/4) + w''/2

which is negative exactly where the risk-neutral density is, a butterfly
arbitrage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import numpy as np
from scipy.optimize import least_squares

from config.settings import SVI_STARTS

#: Plain-English meaning of each parameter, served with every fit.
PARAMETER_MEANINGS = {
    "a": "Level: total variance added everywhere along the smile.",
    "b": "Wings: how fast variance rises away from the smile's bottom.",
    "rho": "Tilt, -1 to 1: negative makes the low-strike (put) wing steeper.",
    "m": "Shift: where the smile's bottom sits, in ln(strike / forward).",
    "sigma": "Rounding: how wide and smooth the bottom is; near 0 is a sharp V.",
}

_RHO_BOUND = 0.999
_SIGMA_BOUNDS = (1e-4, 5.0)


@dataclass(frozen=True)
class SVIParams:
    a: float
    b: float
    rho: float
    m: float
    sigma: float

    def _x(self, k):
        x = np.asarray(k, dtype=float) - self.m
        return x, np.sqrt(x * x + self.sigma * self.sigma)

    def w(self, k):
        x, r = self._x(k)
        return self.a + self.b * (self.rho * x + r)

    def w1(self, k):
        x, r = self._x(k)
        return self.b * (self.rho + x / r)

    def w2(self, k):
        x, r = self._x(k)
        return self.b * self.sigma**2 / r**3

    def g(self, k):
        k = np.asarray(k, dtype=float)
        w, w1, w2 = self.w(k), self.w1(k), self.w2(k)
        return (1 - k * w1 / (2 * w)) ** 2 - (w1 * w1 / 4) * (1 / w + 0.25) + w2 / 2

    def iv(self, k, T: float):
        return np.sqrt(np.maximum(self.w(k), 0.0) / T)

    @property
    def min_variance(self) -> float:
        return self.a + self.b * self.sigma * np.sqrt(1 - self.rho**2)

    @property
    def wing_slopes(self) -> Tuple[float, float]:
        """dw/dk far left and far right: b(rho - 1) and b(rho + 1)."""
        return self.b * (self.rho - 1), self.b * (self.rho + 1)


@dataclass(frozen=True)
class SVIFit:
    params: SVIParams
    rmse_iv: float  # root-mean-square error in implied vol (0.01 = 1 vol point)
    points: int


def _params(x: np.ndarray) -> SVIParams:
    vmin, b, rho, m, sigma = x
    return SVIParams(vmin - b * sigma * np.sqrt(1 - rho * rho), b, rho, m, sigma)


def _slope(k: np.ndarray, w: np.ndarray) -> float:
    return float(np.polyfit(k, w, 1)[0])


def _starts(k: np.ndarray, w: np.ndarray, n: int):
    third = max(2, len(k) // 3)
    left, right = _slope(k[:third], w[:third]), _slope(k[-third:], w[-third:])
    b = max((right - left) / 2, 1e-3)
    rho = float(np.clip((right + left) / (2 * b), -0.9, 0.9))
    bottom = float(k[np.argmin(w)])
    vmin = max(float(w.min()) * 0.9, 0.0)
    span = max(float(k[-1] - k[0]), 1e-3)
    candidates = [(bottom, 0.1 * span), (0.0, 0.05 * span), (bottom, 0.3 * span)]
    for m, sigma in candidates[:n]:
        yield np.array([vmin, b, rho, m, float(np.clip(sigma, *_SIGMA_BOUNDS))])


def fit(
    log_moneyness: Sequence[float],
    iv: Sequence[float],
    T: float,
    starts: Optional[int] = None,
) -> SVIFit:
    """Raw SVI through one expiry's (k, iv) points; raises ValueError on fewer than 5."""
    k = np.asarray(log_moneyness, dtype=float)
    v = np.asarray(iv, dtype=float)
    ok = np.isfinite(k) & np.isfinite(v) & (v > 0)
    k, v = k[ok], v[ok]
    if len(k) < 5:
        raise ValueError(f"SVI needs 5 points, got {len(k)}")
    if not T > 0:
        raise ValueError("T must be positive")
    order = np.argsort(k)
    k, v = k[order], v[order]
    w = v * v * T

    span = float(k[-1] - k[0])
    lower = [0.0, 0.0, -_RHO_BOUND, k[0] - span - 1, _SIGMA_BOUNDS[0]]
    upper = [np.inf, np.inf, _RHO_BOUND, k[-1] + span + 1, _SIGMA_BOUNDS[1]]

    def residuals(x):
        return _params(x).w(k) - w

    best = None
    for x0 in _starts(k, w, starts or SVI_STARTS):
        x0 = np.clip(x0, lower, upper)
        result = least_squares(
            residuals, x0, bounds=(lower, upper), xtol=1e-15, ftol=1e-15, gtol=1e-15
        )
        if best is None or result.cost < best.cost:
            best = result

    params = _params(best.x)
    rmse = float(np.sqrt(np.mean((params.iv(k, T) - v) ** 2)))
    return SVIFit(params, rmse, len(k))
