"""
core/svi_report.py

One surface's SVI fits and arbitrage checks (decision 56,
research/svi-surface-2026-10-20.md), built from the cached surface payload
(core/options_surface.py) with no re-pricing: the fits read our implied
vols, the quoted butterfly check reads the bid and ask the smile kept.

Kept in process by the surface's key, so a re-priced surface is re-fitted
and nothing about SVI enters the surface's own cache fingerprint.

WITHIN THE SPREAD
    A quote's half-spread in volatility is (ask - bid) / 2 over its vega,
    D F sqrt(T) n(d1) at our vol. A fit is within the spread at a quote when
    it misses our vol by no more than that. Quotes with no vega are left out.

READING THE PARAMETERS
    They are read one by one only when the smile's bottom m lies among the
    quotes (`bottom_quoted`). An equity smile often falls across its whole
    quoted range; the fit then puts m past the last quote, and rho, b and m
    trade off against each other to draw the same curve: 57% of the 1,938
    fits measured. The curve is still the fit; its parameters are not a
    reading of the market. `wings_within_lee` says whether the fitted wings
    rise no faster than Lee's bound allows beyond the quotes.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional

import numpy as np
from scipy.stats import norm

from config import settings
from pricing import arbitrage as A
from pricing import svi

DAYS_PER_YEAR = 365.0
#: Roger Lee's moment formula: far from the money, total variance can rise
#: no faster than 2 per unit of log-moneyness on either side.
LEE_MAX_SLOPE = 2.0


def _within_spread(
    points: List[Dict[str, Any]], fitted_iv: np.ndarray, forward: float, T: float, discount: float
) -> Optional[float]:
    iv = np.array([p["iv"] for p in points], dtype=float)
    k = np.array([p["log_moneyness"] for p in points], dtype=float)
    spread = np.array([p["ask"] - p["bid"] for p in points], dtype=float)
    s = iv * np.sqrt(T)
    vega = discount * forward * np.sqrt(T) * norm.pdf(-k / s + s / 2)
    ok = np.isfinite(vega) & (vega > 0) & np.isfinite(spread)
    if not ok.any():
        return None
    half = spread[ok] / 2 / vega[ok]
    return float(np.mean(np.abs(fitted_iv[ok] - iv[ok]) <= half))


def _interval(pair) -> Dict[str, float]:
    return {"low": pair[0], "high": pair[1]}


def build_report(surface: Dict[str, Any]) -> Dict[str, Any]:
    started = time.perf_counter()
    rate = float(surface["rate"]["continuous"])
    fits: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []
    fitted: List[tuple] = []  # (expiry, params, k, iv, T) in T order, for the calendar check

    for e in sorted(surface["expiries"], key=lambda e: e["T"]):
        expiry, T = e["expiry"], float(e["T"])
        points = surface["smiles"].get(expiry) or []
        if T * DAYS_PER_YEAR < settings.SVI_MIN_DAYS:
            skipped.append(
                {"expiry": expiry, "reason": f"under {settings.SVI_MIN_DAYS} days to run"}
            )
            continue
        if len(points) < settings.SVI_MIN_POINTS:
            skipped.append(
                {
                    "expiry": expiry,
                    "reason": f"{len(points)} smile points, fewer than {settings.SVI_MIN_POINTS}",
                }
            )
            continue

        k = np.array([p["log_moneyness"] for p in points], dtype=float)
        iv = np.array([p["iv"] for p in points], dtype=float)
        fit = svi.fit(k, iv, T)
        params = fit.params
        forward, discount = float(e["forward"]), float(np.exp(-rate * T))
        k_range = (float(k.min()), float(k.max()))
        quoted = A.quoted_butterflies(
            [p["strike"] for p in points],
            [p["is_call"] for p in points],
            [p["bid"] for p in points],
            [p["ask"] for p in points],
            forward,
        )
        fly = A.butterfly(params, k_range, quoted)
        curve_k = np.linspace(*fly.checked, settings.SVI_CURVE_POINTS)
        left, right = params.wing_slopes

        fits.append(
            {
                "expiry": expiry,
                "T": T,
                "days": round(T * DAYS_PER_YEAR, 2),
                "points": fit.points,
                "params": {
                    "a": params.a,
                    "b": params.b,
                    "rho": params.rho,
                    "m": params.m,
                    "sigma": params.sigma,
                },
                "rmse_vol_pts": fit.rmse_iv * 100,
                "within_spread_share": _within_spread(
                    points, params.iv(k, T), forward, T, discount
                ),
                "atm_iv": float(params.iv(0.0, T)),
                "min_iv": float(np.sqrt(max(params.min_variance, 0.0) / T)),
                "left_wing_slope": left,
                "right_wing_slope": right,
                "bottom_quoted": bool(k_range[0] <= params.m <= k_range[1]),
                "wings_within_lee": bool(max(abs(left), abs(right)) <= LEE_MAX_SLOPE),
                "k_range": _interval(k_range),
                "curve": [
                    {"log_moneyness": float(x), "iv": float(v)}
                    for x, v in zip(curve_k, params.iv(curve_k, T), strict=True)
                ],
                "butterfly": {
                    "verdict": fly.verdict,
                    "checked": _interval(fly.checked),
                    "fit": [_interval(i) for i in fly.fit],
                    "min_g": fly.min_g,
                    "quotes": [_interval(i) for i in fly.quotes],
                },
            }
        )
        fitted.append((expiry, params, k, iv, T))

    calendar = []
    for (near, p1, k1, iv1, t1), (far, p2, k2, iv2, t2) in zip(fitted, fitted[1:], strict=False):
        c = A.calendar(p1, k1, iv1, t1, p2, k2, iv2, t2)
        calendar.append(
            {
                "near": near,
                "far": far,
                "verdict": c.verdict,
                "shared": _interval(c.shared) if c.shared else None,
                "fit": [_interval(i) for i in c.fit],
                "quotes": [_interval(i) for i in c.quotes],
            }
        )

    def tally(rows):
        out = {"none": 0, "both": 0, "fit_only": 0, "quotes_only": 0}
        for r in rows:
            out[r["verdict"]] += 1
        return out

    return {
        "ticker": surface["ticker"],
        "day": surface["day"],
        "captured_at": surface["captured_at"],
        "parameters": dict(svi.PARAMETER_MEANINGS),
        "min_points": settings.SVI_MIN_POINTS,
        "min_days": settings.SVI_MIN_DAYS,
        "butterfly_margin": settings.SVI_BUTTERFLY_MARGIN,
        "fits": fits,
        "skipped": skipped,
        "calendar": calendar,
        "butterfly_summary": tally(f["butterfly"] for f in fits),
        "calendar_summary": tally(calendar),
        "fit_seconds": round(time.perf_counter() - started, 2),
    }


class ReportCache:
    """The last `size` reports, by surface key. In process; rebuilt in seconds."""

    def __init__(self, size: int = 32) -> None:
        self.size = size
        self._items: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._lock = threading.Lock()

    def get_or_build(self, surface: Dict[str, Any]) -> Dict[str, Any]:
        key = surface["key"]
        with self._lock:
            if key in self._items:
                self._items.move_to_end(key)
                return self._items[key]
        report = build_report(surface)
        with self._lock:
            self._items[key] = report
            while len(self._items) > self.size:
                self._items.popitem(last=False)
        return report
