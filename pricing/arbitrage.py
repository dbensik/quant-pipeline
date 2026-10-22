"""
pricing/arbitrage.py

Butterfly and calendar no-arbitrage checks for SVI fits, each beside the same
check on the quotes (decision 56, research/svi-surface-2026-10-20.md).

A flag that only described the fit would mislead, so every check answers
twice: does the FIT show it, and do the QUOTES show it. The quotes' side is
always a trade at bid and ask, never a comparison of mids.

BUTTERFLY, one expiry
    fit     g(k) < 0 (pricing/svi.py) on the quoted range plus
            SVI_BUTTERFLY_MARGIN of log-moneyness either side.
    quotes  a butterfly on three adjacent strikes of one right can be
            BOUGHT for less than nothing: wings at the ask, body at the bid. Tested on mids
            instead, 1,917 of 1,938 slices failed: a mid is not a price
            anyone can trade, and adjacent mids wobble by more than the
            curvature between them.

CALENDAR, two adjacent expiries
    fit     total variance w falls from the nearer expiry to the farther
            one somewhere on their SHARED quoted range of k = ln(K/F).
    quotes  at a strike both expiries quote on one right, the farther
            option's ASK is below the nearer one's BID: buy far, sell near,
            for a credit. A longer American option is worth at least a
            shorter one at the same strike, dividends or not, so this is
            model-free (2026-10-22, research/svi-surface-2026-10-20.md,
            "Calendar at executable prices").
    mid     the old quote side, kept beside it as `mid_crossing` and never
            in the verdict: quoted MID total variance, interpolated in k,
            falling with time. On 1,863 pairs it flagged 692; 25 survive at
            bid and ask. It is mostly the spread, not an arbitrage.

Ranges come back as [low, high] in k on the evaluation grid.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Sequence, Tuple

import numpy as np

from config.settings import SVI_BUTTERFLY_MARGIN, SVI_CHECK_POINTS
from pricing.svi import SVIParams

Interval = Tuple[float, float]
Verdict = Literal["none", "both", "fit_only", "quotes_only"]

#: Absorbs float rounding in a butterfly's cost (dollars x strike gap).
_PRICE_TOL = 1e-9
#: Absorbs float rounding between two total variances: equal is not a crossing.
_VARIANCE_TOL = 1e-12


def verdict(fit: bool, quotes: bool) -> Verdict:
    if fit and quotes:
        return "both"
    if fit:
        return "fit_only"
    return "quotes_only" if quotes else "none"


def intervals(grid: np.ndarray, mask: np.ndarray) -> List[Interval]:
    """The runs of True in `mask`, as [first, last] grid values."""
    out: List[Interval] = []
    start: Optional[int] = None
    for i, flagged in enumerate(mask):
        if flagged and start is None:
            start = i
        if not flagged and start is not None:
            out.append((float(grid[start]), float(grid[i - 1])))
            start = None
    if start is not None:
        out.append((float(grid[start]), float(grid[-1])))
    return out


@dataclass(frozen=True)
class Butterfly:
    fit: List[Interval]
    min_g: float
    quotes: List[Interval]  # [k of the lower strike, k of the upper] per bad triple
    checked: Interval

    @property
    def verdict(self) -> Verdict:
        return verdict(bool(self.fit), bool(self.quotes))


@dataclass(frozen=True)
class CalendarCredit:
    """One strike where the farther option is offered below the nearer one's bid."""

    strike: float
    is_call: bool
    near_bid: float
    far_ask: float

    @property
    def credit(self) -> float:
        return self.near_bid - self.far_ask


@dataclass(frozen=True)
class Calendar:
    shared: Optional[Interval]
    fit: List[Interval]
    quotes: List[CalendarCredit]
    mid_crossing: List[Interval]

    @property
    def verdict(self) -> Verdict:
        return verdict(bool(self.fit), bool(self.quotes))


def _sorted(k: Sequence[float], iv: Sequence[float]) -> Tuple[np.ndarray, np.ndarray]:
    k, iv = np.asarray(k, dtype=float), np.asarray(iv, dtype=float)
    ok = np.isfinite(k) & np.isfinite(iv) & (iv > 0)
    k, iv = k[ok], iv[ok]
    order = np.argsort(k, kind="stable")
    k, iv = k[order], iv[order]
    # One value per strike: a strike quoted on both sides keeps their mean.
    uniq, inverse = np.unique(k, return_inverse=True)
    if len(uniq) < len(k):
        iv = np.bincount(inverse, weights=iv) / np.bincount(inverse)
        k = uniq
    return k, iv


def quoted_butterflies(
    strike: Sequence[float],
    is_call: Sequence[bool],
    bid: Sequence[float],
    ask: Sequence[float],
    forward: float,
) -> List[Interval]:
    """
    Adjacent strike triples of ONE right whose butterfly can be BOUGHT at the
    quotes for less than nothing (wings at the ask, body at the bid), as
    [k1, k3]. Model-free: no forward, rate or dividend enters the test.

    A triple across the put/call boundary is not tested. Mapping a put to a
    call by parity needs the forward, and an error in it is not linear in
    the triple: on SPY 2026-10-19 the mixed triples at the money "showed"
    credits of up to $1.53, largest on 2026-12-18, the expiry whose forward
    sits $0.85 off raw parity (decision 55 B). Convexity in strike holds for
    American options too, so the archive's quotes are tested as served.
    """
    K = np.asarray(strike, dtype=float)
    calls = np.asarray(is_call, dtype=bool)
    b, a = np.asarray(bid, dtype=float), np.asarray(ask, dtype=float)
    out: List[Interval] = []
    for right in (False, True):
        keep = (calls == right) & np.isfinite(K) & np.isfinite(b) & np.isfinite(a)
        k_, b_, a_ = K[keep], b[keep], a[keep]
        k_, first = np.unique(k_, return_index=True)  # one quote per strike
        b_, a_ = b_[first], a_[first]
        if len(k_) < 3:
            continue
        lo, mid, hi = slice(None, -2), slice(1, -1), slice(2, None)
        cost = (
            (k_[hi] - k_[mid]) * a_[lo] + (k_[mid] - k_[lo]) * a_[hi] - (k_[hi] - k_[lo]) * b_[mid]
        )
        for i in np.nonzero(cost < -_PRICE_TOL)[0]:
            out.append((float(np.log(k_[i] / forward)), float(np.log(k_[i + 2] / forward))))
    return sorted(out)


def butterfly(
    params: SVIParams,
    k_range: Interval,
    quotes: List[Interval],
    margin: float = SVI_BUTTERFLY_MARGIN,
    points: int = SVI_CHECK_POINTS,
) -> Butterfly:
    """g(k) < 0 on the quoted range `k_range` widened by `margin`, beside `quotes`."""
    lo, hi = k_range[0] - margin, k_range[1] + margin
    grid = np.linspace(lo, hi, points)
    g = params.g(grid)
    return Butterfly(
        fit=intervals(grid, g < 0), min_g=float(np.min(g)), quotes=quotes, checked=(lo, hi)
    )


def quoted_calendars(near: Sequence[dict], far: Sequence[dict]) -> List[CalendarCredit]:
    """
    Strikes quoted on the same right in both expiries where the farther ask
    is below the nearer bid. Each point needs strike, is_call, bid and ask.
    """
    by_key = {}
    for p in near:
        if np.isfinite(p["bid"]):
            by_key.setdefault((float(p["strike"]), bool(p["is_call"])), p)
    out: List[CalendarCredit] = []
    seen = set()
    for p in far:
        key = (float(p["strike"]), bool(p["is_call"]))
        q = by_key.get(key)
        if q is None or key in seen or not np.isfinite(p["ask"]):
            continue
        seen.add(key)
        if p["ask"] < q["bid"] - _PRICE_TOL:
            out.append(CalendarCredit(key[0], key[1], float(q["bid"]), float(p["ask"])))
    return sorted(out, key=lambda c: (c.strike, c.is_call))


def calendar(
    near: SVIParams,
    near_k: Sequence[float],
    near_iv: Sequence[float],
    near_T: float,
    far: SVIParams,
    far_k: Sequence[float],
    far_iv: Sequence[float],
    far_T: float,
    quotes: List[CalendarCredit],
    points: int = SVI_CHECK_POINTS,
) -> Calendar:
    """
    The fit's total variance falling from `near` to `far` on their shared
    quoted range, beside `quotes` (quoted_calendars) and the mid crossing.
    """
    nk, niv = _sorted(near_k, near_iv)
    fk, fiv = _sorted(far_k, far_iv)
    lo, hi = max(nk[0], fk[0]), min(nk[-1], fk[-1])
    if not lo < hi:
        return Calendar(shared=None, fit=[], quotes=quotes, mid_crossing=[])
    grid = np.linspace(lo, hi, points)
    fit_mask = far.w(grid) < near.w(grid) - _VARIANCE_TOL
    quoted_near = np.interp(grid, nk, niv * niv * near_T)
    quoted_far = np.interp(grid, fk, fiv * fiv * far_T)
    return Calendar(
        shared=(float(lo), float(hi)),
        fit=intervals(grid, fit_mask),
        quotes=quotes,
        mid_crossing=intervals(grid, quoted_far < quoted_near - _VARIANCE_TOL),
    )
