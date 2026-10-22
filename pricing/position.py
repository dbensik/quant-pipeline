"""
pricing/position.py

A multi-leg option position before expiry (decision 55,
research/option-greeks-2026-10-20.md): its value, profit and five Greeks on a
grid of underlying prices, each leg priced at its own market volatility.
Every price and first-order Greek comes from pricing.black_scholes; this
module adds only what the smile contributes.

CARRY
    Each expiry's forward F0 is the surface's parity forward (the projected
    discrete dividends are already in it). A leg is priced by Black-Scholes-
    Merton with the constant carry that reproduces it at the capture's spot
    S0, q = r - ln(F0/S0)/T0, as the Options page's pricer prefill does. At
    T_rem years left the model forward is S (F0/S0)^(T_rem/T0): F0 at the
    capture, S itself at expiry, so the curve lands on the payoff at expiry.
    A projected dividend stays spread through the carry after its ex-date
    has passed on the time slider.

TIME
    `days` calendar days after the capture: T_rem = T0 - days/365, and
    exactly 0 from the expiry date on. T0 runs from the capture (~15:45 New
    York) to the 16:00 close, so whole days never reach 0 by subtraction.

VOLATILITY
    From the expiry's smile, ln(K/F0) -> our implied vol, linearly
    interpolated and flat past the last quote:
        sticky strike     sigma = smile(ln(K/F0)), fixed as spot moves
        sticky moneyness  sigma = smile(ln(K/F)), F the moved forward at T_rem
    The Greeks are derivatives of the curve drawn. Under sticky moneyness
    sigma moves with the spot, with time and with the rate (all through F),
    and those terms are added; on a linear smile d2sigma/dk2 is 0 between
    quotes. Vega is one parallel shift of every leg's volatility. Rho holds
    each leg's carry q fixed, so the forward moves with the rate.

UNITS, for the whole position in dollars
    delta per $1 of the underlying, gamma per $1 squared, vega per vol point
    (0.01), theta per calendar day passing, rho per percentage point (0.01).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Literal, Optional, Sequence

import numpy as np
from scipy.stats import norm

from pricing import black_scholes as bs

LegKind = Literal["call", "put", "stock"]
VolRule = Literal["sticky_strike", "sticky_moneyness"]
VolSource = Literal["interpolated", "extrapolated"]

#: Shares a unit: 100 a contract, 1 a share (frontend payoff.ts MULTIPLIER).
MULTIPLIER: Dict[str, float] = {"call": 100.0, "put": 100.0, "stock": 1.0}
DAYS_PER_YEAR = 365.0
GREEKS = ("delta", "gamma", "vega", "theta", "rho")


@dataclass(frozen=True)
class Leg:
    kind: LegKind
    side: int  # +1 bought, -1 sold
    quantity: float
    premium: float  # per share; the stock leg's entry price
    strike: Optional[float] = None
    expiry: Optional[date] = None

    @property
    def units(self) -> float:
        return self.side * self.quantity * MULTIPLIER[self.kind]


class Smile:
    """One expiry's implied vol against ln(K/F0): linear, flat past the ends."""

    def __init__(self, log_moneyness: Sequence[float], iv: Sequence[float]) -> None:
        k, v = np.asarray(log_moneyness, dtype=float), np.asarray(iv, dtype=float)
        ok = np.isfinite(k) & np.isfinite(v) & (v > 0)
        k, v = k[ok], v[ok]
        order = np.argsort(k, kind="stable")
        k, v = k[order], v[order]
        keep = np.concatenate([[True], np.diff(k) > 0])  # one vol per moneyness
        self.k, self.iv = k[keep], v[keep]
        if self.k.size < 2:
            raise ValueError("a smile needs at least two quoted points")

    def at(self, k) -> tuple:
        """(sigma, dsigma/dk, inside) at log-moneyness k; slope 0 outside the quotes."""
        k = np.asarray(k, dtype=float)
        sigma = np.interp(k, self.k, self.iv)
        i = np.clip(np.searchsorted(self.k, k, side="right") - 1, 0, self.k.size - 2)
        slope = (self.iv[i + 1] - self.iv[i]) / (self.k[i + 1] - self.k[i])
        inside = (k >= self.k[0]) & (k <= self.k[-1])
        return sigma, np.where(inside, slope, 0.0), inside


@dataclass(frozen=True)
class Expiry:
    expiry: date
    T0: float  # years from the capture to the 16:00 close on `expiry`
    forward: float  # the parity forward F0
    smile: Smile


@dataclass(frozen=True)
class LegQuote:
    """One leg as priced at the reference spot S0 on the curve's date."""

    T_remaining: float
    forward: Optional[float]
    carry: Optional[float]  # q
    iv: Optional[float]
    vol_source: Optional[VolSource]


def carry(e: Expiry, spot0: float, r: float) -> float:
    """The q that makes S0 e^{(r-q)T0} the parity forward."""
    return r - np.log(e.forward / spot0) / e.T0 if e.T0 > 0 else r


def time_left(e: Expiry, capture_day: date, as_of: date, day_shift: float = 0.0) -> float:
    if as_of >= e.expiry:
        return 0.0
    return max(e.T0 - ((as_of - capture_day).days + day_shift) / DAYS_PER_YEAR, 0.0)


def _vanna_volga(S, K, T, r, q, sigma):
    """d2V/dSdsigma and d2V/dsigma2 (same for calls and puts); 0 when sigma*sqrt(T) is 0."""
    S, K, T, sigma = np.broadcast_arrays(*(np.asarray(x, dtype=float) for x in (S, K, T, sigma)))
    vol = sigma * np.sqrt(T)
    live = vol > 0
    safe = np.where(live, vol, 1.0)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / safe
    d2 = d1 - safe
    pdf = norm.pdf(np.where(live, d1, 0.0))
    sig = np.where(live, sigma, 1.0)
    vega = S * np.exp(-q * T) * pdf * np.sqrt(T)
    vanna = np.where(live, -np.exp(-q * T) * pdf * d2 / sig, 0.0)
    volga = np.where(live, vega * d1 * d2 / sig, 0.0)
    return vanna, volga


def _option_leg(leg, e, spots, spot0, r_carry, r, capture_day, as_of, rule, vol_shift, day_shift):
    K = float(leg.strike)
    q = carry(e, spot0, r_carry)
    T = time_left(e, capture_day, as_of, day_shift)
    if rule == "sticky_strike":
        k = np.full_like(spots, np.log(K / e.forward))
    else:
        k = np.log(K / spots) - (r - q) * T
    sigma, slope, _ = e.smile.at(k)
    sigma = sigma + vol_shift
    value = np.asarray(bs.price(spots, K, T, r, q, sigma, leg.kind), dtype=float)
    g = bs.greeks(spots, K, T, r, q, sigma, leg.kind)
    delta, gamma, vega, theta, rho = (
        np.asarray(x, dtype=float) for x in (g.delta, g.gamma, g.vega, g.theta, g.rho)
    )
    if rule == "sticky_moneyness" and T > 0:
        # sigma(k), k = ln K - ln S - (r - q) T_rem
        vanna, volga = _vanna_volga(spots, K, T, r, q, sigma)
        s_S, s_SS = -slope / spots, slope / spots**2
        delta = delta + vega * s_S
        gamma = gamma + 2 * vanna * s_S + volga * s_S**2 + vega * s_SS
        theta = theta + vega * slope * (r - q)  # dk/dt = r - q as T_rem falls
        rho = rho + vega * slope * (-T)
    return value, (delta, gamma, vega, theta, rho)


def position_curve(
    legs: Sequence[Leg],
    expiries: Dict[date, Expiry],
    spots: Sequence[float],
    spot0: float,
    rate: float,
    capture_day: date,
    as_of: date,
    rule: VolRule = "sticky_strike",
    vol_shift: float = 0.0,
    rate_shift: float = 0.0,
    day_shift: float = 0.0,
) -> Dict[str, np.ndarray]:
    """
    Value, profit and Greeks of the position at each spot, `as_of`.

    `rate` is the capture's continuous rate; each leg's carry is fixed from
    it. The shifts move one input for a finite-difference check: every leg's
    vol (decimal), the rate the legs are priced at, and fractional days.
    """
    S = np.asarray(spots, dtype=float)
    r = rate + rate_shift
    out = {f: np.zeros_like(S) for f in ("value", "profit") + GREEKS}
    for leg in legs:
        if leg.kind == "stock":
            value, greeks = S, (np.ones_like(S),) + (np.zeros_like(S),) * 4
        else:
            value, greeks = _option_leg(
                leg,
                expiries[leg.expiry],
                S,
                spot0,
                rate,
                r,
                capture_day,
                as_of,
                rule,
                vol_shift,
                day_shift,
            )
        out["value"] += leg.units * value
        out["profit"] += leg.units * (value - leg.premium)
        for name, x in zip(GREEKS, greeks, strict=True):
            out[name] += leg.units * x
    out["vega"] /= 100.0
    out["theta"] /= DAYS_PER_YEAR
    out["rho"] /= 100.0
    return out


def leg_quotes(
    legs: Sequence[Leg],
    expiries: Dict[date, Expiry],
    spot0: float,
    rate: float,
    capture_day: date,
    as_of: date,
    rule: VolRule = "sticky_strike",
) -> List[LegQuote]:
    """Each leg's time left, forward, carry and vol at S0 on `as_of`."""
    quotes = []
    for leg in legs:
        if leg.kind == "stock":
            quotes.append(LegQuote(0.0, None, None, None, None))
            continue
        e = expiries[leg.expiry]
        q = carry(e, spot0, rate)
        T = time_left(e, capture_day, as_of)
        F = spot0 * np.exp((rate - q) * T)
        k = np.log(leg.strike / (e.forward if rule == "sticky_strike" else F))
        sigma, _, inside = e.smile.at(k)
        quotes.append(
            LegQuote(
                T_remaining=T,
                forward=float(F),
                carry=float(q),
                iv=float(sigma),
                vol_source="interpolated" if bool(inside) else "extrapolated",
            )
        )
    return quotes
