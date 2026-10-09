"""
pricing/dividends.py

Discrete dividends for American option pricing: projecting the next ex-dates
from history, and the escrowed-dividend quantities the tree needs.

WHY DISCRETE, NOT A CONTINUOUS YIELD (a measured deviation from decision 4)
    The approved plan priced dividends as a continuous trailing-12-month
    yield. On SPY 2026-10-07 that misprices the early-exercise premium of a
    near-the-money put by $0.11-0.32, 4.5 to 16 times the half-spread, on
    expiries before and around the 2026-12-18 ex-date: before an ex-date
    there is no dividend at all, and a smeared yield invents one.

THE ESCROWED MODEL
    Spot is split into a risky part and the present value of the dividends
    paid before expiry. The tree runs on the risky part with no yield; at a
    node at time t the exercise value adds back the PV, as of t, of the
    dividends still to come. With a parity forward F per expiry, the risky
    part today is simply F * exp(-r T): no recorded spot is needed.

PROJECTION
    Next ex-date = last ex-date + median gap of the history; amount = last
    amount. A projected date can miss by days, so callers flag expiries
    within OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS of one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class Dividend:
    ex_date: date
    amount: float
    projected: bool


def project_dividends(
    history: Sequence[Tuple[date, float]],
    after: date,
    until: date,
    min_history: int = 3,
) -> List[Dividend]:
    """
    Projected dividends with ex-date in (after, until]. Needs at least
    `min_history` past dividends to infer a cadence; returns [] otherwise
    (a non-payer, or too little history to say).
    """
    past = sorted((d, a) for d, a in history if d <= after and a > 0)
    if len(past) < min_history:
        return []
    gaps = np.diff([d.toordinal() for d, _ in past])
    gap = int(round(float(np.median(gaps))))
    if gap < 20:
        raise ValueError(f"median dividend gap of {gap} days is not a periodic schedule")
    last_date, amount = past[-1]
    out: List[Dividend] = []
    nxt = last_date + timedelta(days=gap)
    while nxt <= until:
        if nxt > after:
            out.append(Dividend(nxt, amount, projected=True))
        nxt += timedelta(days=gap)
    return out


def year_fraction(start: date, end: date) -> float:
    return (end - start).days / 365.0


def dividends_before(
    dividends: Sequence[Dividend], valuation: date, expiry: date
) -> List[Tuple[float, float]]:
    """(time in years from valuation, amount) for ex-dates in (valuation, expiry]."""
    return [
        (year_fraction(valuation, d.ex_date), d.amount)
        for d in dividends
        if valuation < d.ex_date <= expiry
    ]


def pv_dividends(divs: Sequence[Tuple[float, float]], r: float, at: float = 0.0, until: float = np.inf) -> float:
    """PV as of time `at` of dividends paid in (at, until]."""
    return float(sum(a * np.exp(-r * (t - at)) for t, a in divs if at < t <= until))


def near_ex_date(dividends: Sequence[Dividend], expiry: date, days: int) -> bool:
    return any(d.projected and abs((d.ex_date - expiry).days) <= days for d in dividends)
