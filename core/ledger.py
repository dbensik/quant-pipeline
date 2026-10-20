"""
core/ledger.py

Short-position accounting, shared by PortfolioBacktester and (decision 49)
paper portfolios (decision 48, research/short-engine-2026-10-19.md). Pure
functions: no database, no I/O, so each rule can be checked by hand.

A short sale's proceeds are COLLATERAL, not spendable cash. With signed
positions, `cash` holds them and the short's market value is a liability:

    equity     = cash + sum(quantity x price)            (short quantity < 0)
    free cash  = cash - short value                      what buys are paid from
    gross      = long value + short value                both as magnitudes

Free cash earns the risk-free rate; collateral earns nothing (decision 47:
paying interest on it was worth 0.01-0.05 Sharpe). Buys are paid from free
cash only: there is no margin loan, so leverage exists only through shorts and
is bounded by `max_gross`.

The three terms are facts about a broker the repo cannot verify, so
ShortTerms has no defaults (like `cash_accrual` and lot sizes). Reference
points, not defaults: Reg T initial margin is 50% (max_gross 2); FINRA's
minimum maintenance on a short is 30% of its value for stocks at $5 or more,
and brokers often ask more; borrow on easy-to-borrow ETFs is usually well
under 1% a year. Every name is assumed easy to borrow at one flat rate: there
is no source for locates or hard-to-borrow rates.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

#: Calendar days in the year borrow is quoted over.
BORROW_DAY_COUNT = 365.0


@dataclass(frozen=True)
class ShortTerms:
    """
    borrow_rate   annual fee on a short's value (0.01 = 1%)
    max_gross     cap on gross exposure as a multiple of equity, at entry
    maintenance   equity must stay at least this fraction of gross exposure;
                  below it, positions are cut pro rata at the next open
    """

    borrow_rate: float
    max_gross: float
    maintenance: float

    def __post_init__(self):
        if self.borrow_rate < 0:
            raise ValueError(f"borrow_rate must be >= 0, got {self.borrow_rate}")
        if self.max_gross <= 0:
            raise ValueError(f"max_gross must be > 0, got {self.max_gross}")
        if not 0 < self.maintenance < 1:
            raise ValueError(f"maintenance must be between 0 and 1, got {self.maintenance}")
        if self.maintenance * self.max_gross >= 1:
            # Entering at max_gross would already breach maintenance.
            raise ValueError(
                f"maintenance {self.maintenance} x max_gross {self.max_gross} must be below 1: "
                "a position opened at the cap would be cut at once"
            )


def long_value(positions: Mapping[str, float], prices: Mapping[str, float]) -> float:
    return sum(q * prices[t] for t, q in positions.items() if q > 0)


def short_value(positions: Mapping[str, float], prices: Mapping[str, float]) -> float:
    """The shorts' market value, as a positive number."""
    return sum(-q * prices[t] for t, q in positions.items() if q < 0)


def gross_exposure(positions: Mapping[str, float], prices: Mapping[str, float]) -> float:
    return long_value(positions, prices) + short_value(positions, prices)


def equity(cash: float, positions: Mapping[str, float], prices: Mapping[str, float]) -> float:
    return cash + sum(q * prices[t] for t, q in positions.items() if q != 0)


def free_cash(cash: float, positions: Mapping[str, float], prices: Mapping[str, float]) -> float:
    """
    Cash less the collateral the shorts tie up. Negative only when shorts have
    lost more than the free cash there was, which is the maintenance region:
    interest then charges at the risk-free rate, an understatement of a real
    debit balance.
    """
    return cash - short_value(positions, prices)


def borrow_charge(
    positions: Mapping[str, float], prev_closes: Mapping[str, float], days: float, rate: float
) -> float:
    """Fee for holding the shorts over `days` calendar days, on their value at the previous close."""
    return short_value(positions, prev_closes) * rate * days / BORROW_DAY_COUNT


def short_room(
    equity_value: float, gross: float, max_gross: float, price: float, lot_size: float
) -> float:
    """Most units that can be ADDED to gross exposure at `price` without passing max_gross."""
    room = max_gross * equity_value - gross
    if room <= 0 or price <= 0:
        return 0.0
    return math.floor(room / price / lot_size) * lot_size


def margin_cut(equity_value: float, gross: float, maintenance: float) -> float:
    """
    The fraction of every position to close so that equity >= maintenance x
    gross again; 0 when the account is within maintenance. Pro rata, so a
    hedged book stays hedged. Costs of the closing fills are not in it: the
    next close re-checks.
    """
    if gross <= 0 or equity_value >= maintenance * gross:
        return 0.0
    if equity_value <= 0:
        return 1.0
    return 1.0 - equity_value / (maintenance * gross)
