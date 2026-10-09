"""
modeling/dcf.py

Discounted cash flow kernels. Pure numpy/scipy, every input explicit, no I/O.
Tier 3 phase 3 of research/dcf-plan-2026-10-08.md.

CONVENTIONS
    Year-end discounting: the year-t cash flow is discounted by (1 + wacc)^t,
    t = 1..N. Growth fades LINEARLY from g1 in year 1 to the terminal rate in
    year N: g_t = g1 + (g_T - g1) (t - 1) / (N - 1). The Gordon terminal
    value at year N is FCF_N (1 + g_T) / (wacc - g_T), discounted with year N.
    So with g1 == g_T the whole DCF equals FCF_0 (1 + g) / (wacc - g) for any
    N: an identity the tests use as the known answer.

    Enterprise value -> equity value (less net debt; NEGATIVE net debt, net
    cash, adds) -> per share.

NOTHING SILENT
    wacc <= g_T has no Gordon value: NaN with a reason, so a sensitivity grid
    cell can be empty without the grid failing. The reverse DCF returns NaN
    with a reason when base FCF is not positive or no growth in the bracket
    reproduces the price. Every result carries the inputs that produced it.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
from scipy.optimize import brentq

from config import settings


@dataclass(frozen=True)
class DCFInputs:
    base_fcf: float
    growth_first: float
    growth_terminal: float
    wacc: float
    net_debt: float
    shares: float
    years: int = settings.DCF_PROJECTION_YEARS
    #: For the exit-multiple terminal value: the metric in year 0 (grown on the
    #: same path) and the multiple applied to it in year N. None skips it.
    exit_metric_base: Optional[float] = None
    exit_multiple: Optional[float] = None


@dataclass
class DCFResult:
    inputs: DCFInputs
    growth_path: List[float]
    fcf: List[float]
    pv_fcf: List[float]
    terminal_value: Optional[float]
    pv_terminal: Optional[float]
    enterprise_value: Optional[float]
    equity_value: Optional[float]
    per_share: Optional[float]
    terminal_share: Optional[float]
    exit_enterprise_value: Optional[float] = None
    exit_per_share: Optional[float] = None
    reason: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict:
        out = asdict(self)
        out["inputs"] = asdict(self.inputs)
        return out


def growth_path(g1: float, g_terminal: float, years: int) -> List[float]:
    if years < 1:
        raise ValueError("years must be >= 1")
    if years == 1:
        return [g_terminal]
    return [g1 + (g_terminal - g1) * (t - 1) / (years - 1) for t in range(1, years + 1)]


def run(x: DCFInputs) -> DCFResult:
    path = growth_path(x.growth_first, x.growth_terminal, x.years)
    fcf, level = [], x.base_fcf
    for g in path:
        level *= 1.0 + g
        fcf.append(level)
    discount = [(1.0 + x.wacc) ** -t for t in range(1, x.years + 1)]
    pv = [f * d for f, d in zip(fcf, discount)]

    exit_ev = exit_ps = None
    if x.exit_metric_base is not None and x.exit_multiple is not None:
        metric_n = x.exit_metric_base * float(np.prod([1.0 + g for g in path]))
        exit_ev = sum(pv) + x.exit_multiple * metric_n * discount[-1]
        exit_ps = (exit_ev - x.net_debt) / x.shares if x.shares > 0 else None

    if x.wacc <= x.growth_terminal:
        return DCFResult(x, path, fcf, pv, None, None, None, None, None, None, exit_ev, exit_ps,
                         reason=f"wacc {x.wacc:.4f} <= terminal growth {x.growth_terminal:.4f}: no Gordon value")
    if x.shares <= 0:
        return DCFResult(x, path, fcf, pv, None, None, None, None, None, None, exit_ev, exit_ps,
                         reason="share count is not positive")
    tv = fcf[-1] * (1.0 + x.growth_terminal) / (x.wacc - x.growth_terminal)
    pv_tv = tv * discount[-1]
    ev = sum(pv) + pv_tv
    equity = ev - x.net_debt
    result = DCFResult(
        inputs=x, growth_path=path, fcf=fcf, pv_fcf=pv, terminal_value=tv, pv_terminal=pv_tv,
        enterprise_value=ev, equity_value=equity, per_share=equity / x.shares,
        terminal_share=pv_tv / ev if ev else None, exit_enterprise_value=exit_ev, exit_per_share=exit_ps,
    )
    if result.terminal_share is not None and result.terminal_share > settings.DCF_TERMINAL_SHARE_WARN:
        result.warnings.append(
            f"terminal value is {result.terminal_share:.0%} of enterprise value: "
            "the valuation is mostly the terminal assumption"
        )
    if x.base_fcf <= 0:
        result.warnings.append("base free cash flow is not positive; growing it says little")
    return result


def gordon_implied_multiple(result: DCFResult) -> Optional[float]:
    """The exit multiple on the year-N metric that reproduces the Gordon value."""
    x = result.inputs
    if result.terminal_value is None or x.exit_metric_base in (None, 0):
        return None
    metric_n = x.exit_metric_base * float(np.prod([1.0 + g for g in result.growth_path]))
    return result.terminal_value / metric_n


def sensitivity(
    x: DCFInputs,
    wacc_step: float = settings.DCF_GRID_WACC_STEP,
    growth_step: float = settings.DCF_GRID_GROWTH_STEP,
    points: int = settings.DCF_GRID_POINTS,
) -> Dict[str, object]:
    """Per-share value over a WACC x terminal-growth grid; NaN cells have no Gordon value."""
    half = points // 2
    waccs = [x.wacc + (i - half) * wacc_step for i in range(points)]
    growths = [x.growth_terminal + (j - half) * growth_step for j in range(points)]
    grid = []
    for w in waccs:
        row = []
        for g in growths:
            r = run(DCFInputs(**{**asdict(x), "wacc": w, "growth_terminal": g}))
            row.append(r.per_share if r.per_share is not None else math.nan)
        grid.append(row)
    return {"wacc": waccs, "growth_terminal": growths, "per_share": grid}


@dataclass(frozen=True)
class Implied:
    growth_first: float
    reason: Optional[str]


def reverse_dcf(
    x: DCFInputs,
    price: float,
    bounds: Tuple[float, float] = settings.DCF_REVERSE_GROWTH_BOUNDS,
) -> Implied:
    """The first-year growth that makes per-share value equal `price`."""
    if x.base_fcf <= 0:
        return Implied(math.nan, "base free cash flow is not positive: no growth rate rescues it")
    if x.wacc <= x.growth_terminal:
        return Implied(math.nan, "wacc <= terminal growth")

    def gap(g: float) -> float:
        return run(DCFInputs(**{**asdict(x), "growth_first": g})).per_share - price

    lo, hi = bounds
    f_lo, f_hi = gap(lo), gap(hi)
    if f_lo > 0:
        return Implied(math.nan, f"even {lo:.0%} first-year growth values the share above {price}")
    if f_hi < 0:
        return Implied(math.nan, f"even {hi:.0%} first-year growth values the share below {price}")
    return Implied(float(brentq(gap, lo, hi, xtol=1e-14, rtol=1e-14, maxiter=200)), None)


# ---------------------------------------------------------------------------
# Discount rate
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Beta:
    beta: float
    std_error: float
    observations: int


def regress_beta(asset_returns: Sequence[float], benchmark_returns: Sequence[float]) -> Beta:
    """OLS slope of asset on benchmark daily returns (already date-aligned)."""
    y, x = np.asarray(asset_returns, float), np.asarray(benchmark_returns, float)
    if y.shape != x.shape or y.size < 30:
        raise ValueError("need two aligned return series of at least 30 observations")
    ok = np.isfinite(x) & np.isfinite(y)
    y, x = y[ok], x[ok]
    xm, ym = x - x.mean(), y - y.mean()
    sxx = float(xm @ xm)
    if sxx == 0:
        raise ValueError("benchmark returns have no variance")
    beta = float(xm @ ym) / sxx
    resid = ym - beta * xm
    se = math.sqrt(float(resid @ resid) / (y.size - 2) / sxx)
    return Beta(beta, se, int(y.size))


@dataclass(frozen=True)
class WACC:
    wacc: float
    cost_of_equity: float
    cost_of_debt_pre_tax: float
    cost_of_debt_after_tax: float
    weight_equity: float
    weight_debt: float
    cost_of_debt_estimated: bool
    notes: Tuple[str, ...] = ()


def wacc(
    risk_free: float,
    beta: float,
    equity_market_value: float,
    debt: float,
    tax_rate: float,
    interest_expense: Optional[float] = None,
    equity_risk_premium: float = settings.DCF_EQUITY_RISK_PREMIUM,
    debt_spread: float = settings.DCF_DEBT_SPREAD,
) -> WACC:
    """
    CAPM cost of equity; cost of debt = interest / debt when interest is
    reported, else risk-free + spread (estimated, and said so). Market-value
    equity weight, book debt.
    """
    if equity_market_value <= 0:
        raise ValueError("equity market value must be positive")
    if not 0 <= tax_rate < 1:
        raise ValueError(f"tax rate {tax_rate} outside [0, 1)")
    ke = risk_free + beta * equity_risk_premium
    notes: List[str] = []
    estimated = interest_expense is None or debt <= 0
    if estimated:
        kd = risk_free + debt_spread
        notes.append(f"no interest expense reported: cost of debt estimated as risk-free + {debt_spread:.2%}")
    else:
        kd = interest_expense / debt
    debt = max(debt, 0.0)
    total = equity_market_value + debt
    we, wd = equity_market_value / total, debt / total
    kd_after = kd * (1.0 - tax_rate)
    return WACC(we * ke + wd * kd_after, ke, kd, kd_after, we, wd, estimated, tuple(notes))
