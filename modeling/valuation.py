"""
modeling/valuation.py

Turn stored facts, prices and the risk-free rate into DCF inputs, with the
provenance of every number. Tier 3 phase 3 of research/dcf-plan-2026-10-08.md.

TWO PRICE BASES, NEVER MIXED
    Share counts from modeling/statements.py are in the split basis of the
    as-of date (the basis the stock traded in that day). So every price set
    against them — market cap in the WACC weights, value per share against
    price, the reverse DCF's target — is the price AS TRADED: fetch_range
    with adjust="none". A split-adjusted price would quarter Apple's market
    cap at any date before 2020-08-31. Beta uses TOTAL-return prices
    (adjust="total"), returns joined on date, not position.

FREE CASH FLOW (unlevered, trailing twelve months)
    CFO - capex + after-tax interest, minus stock-based compensation when
    DCF_SUBTRACT_SBC (the default: it is added back inside CFO and is a real
    cost). Each term is returned as its own line. Interest comes from the
    filer when reported; Apple stopped tagging InterestExpense after mid-2023,
    and then it is ESTIMATED as (risk-free + spread) x debt and marked so.

SHARES
    The cover-page count (dei:EntityCommonStockSharesOutstanding) as of the
    latest filing; the latest diluted weighted average if there is none.
    Which one was used is returned.

REFUSED
    Financials (bank and insurer cash flow is not free cash flow): SVB is the
    test case. And a filer with no operating cash flow on file.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from config import settings
from modeling.dcf import DCFInputs, WACC, Beta, regress_beta, wacc
from modeling.statements import Statements

FALLBACK_TAX_RATE = 0.21
GROWTH_CLAMP = (-0.10, 0.25)


@dataclass
class Refusal:
    reason: str


@dataclass
class ValuationInputs:
    dcf: DCFInputs
    discount: WACC
    price: float
    market_cap: float
    lines: Dict[str, Any]
    notes: List[str] = field(default_factory=list)


#: A trailing figure older than this is treated as NOT REPORTED: Apple's last
#: tagged interest expense is the quarter to 2023-07-01, and using it against
#: 2026 debt mixed two different years silently.
MAX_TTM_AGE_DAYS = 200
MAX_ANNUAL_AGE_DAYS = 460


def _ttm_or_annual(st: Statements, item: str, as_of: date) -> Tuple[Optional[float], str]:
    t = st.ttm(item, as_of)
    if t is not None and (as_of - t[1][-1].period_end).days <= MAX_TTM_AGE_DAYS:
        return t[0], f"TTM to {t[1][-1].period_end}"
    a = st.annual(item, as_of)
    if a and (as_of - a[-1].period_end).days <= MAX_ANNUAL_AGE_DAYS:
        return a[-1].value, f"FY to {a[-1].period_end}"
    last = (t[1][-1].period_end if t else (a[-1].period_end if a else None))
    return None, f"not reported{f' since {last}' if last else ''}"


def default_growth(st: Statements, as_of: date) -> Tuple[float, str]:
    """3-year revenue CAGR, clamped to GROWTH_CLAMP; an editable starting point."""
    rev = st.annual("revenue", as_of)
    if len(rev) >= 4 and rev[-4].value > 0 and rev[-1].value > 0:
        cagr = (rev[-1].value / rev[-4].value) ** (1 / 3) - 1
        clamped = min(max(cagr, GROWTH_CLAMP[0]), GROWTH_CLAMP[1])
        return clamped, f"3-year revenue CAGR {cagr:.2%} to FY {rev[-1].period_end}" + (" (clamped)" if clamped != cagr else "")
    return settings.DCF_TERMINAL_GROWTH, "too little revenue history: terminal growth used"


def build_inputs(
    st: Statements,
    as_of: date,
    price: float,
    risk_free: float,
    beta: Beta,
    sector: Optional[str] = None,
    growth_first: Optional[float] = None,
) -> "ValuationInputs | Refusal":
    if sector in settings.DCF_REFUSED_SECTORS:
        return Refusal(f"{sector}: free cash flow is not meaningful for banks and insurers")
    notes: List[str] = []

    cfo, cfo_src = _ttm_or_annual(st, "cfo", as_of)
    if cfo is None:
        return Refusal("no operating cash flow on file before this date")
    capex, capex_src = _ttm_or_annual(st, "capex", as_of)
    sbc, sbc_src = _ttm_or_annual(st, "share_based_comp", as_of)
    interest, interest_src = _ttm_or_annual(st, "interest_expense", as_of)
    tax = st.effective_tax_rate(as_of)
    if tax is None or not 0 <= tax < 1:
        notes.append(f"effective tax rate unavailable ({tax}); {FALLBACK_TAX_RATE:.0%} statutory used")
        tax = FALLBACK_TAX_RATE

    debt = st.total_debt(as_of) or 0.0
    net_debt = st.net_debt(as_of)
    if net_debt is None:
        net_debt = debt
        notes.append("cash not reported: net debt taken as total debt")

    shares_v = st.instant("shares_outstanding", as_of)
    shares_src = "cover-page shares outstanding"
    if shares_v is None:
        diluted = st.quarterly("diluted_shares", as_of) or st.annual("diluted_shares", as_of)
        if not diluted:
            return Refusal("no share count on file before this date")
        shares_v, shares_src = diluted[-1], "latest diluted weighted average"
    shares = shares_v.value

    market_cap = price * shares
    discount = wacc(risk_free, beta.beta, market_cap, debt, tax, interest_expense=interest)
    notes.extend(discount.notes)
    if interest is None:
        interest = discount.cost_of_debt_pre_tax * debt
        interest_src = "estimated: (risk-free + spread) x total debt"

    after_tax_interest = interest * (1 - tax)
    subtract_sbc = settings.DCF_SUBTRACT_SBC and sbc is not None
    base_fcf = cfo - (capex or 0.0) + after_tax_interest - (sbc if subtract_sbc else 0.0)

    op, _ = _ttm_or_annual(st, "operating_income", as_of)
    da, _ = _ttm_or_annual(st, "d_and_a", as_of)
    ebitda = op + da if op is not None and da is not None else None

    if growth_first is None:
        growth_first, growth_src = default_growth(st, as_of)
    else:
        growth_src = "given"

    dcf = DCFInputs(
        base_fcf=base_fcf, growth_first=growth_first, growth_terminal=settings.DCF_TERMINAL_GROWTH,
        wacc=discount.wacc, net_debt=net_debt, shares=shares,
        exit_metric_base=ebitda, exit_multiple=settings.DCF_EXIT_MULTIPLE if ebitda else None,
    )
    lines = {
        "cfo": (cfo, cfo_src), "capex": (capex, capex_src),
        "after_tax_interest": (after_tax_interest, interest_src),
        "share_based_comp": (sbc, sbc_src + ("; subtracted" if subtract_sbc else "; NOT subtracted")),
        "base_fcf": (base_fcf, "CFO - capex + after-tax interest" + (" - SBC" if subtract_sbc else "")),
        "tax_rate": (tax, "effective, latest fiscal year"),
        "total_debt": (debt, "long-term debt + commercial paper, latest balance"),
        "net_debt": (net_debt, "debt - cash - marketable securities (current and noncurrent)"),
        "shares": (shares, f"{shares_src}, period {shares_v.period_end}, filed {shares_v.filed}"),
        "ebitda": (ebitda, "operating income + D&A, TTM"),
        "growth_first": (growth_first, growth_src),
        "beta": (beta.beta, f"vs {settings.DCF_BETA_BENCHMARK}, {beta.observations} daily returns, SE {beta.std_error:.3f}"),
        "risk_free": (risk_free, "^IRX continuous, as of the valuation date"),
    }
    return ValuationInputs(dcf=dcf, discount=discount, price=price, market_cap=market_cap, lines=lines, notes=notes)


# ---------------------------------------------------------------------------
# Prices from the market-data repository
# ---------------------------------------------------------------------------

async def price_as_traded(repo: Any, symbol: str, as_of: date) -> Optional[Tuple[date, float]]:
    """The last close on or before `as_of`, AS TRADED (adjust='none')."""
    end = datetime.combine(as_of, time(23, 59, 59), tzinfo=timezone.utc)
    start = end - timedelta(days=14)
    bars = await repo.fetch_range(symbol, None, start, end, adjust="none")
    if not bars:
        return None
    last = bars[-1].ohlcv
    return last.timestamp.utc.date(), float(last.close)


async def beta_from_bars(repo: Any, symbol: str, as_of: date,
                         window: int = settings.DCF_BETA_WINDOW_DAYS,
                         benchmark: str = settings.DCF_BETA_BENCHMARK) -> Beta:
    """Daily total-return returns joined ON DATE with the benchmark's."""
    end = datetime.combine(as_of, time(23, 59, 59), tzinfo=timezone.utc)
    start = end - timedelta(days=int(window * 1.6) + 30)

    async def closes(sym: str) -> Dict[date, float]:
        rows = await repo.fetch_range(sym, None, start, end, adjust="total")
        return {r.ohlcv.timestamp.utc.date(): float(r.ohlcv.close) for r in rows if r.ohlcv.close}

    a, b = await closes(symbol), await closes(benchmark)
    days = sorted(set(a) & set(b))[-(window + 1):]
    if len(days) < 31:
        raise ValueError(f"only {len(days)} common sessions with {benchmark}")
    ra = np.diff(np.log([a[d] for d in days]))
    rb = np.diff(np.log([b[d] for d in days]))
    return regress_beta(ra, rb)
