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
    test case. A filer with no operating cash flow on file, or no capex in a
    standard concept. And one that reports interest but has no debt balance
    within MAX_TTM_AGE_DAYS: missing debt is not zero debt.

BALANCES AGE TOO
    Debt and cash are used only if their balance date is within
    MAX_ANNUAL_AGE_DAYS (the last 10-K), the cover-page share count within
    MAX_TTM_AGE_DAYS; an instant has no other staleness check.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from config import settings
from modeling.dcf import WACC, Beta, DCFInputs, regress_beta, wacc
from modeling.statements import Statements

FALLBACK_TAX_RATE = 0.21
GROWTH_CLAMP = (-0.10, settings.DCF_DEFAULT_GROWTH_CAP)


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
    """
    The lower of the 3- and 5-year revenue CAGR (the 3-year alone with less
    history), clamped to GROWTH_CLAMP; an editable starting point. Decision 15.
    """
    rev = st.annual("revenue", as_of)
    if rev and (as_of - rev[-1].period_end).days > MAX_ANNUAL_AGE_DAYS:
        # APA's current filer tags revenue under a concept the list does not
        # carry, so its latest revenue was its predecessor's FY2020: a growth
        # rate six years stale. Stale revenue is not a growth rate.
        return settings.DCF_TERMINAL_GROWTH, (
            f"latest revenue on file ends {rev[-1].period_end} ({rev[-1].concept}): stale, terminal growth used"
        )
    if len(rev) >= 4 and rev[-4].value > 0 and rev[-1].value > 0:
        cagrs = {3: (rev[-1].value / rev[-4].value) ** (1 / 3) - 1}
        if len(rev) >= 6 and rev[-6].value > 0:
            cagrs[5] = (rev[-1].value / rev[-6].value) ** (1 / 5) - 1
        years, cagr = min(cagrs.items(), key=lambda kv: kv[1])
        clamped = min(max(cagr, GROWTH_CLAMP[0]), GROWTH_CLAMP[1])
        shown = " and ".join(f"{y}-year {c:.2%}" for y, c in sorted(cagrs.items()))
        return clamped, (f"lower revenue CAGR ({shown}) to FY {rev[-1].period_end}, includes acquisitions"
                         + (f"; clamped to {clamped:.0%}" if clamped != cagr else ""))
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
        return Refusal(f"{sector}: {settings.DCF_REFUSED_SECTORS[sector]}")
    notes: List[str] = []

    cfo, cfo_src = _ttm_or_annual(st, "cfo", as_of)
    if cfo is None:
        return Refusal("no operating cash flow on file before this date")
    capex, capex_src = _ttm_or_annual(st, "capex", as_of)
    if capex is None:
        # Missing capex is NOT zero: treating it as zero made APA's free cash
        # flow its whole operating cash flow (DCF $323 against a $45 price).
        # APA's current filer has tagged no standard capex concept since 2022.
        return Refusal(f"capital expenditure not reported in a standard concept ({capex_src}); "
                       "free cash flow cannot be formed")
    sbc, sbc_src = _ttm_or_annual(st, "share_based_comp", as_of)
    interest, interest_src = _ttm_or_annual(st, "interest_expense", as_of)
    if interest is None:
        paid, paid_src = _ttm_or_annual(st, "interest_paid", as_of)
        if paid is not None:
            # 54 of 132 filers without a current expense tag report only cash
            # interest paid. Close to expense for most borrowers; a proxy.
            interest, interest_src = paid, f"PROXY: cash interest paid, {paid_src}"
            notes.append("interest expense not reported: cash interest paid used as a proxy")
    if interest is not None and interest < 0:
        # DIS, FTV and LKQ tag InterestExpenseNonoperating as a negative number
        # (a sign convention, not interest income): LKQ's cost of debt was -5.5%.
        interest = -interest
        notes.append("interest expense filed as a negative number: its magnitude is used")
    tax = st.effective_tax_rate(as_of)
    if tax is None or not 0 <= tax < 1:
        notes.append(f"effective tax rate unavailable ({tax}); {FALLBACK_TAX_RATE:.0%} statutory used")
        tax = FALLBACK_TAX_RATE

    # Balances get the same age rule as trailing flows. Missing debt is NOT
    # zero for a filer that pays interest: LDOS was valued on its 2015 debt
    # ($1.1B of $6.0B, cost of debt 20%) and CMCSA on none of its $90B.
    # A balance from the last 10-K is the last reported balance, so it gets
    # the annual limit, like an annual flow: CAT, LEN, PHM, OMC and TMUS tag
    # debt only at fiscal year end. Past MAX_TTM_AGE_DAYS it is noted.
    debt_v = st.debt(as_of)
    if debt_v is not None and (as_of - debt_v.period_end).days <= MAX_ANNUAL_AGE_DAYS:
        debt, debt_src = debt_v.value, f"{debt_v.concept}, balance {debt_v.period_end}"
        if (as_of - debt_v.period_end).days > MAX_TTM_AGE_DAYS:
            notes.append(f"latest debt balance is from {debt_v.period_end} (fiscal year end)")
    elif interest is not None and interest > 0:
        last = f"; latest on file is {debt_v.period_end}" if debt_v else ""
        return Refusal(f"interest is reported ({interest_src}) but no debt balance within "
                       f"{MAX_ANNUAL_AGE_DAYS} days in a standard concept{last}; net debt cannot be formed")
    else:
        debt, debt_src = 0.0, "none on file and no interest reported: treated as debt-free"
        notes.append("no debt balance and no interest on file: treated as debt-free")
    liquid = st.liquid_assets(as_of)
    if liquid is not None and (as_of - liquid.period_end).days <= MAX_ANNUAL_AGE_DAYS:
        net_debt, net_src = debt - liquid.value, f"debt - ({liquid.concept}), balance {liquid.period_end}"
    else:
        net_debt, net_src = debt, "debt only: no cash balance on file"
        stale = f" since {liquid.period_end}" if liquid else ""
        notes.append(f"cash not reported{stale}: net debt taken as total debt")

    # A cover-page count can be years stale: Fox's only untagged one is 1.0
    # share from its 2019 registration (it reports current counts per class),
    # CMCSA's is from 2009, CHTR's from 2016. A count is used only if its
    # period ended within MAX_TTM_AGE_DAYS; the diluted average is the fallback.
    shares_v, shares_src = st.instant("shares_outstanding", as_of), "cover-page shares outstanding"
    if shares_v is None or (as_of - shares_v.period_end).days > MAX_TTM_AGE_DAYS:
        stale_cover = shares_v
        # The newest of the quarterly and annual averages: Fox's FY (to
        # 2026-06-30) is newer than its last filed quarter (2026-03-31).
        diluted = sorted(st.quarterly("diluted_shares", as_of) + st.annual("diluted_shares", as_of),
                         key=lambda v: (v.period_end, v.filed))
        if not diluted or (as_of - diluted[-1].period_end).days > MAX_TTM_AGE_DAYS:
            return Refusal("no share count on file within "
                           f"{MAX_TTM_AGE_DAYS} days of this date")
        shares_v, shares_src = diluted[-1], "latest diluted weighted average"
        if stale_cover is not None:
            notes.append(f"cover-page share count is from {stale_cover.period_end} (stale): diluted average used")
    shares = shares_v.value

    market_cap = price * shares
    discount = wacc(risk_free, beta.beta, market_cap, debt, tax, interest_expense=interest)
    notes.extend(discount.notes)
    if interest is None:
        interest = discount.cost_of_debt_pre_tax * debt
        interest_src = "estimated: (risk-free + spread) x total debt"

    after_tax_interest = interest * (1 - tax)
    subtract_sbc = settings.DCF_SUBTRACT_SBC and sbc is not None
    base_fcf = cfo - capex + after_tax_interest - (sbc if subtract_sbc else 0.0)

    op, op_src = _ttm_or_annual(st, "operating_income", as_of)
    da, da_src = _ttm_or_annual(st, "d_and_a", as_of)
    da_label = "D&A"
    if da is None:
        # No D&A total: depreciation plus amortisation of intangibles over the
        # SAME trailing period (MSFT tags only these), or depreciation alone.
        dep, dep_src = _ttm_or_annual(st, "depreciation", as_of)
        amort, amort_src = _ttm_or_annual(st, "amortization_intangibles", as_of)
        if dep is not None and amort is not None and amort_src == dep_src:
            da, da_src, da_label = dep + amort, dep_src, "depreciation + amortisation of intangibles"
        elif dep is not None:
            da, da_src, da_label = dep, dep_src, "depreciation only (no amortisation on file for the period)"
    ebitda = op + da if op is not None and da is not None else None
    ebitda_src = (f"operating income ({op_src}) + {da_label} ({da_src})" if ebitda is not None
                  else "operating income + D&A: not both on file")

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
        "total_debt": (debt, debt_src),
        "net_debt": (net_debt, net_src),
        "shares": (shares, f"{shares_src}, period {shares_v.period_end}, filed {shares_v.filed}"),
        "ebitda": (ebitda, ebitda_src),
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
                         benchmark: str = settings.DCF_BETA_BENCHMARK,
                         frequency: str = "daily") -> Beta:
    """
    Total-return log returns joined ON DATE with the benchmark's. `daily`
    uses the last `window` sessions (decision 4: 756, about 3 years);
    `weekly` uses the last `window` Friday closes (260 is about 5 years) —
    reported beside the decision-4 beta because low-beta names (MO: daily
    −0.07, weekly 0.16, monthly 0.46) depend heavily on the frequency.
    """
    end = datetime.combine(as_of, time(23, 59, 59), tzinfo=timezone.utc)
    days_back = int(window * 1.6) + 30 if frequency == "daily" else int(window * 7) + 30
    start = end - timedelta(days=days_back)

    async def closes(sym: str) -> Dict[date, float]:
        rows = await repo.fetch_range(sym, None, start, end, adjust="total")
        return {r.ohlcv.timestamp.utc.date(): float(r.ohlcv.close) for r in rows if r.ohlcv.close}

    a, b = await closes(symbol), await closes(benchmark)
    common = sorted(set(a) & set(b))
    if frequency == "weekly":
        # Last common close of each ISO week.
        last_of_week: Dict[Tuple[int, int], date] = {}
        for d in common:
            last_of_week[d.isocalendar()[:2]] = d
        common = sorted(last_of_week.values())
    elif frequency != "daily":
        raise ValueError(f"frequency must be daily or weekly; got {frequency!r}")
    days = common[-(window + 1):]
    if len(days) < 31:
        raise ValueError(f"only {len(days)} common sessions with {benchmark}")
    ra = np.diff(np.log([a[d] for d in days]))
    rb = np.diff(np.log([b[d] for d in days]))
    return regress_beta(ra, rb)
