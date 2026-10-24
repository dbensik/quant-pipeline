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
    cost), minus noncontrolling interests' share of income (decision 17:
    the flows are consolidated). Each term is returned as its own line. Interest comes from the
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
    Across a spin-off (decision 58): from its ex-date until a filing for a
    period after it (the price is the company after, the statements the
    company before), and while the trailing year bridges two bases (a
    business moved into discontinued operations after the 10-K). Then, by
    date alone (58b), until a 10-K for a fiscal year ending after the spin:
    a trailing year built on an earlier one straddles it.

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
from modeling.statements import COVER_CONCEPT, DILUTED_CONCEPT, Statements

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
    last = t[1][-1].period_end if t else (a[-1].period_end if a else None)
    return None, f"not reported{f' since {last}' if last else ''}"


def noncontrolling_income(st: Statements, as_of: date) -> Tuple[Optional[float], str, str]:
    """
    Noncontrolling interests' share of consolidated profit, trailing: (value,
    window, label). The total concept, else the redeemable and nonredeemable
    parts over ONE window (MPC, SPGI, ICE, WBD tag only the parts).
    """
    total, src = _ttm_or_annual(st, "nci_income", as_of)
    if total is not None:
        return total, src, "noncontrolling interests' share of income"
    parts = []
    for item, label in (
        ("nci_income_redeemable", "redeemable"),
        ("nci_income_nonredeemable", "nonredeemable"),
    ):
        v, part_src = _ttm_or_annual(st, item, as_of)
        if v is not None:
            parts.append((v, part_src, label))
    if not parts:
        return None, src, "noncontrolling interests' share of income"
    if len({w for _, w, _ in parts}) > 1:
        # MPC filed redeemable NCI (zero) through FY2025 and then stopped, so
        # its parts end on different windows. Their common latest fiscal year
        # is one window for both, if it is within the annual age rule.
        years = [
            (st.annual(item, as_of) or [None])[-1]
            for item in ("nci_income_redeemable", "nci_income_nonredeemable")
        ]
        years = [y for y in years if y is not None]
        ends = {y.period_end for y in years}
        if (
            len(years) == len(parts)
            and len(ends) == 1
            and (as_of - years[0].period_end).days <= MAX_ANNUAL_AGE_DAYS
        ):
            labels = " + ".join(label for _, _, label in parts)
            return (
                sum(y.value for y in years),
                f"FY to {years[0].period_end}",
                f"noncontrolling interests' share of income ({labels}; parts on one fiscal year)",
            )
        windows = ", ".join(f"{label} {w}" for _, w, label in parts)
        return (
            None,
            f"parts on different windows ({windows})",
            "noncontrolling interests' share of income",
        )
    labels = " + ".join(label for _, _, label in parts)
    return (
        sum(v for v, _, _ in parts),
        parts[0][1],
        f"noncontrolling interests' share of income ({labels})",
    )


def noncontrolling_balance(st: Statements, as_of: date) -> Tuple[float, Optional[date]]:
    """NCI in equity plus redeemable NCI, each only if within MAX_ANNUAL_AGE_DAYS."""
    total, end = 0.0, None
    for item in ("nci_balance", "nci_redeemable_balance"):
        v = st.instant(item, as_of)
        if v is not None and (as_of - v.period_end).days <= MAX_ANNUAL_AGE_DAYS:
            total += v.value
            end = v.period_end if end is None else max(end, v.period_end)
    return total, end


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
    # Each start year on the latest year's basis (Statements.annual_before).
    starts = {y: st.annual_before("revenue", rev[-1], y, as_of) for y in (3, 5)} if rev else {}
    if starts.get(3) is not None and starts[3].value > 0 and rev[-1].value > 0:
        cagrs = {3: (rev[-1].value / starts[3].value) ** (1 / 3) - 1}
        if starts[5] is not None and starts[5].value > 0:
            cagrs[5] = (rev[-1].value / starts[5].value) ** (1 / 5) - 1
        years, cagr = min(cagrs.items(), key=lambda kv: kv[1])
        clamped = min(max(cagr, GROWTH_CLAMP[0]), GROWTH_CLAMP[1])
        shown = " and ".join(f"{y}-year {c:.2%}" for y, c in sorted(cagrs.items()))
        return clamped, (
            f"lower revenue CAGR ({shown}) to FY {rev[-1].period_end}, includes acquisitions"
            + (f"; clamped to {clamped:.0%}" if clamped != cagr else "")
        )
    return settings.DCF_TERMINAL_GROWTH, "too little revenue history: terminal growth used"


def build_inputs(
    st: Statements,
    as_of: date,
    price: float,
    risk_free: float,
    beta: Beta,
    sector: Optional[str] = None,
    growth_first: Optional[float] = None,
    symbol: Optional[str] = None,
) -> "ValuationInputs | Refusal":
    sector_refusal = settings.dcf_sector_refusal(symbol, sector)
    if sector_refusal:
        return Refusal(f"{sector}: {sector_refusal}")
    notes: List[str] = []
    for f in st.set_aside(as_of):
        if (as_of - f.period_end).days > MAX_ANNUAL_AGE_DAYS:
            continue  # too old to have been read anyway (LLY's of 2014, today)
        notes.append(
            f"{f.concept} filed {f.filed} is dated {f.period_end}, after its own filing: set aside"
        )
    if sector in settings.DCF_REFUSED_SECTORS:
        notes.append(
            f"{sector} is refused as a sector; {symbol} is valued by name "
            f"({settings.DCF_ALLOWED_SYMBOLS[symbol.upper()]})"
        )

    for ex_date in st.spinoffs:
        after = ex_date + timedelta(days=settings.SPINOFF_FILING_MIN_DAYS)
        if ex_date <= as_of and not st.reports_period_ending(after, as_of):
            return Refusal(
                f"spun off a business on {ex_date}: the price is the company after it, and "
                f"no 10-Q or 10-K for a period ending on or after {after} is on file"
            )

    cfo, cfo_src = _ttm_or_annual(st, "cfo", as_of)
    if cfo is None:
        return Refusal("no operating cash flow on file before this date")
    shift = st.discontinued_cash(as_of) if cfo_src.startswith("TTM") else None
    if shift is not None:
        # The TTM is FY + YTD - prior YTD; a prior YTD re-presented after the
        # 10-K (HON's Aerospace) carries more discontinued cash than the year.
        fy_disc, prior_disc, fy_total, prior = shift
        if (abs(prior_disc) - abs(fy_disc)) / max(
            abs(fy_total.value), 1.0
        ) > settings.DCF_DISCONTINUED_BREAK_SHARE:
            return Refusal(
                f"the trailing year mixes two bases: the year to date to {prior.period_end} "
                f"carries {prior_disc / 1e6:,.0f}M of discontinued operating cash, the fiscal "
                f"year to {fy_total.period_end} it is subtracted from {fy_disc / 1e6:,.0f}M; "
                "a business was moved into discontinued operations after the 10-K, and the "
                "trailing year without it is not on file until the next one"
            )
    # A spin after the fiscal year the cash flows build on: the year carries
    # the spun business, the quarters since do not, whatever the tagging
    # (decision 58b: CMCSA, FDX tag no continuing CFO; BDX no pair of terms).
    annual = st.annual("cfo", as_of)
    for ex_date in st.spinoffs:
        if annual and annual[-1].period_end < ex_date <= as_of:
            return Refusal(
                f"spun off a business on {ex_date}, after the fiscal year to "
                f"{annual[-1].period_end} the trailing cash flows build on: that year carries "
                "the business and the quarters since do not, and a trailing year on one basis "
                f"is not on file until a 10-K for a year ending after {ex_date}"
            )
    capex, capex_src = _ttm_or_annual(st, "capex", as_of)
    if capex is None:
        # Missing capex is NOT zero: treating it as zero made APA's free cash
        # flow its whole operating cash flow (DCF $323 against a $45 price).
        # APA's current filer has tagged no standard capex concept since 2022.
        return Refusal(
            f"capital expenditure not reported in a standard concept ({capex_src}); "
            "free cash flow cannot be formed"
        )
    # Capitalised software and other intangibles, where filed: cash spent on
    # long-lived assets outside PP&E (FIS $835M of software beside $154M of
    # capex). Its own trailing window, labelled; a filer that tags none
    # capitalises none, so nothing is added and nothing is refused.
    intangible, intangible_src = _ttm_or_annual(st, "intangible_capex", as_of)
    mapped = st.line_concepts.get("intangible_capex", ())
    netted = any(c.startswith("-") for c in mapped)
    if intangible is None and netted:
        # A subtracted part (VST's allowance resales) missing for a period
        # drops the whole net line: the purchases would vanish from capex.
        added = [c for c in mapped if not c.startswith("-")]
        gross, _ = _ttm_or_annual(
            Statements(
                [f for fs in st.by_concept.values() for f in fs],
                st.splits,
                {**st.line_concepts, "intangible_capex": tuple(added)},
                st.spinoffs,
            ),
            "intangible_capex",
            as_of,
        )
        if gross is not None:
            return Refusal(
                "intangible purchases are reported but the sales netted against them "
                f"are not for the same periods ({' '.join(mapped)}); capex cannot be formed"
            )
    if intangible is not None:
        capex += intangible
        what = "net intangible purchases" if netted else "capitalised software/intangibles"
        capex_src += f"; plus {intangible / 1e6:,.0f}M {what} ({intangible_src})"
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
        notes.append(
            f"effective tax rate unavailable ({tax}); {FALLBACK_TAX_RATE:.0%} statutory used"
        )
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
        return Refusal(
            f"interest is reported ({interest_src}) but no debt balance within "
            f"{MAX_ANNUAL_AGE_DAYS} days in a standard concept{last}; net debt cannot be formed"
        )
    else:
        debt, debt_src = 0.0, "none on file and no interest reported: treated as debt-free"
        notes.append("no debt balance and no interest on file: treated as debt-free")
    liquid = st.liquid_assets(as_of)
    if liquid is not None and (as_of - liquid.period_end).days <= MAX_ANNUAL_AGE_DAYS:
        net_debt, net_src = (
            debt - liquid.value,
            f"debt - ({liquid.concept}), balance {liquid.period_end}",
        )
    else:
        net_debt, net_src = debt, "debt only: no cash balance on file"
        stale = f" since {liquid.period_end}" if liquid else ""
        notes.append(f"cash not reported{stale}: net debt taken as total debt")

    # A cover-page count can be years stale: Fox's only untagged one is 1.0
    # share from its 2019 registration (it reports current counts per class),
    # CMCSA's is from 2009, CHTR's from 2016. A count is used only if its
    # period ended within MAX_TTM_AGE_DAYS; the diluted average is the fallback.
    # A count of 0 is not a count (decision 60): ROP's Q3 2015 10-Q, BALL's
    # FY2021 10-K and 22 other filers' covers say 0 shares, which made the
    # market value 0 and wacc() raise.
    # A count in the wrong unit is set aside (decision 62) and named here
    # when it would have been the one read.
    off_scale = [
        f for f in st.off_scale_newest(as_of) if (as_of - f.period_end).days <= MAX_TTM_AGE_DAYS
    ]

    def note_off_scale(concept: str) -> None:
        for f in off_scale:
            if f.concept == concept:
                notes.append(
                    f"{f.concept} filed {f.filed} is {f.value:,.0f}, in another unit than "
                    "its filing's other share counts: set aside"
                )

    note_off_scale(COVER_CONCEPT)
    shares_v, shares_src = st.instant("shares_outstanding", as_of), "cover-page shares outstanding"
    zero_cover = shares_v is not None and shares_v.value <= 0
    if zero_cover or shares_v is None or (as_of - shares_v.period_end).days > MAX_TTM_AGE_DAYS:
        stale_cover = shares_v
        # The newest of the quarterly and annual averages: Fox's FY (to
        # 2026-06-30) is newer than its last filed quarter (2026-03-31).
        diluted = sorted(
            (
                v
                for v in st.quarterly("diluted_shares", as_of) + st.annual("diluted_shares", as_of)
                if v.value > 0
            ),
            key=lambda v: (v.period_end, v.filed),
        )
        if not diluted or (as_of - diluted[-1].period_end).days > MAX_TTM_AGE_DAYS:
            return Refusal(f"no share count on file within {MAX_TTM_AGE_DAYS} days of this date")
        note_off_scale(DILUTED_CONCEPT)
        shares_v, shares_src = diluted[-1], "latest diluted weighted average"
        if zero_cover:
            notes.append(
                f"cover-page share count for {stale_cover.period_end} is filed as "
                f"{stale_cover.value:g}: diluted average used"
            )
        elif stale_cover is not None:
            notes.append(
                f"cover-page share count is from {stale_cover.period_end} (stale): diluted average used"
            )
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

    # Decision 17: CFO, capex and EBITDA are CONSOLIDATED, so the partners in
    # subsidiaries the company controls but does not wholly own are paid out
    # of them. Their share of income comes off base FCF: DVA's clinic partners
    # earned $332M of $1,079M in FY2025, and the DCF had credited all of it to
    # DVA's shareholders.
    nci, nci_window, nci_label = noncontrolling_income(st, as_of)
    nci_bal, nci_bal_end = noncontrolling_balance(st, as_of)
    if nci is None:
        if abs(nci_bal) >= settings.DCF_NCI_IMMATERIAL_BALANCE_SHARE * market_cap:
            return Refusal(
                f"noncontrolling interests of {nci_bal / 1e6:,.0f}M on the balance sheet "
                f"({nci_bal_end}) but their share of income is not reported ({nci_window}); "
                "the shareholders' cash flow cannot be formed"
            )
        if nci_bal:
            notes.append(
                f"noncontrolling interests of {nci_bal / 1e6:,.0f}M ({nci_bal_end}) with no share "
                "of income on file: immaterial, not subtracted"
            )
            nci_line = (
                None,
                f"not reported ({nci_window}); balance {nci_bal / 1e6:,.0f}M immaterial",
            )
        else:
            nci_line = (None, "none on file")
    else:
        if base_fcf > 0 and nci > settings.DCF_NCI_MAX_FCF_SHARE * base_fcf:
            return Refusal(
                f"{nci_label} ({nci / 1e6:,.0f}M, {nci_window}) is {nci / base_fcf:.0%} of "
                f"consolidated free cash flow ({base_fcf / 1e6:,.0f}M): over "
                f"{settings.DCF_NCI_MAX_FCF_SHARE:.0%}, the partners' share of cash flow "
                "cannot be estimated from it"
            )
        if nci < 0:
            # Partners who share a LOSS do not pay the parent their share in
            # cash: ROK's -$110M (TTM to 2026-06-30) would have added $110M
            # of free cash flow that does not exist. Only a claim comes off.
            nci_line = (
                nci,
                f"{nci_label}, {nci_window}; a loss shared by the partners: not added back",
            )
            nci = 0.0
        else:
            base_fcf -= nci
            nci_line = (nci, f"{nci_label}, {nci_window}; subtracted")

    op, op_src = _ttm_or_annual(st, "operating_income", as_of)
    da, da_src = _ttm_or_annual(st, "d_and_a", as_of)
    da_label = "D&A"
    if da is None:
        # No D&A total: depreciation plus amortisation of intangibles over the
        # SAME trailing period (MSFT tags only these), or depreciation alone.
        dep, dep_src = _ttm_or_annual(st, "depreciation", as_of)
        amort, amort_src = _ttm_or_annual(st, "amortization_intangibles", as_of)
        if dep is not None and amort is not None and amort_src == dep_src:
            da, da_src, da_label = (
                dep + amort,
                dep_src,
                "depreciation + amortisation of intangibles",
            )
        elif dep is not None:
            da, da_src, da_label = (
                dep,
                dep_src,
                "depreciation only (no amortisation on file for the period)",
            )
    ebitda = op + da if op is not None and da is not None else None
    ebitda_src = (
        f"operating income ({op_src}) + {da_label} ({da_src})"
        if ebitda is not None
        else "operating income + D&A: not both on file"
    )
    # The exit multiple values consolidated EBITDA, so it takes the parent's
    # share of consolidated profit over the NCI line's own window; without
    # that share it is dropped rather than left on the partners' earnings.
    exit_base = ebitda
    if ebitda is not None and nci:
        consolidated, cons_src = _ttm_or_annual(st, "consolidated_net_income", as_of)
        if consolidated is None or cons_src != nci_window:
            ni, ni_src = _ttm_or_annual(st, "net_income", as_of)
            consolidated, cons_src = (
                (ni + nci, ni_src) if ni is not None and ni_src == nci_window else (None, "")
            )
        parent = 1 - nci / consolidated if consolidated else None
        if parent is not None and consolidated > 0 and parent > 0:
            exit_base = ebitda * parent
            ebitda_src += (
                f"; x {parent:.3f}, the parent's share of consolidated profit ({nci_window})"
            )
        else:
            exit_base = None
            ebitda_src += (
                "; no exit value: the parent's share of consolidated profit is not defined"
            )
            notes.append(
                "exit-multiple cross-check dropped: noncontrolling interests' share of "
                "consolidated profit is not defined for the period"
            )

    # Decision 18: preferred stock is a claim ahead of the common, paid below
    # CFO, so it comes off equity value like debt (OXY's $8.5B, ALB's $2.2B
    # mandatory convertible: at conversion it is dilution of about that
    # value). A filer that tags no preferred has none; a stale balance is not
    # today's, by the same age rule as debt.
    pref_v = st.preferred_stock(as_of)
    if pref_v is not None and (as_of - pref_v.period_end).days <= MAX_ANNUAL_AGE_DAYS:
        preferred, pref_src = pref_v.value, f"{pref_v.concept}, balance {pref_v.period_end}"
    else:
        stale = f" since {pref_v.period_end}" if pref_v else ""
        preferred, pref_src = 0.0, f"none on file{stale}"
    # Equity-method stakes are NOT added: the dividends they pay are already
    # inside CFO (SRE receives $1,120M on $1,604M of income), and book value
    # is cost, not value. Large ones are named so the reader knows.
    emi = st.instant("equity_method_investments", as_of)
    if (
        emi is not None
        and (as_of - emi.period_end).days <= MAX_ANNUAL_AGE_DAYS
        and emi.value >= settings.DCF_EQUITY_METHOD_NOTE_SHARE * market_cap
    ):
        notes.append(
            f"equity-method stakes of {emi.value / 1e6:,.0f}M ({emi.value / market_cap:.0%} of "
            f"market cap, {emi.period_end}) are valued only through the dividends they pay into CFO"
        )
    # Decision 19: unfunded pensions are NOT subtracted. Contributions,
    # catch-up payments included, are already inside CFO, and the totals are
    # missing for filers that tag per plan (GE, T), so a line would treat
    # them as having no deficit. Large ones are named.
    pension = st.pension_deficit(as_of)
    if (
        pension is not None
        and (as_of - pension.period_end).days <= MAX_ANNUAL_AGE_DAYS
        and pension.value >= settings.DCF_PENSION_NOTE_SHARE * market_cap
    ):
        notes.append(
            f"unfunded pension and retiree benefits of {pension.value / 1e6:,.0f}M "
            f"({pension.value / market_cap:.0%} of market cap, {pension.period_end}) are not "
            "subtracted: contributions are paid inside CFO"
        )

    if growth_first is None:
        growth_first, growth_src = default_growth(st, as_of)
    else:
        growth_src = "given"

    dcf = DCFInputs(
        base_fcf=base_fcf,
        growth_first=growth_first,
        growth_terminal=settings.DCF_TERMINAL_GROWTH,
        wacc=discount.wacc,
        net_debt=net_debt,
        shares=shares,
        exit_metric_base=exit_base,
        exit_multiple=settings.DCF_EXIT_MULTIPLE if exit_base else None,
        preferred=preferred,
    )
    lines = {
        "cfo": (cfo, cfo_src),
        "capex": (capex, capex_src),
        "after_tax_interest": (after_tax_interest, interest_src),
        "share_based_comp": (
            sbc,
            sbc_src + ("; subtracted" if subtract_sbc else "; NOT subtracted"),
        ),
        "noncontrolling_interests": nci_line,
        "base_fcf": (
            base_fcf,
            "CFO - capex + after-tax interest"
            + (" - SBC" if subtract_sbc else "")
            + (" - noncontrolling interests" if nci else ""),
        ),
        "tax_rate": (tax, "effective, latest fiscal year"),
        "total_debt": (debt, debt_src),
        "net_debt": (net_debt, net_src),
        "preferred_stock": (preferred, pref_src),
        "shares": (shares, f"{shares_src}, period {shares_v.period_end}, filed {shares_v.filed}"),
        "ebitda": (ebitda, ebitda_src),
        "growth_first": (growth_first, growth_src),
        "beta": (
            beta.beta,
            f"vs {settings.DCF_BETA_BENCHMARK}, {beta.observations} daily returns, SE {beta.std_error:.3f}",
        ),
        "risk_free": (risk_free, "^IRX continuous, as of the valuation date"),
    }
    return ValuationInputs(
        dcf=dcf, discount=discount, price=price, market_cap=market_cap, lines=lines, notes=notes
    )


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


async def beta_from_bars(
    repo: Any,
    symbol: str,
    as_of: date,
    window: int = settings.DCF_BETA_WINDOW_DAYS,
    benchmark: str = settings.DCF_BETA_BENCHMARK,
    frequency: str = "daily",
) -> Beta:
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
    days = common[-(window + 1) :]
    if len(days) < 31:
        raise ValueError(f"only {len(days)} common sessions with {benchmark}")
    ra = np.diff(np.log([a[d] for d in days]))
    rb = np.diff(np.log([b[d] for d in days]))
    return regress_beta(ra, rb)
