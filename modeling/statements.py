"""
modeling/statements.py

Standardised, POINT-IN-TIME line items from raw SEC facts. Tier 3 phase 2 of
research/dcf-plan-2026-10-08.md. Pure: takes stored `Fact`s and splits.

THE READ RULE: a fact is visible at `as_of` only if it was filed STRICTLY
BEFORE `as_of` (measured 2026-10-09: 13 of AAPL's 38 10-K/10-Qs were
accepted after the 16:00 close on their filing date). For any period, the
visible row filed LAST wins, so a restatement replaces the original value
from the day after it was filed, and never before.

CONCEPT DRIFT: each line item is an ordered list of concepts; for each
period the first concept with a visible value is used and recorded. AAPL
revenue is SalesRevenueNet to FY2017, Revenues to FY2018, and
RevenueFromContractWithCustomerExcludingAssessedTax (ASC 606) after: one
continuous series. A period with none of them is MISSING, never zero.

PERIODS: annual = a duration of 350-380 days; quarter = 80-100 days. 10-Qs
also carry 6- and 9-month year-to-date rows, which the span filter drops.
Q4 is never filed on its own: it is derived as FY minus the three quarters
inside that fiscal year, and marked derived.

TTM: CASH-FLOW lines are filed year-to-date ONLY (Apple's 10-Qs carry 3-, 6-
and 9-month cash flows from the fiscal year start, never a lone quarter
after Q1), so four quarters rarely exist for them. TTM is therefore the
standard identity FY + YTD(current) - YTD(same span last year) whenever that
is more recent than four contiguous quarters.

SHARE COUNTS AND SPLITS: a share count is in the split basis of the day it
was FILED, not of its period. AAPL's FY2019 diluted shares were filed as
4.649B in 2019 and as 18.596B in the 10-Ks after the 2020-08-31 4:1 split.
So a count filed before a split that falls on or before `as_of` is
multiplied by that split's ratio; a count filed after it already includes
it. Every per-share figure is then in `as_of`'s basis, the basis of the
price on that day.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from core.fundamentals import Fact

LINE_ITEMS: Dict[str, Tuple[str, ...]] = {
    # durations
    "revenue": ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"),
    "cost_of_revenue": ("CostOfGoodsAndServicesSold",),
    "operating_income": ("OperatingIncomeLoss",),
    "pretax_income": ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",),
    "income_tax": ("IncomeTaxExpenseBenefit",),
    "interest_expense": ("InterestExpense", "InterestExpenseNonoperating"),
    "net_income": ("NetIncomeLoss",),
    "d_and_a": ("DepreciationDepletionAndAmortization", "DepreciationAmortizationAndAccretionNet"),
    "share_based_comp": ("ShareBasedCompensation",),
    "cfo": ("NetCashProvidedByUsedInOperatingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment",),
    "diluted_shares": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
    # instants
    "cash": ("CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
    "securities_current": ("MarketableSecuritiesCurrent",),
    "securities_noncurrent": ("MarketableSecuritiesNoncurrent",),
    "long_term_debt": ("LongTermDebt",),
    "long_term_debt_noncurrent": ("LongTermDebtNoncurrent",),
    "long_term_debt_current": ("LongTermDebtCurrent",),
    "commercial_paper": ("CommercialPaper",),
    "equity": ("StockholdersEquity",),
    "shares_outstanding": ("EntityCommonStockSharesOutstanding",),
}
INSTANTS = {"cash", "securities_current", "securities_noncurrent", "long_term_debt", "long_term_debt_noncurrent",
            "long_term_debt_current", "commercial_paper", "equity", "shares_outstanding"}
SHARE_ITEMS = {"diluted_shares", "shares_outstanding"}
ANNUAL_DAYS = (350, 380)
QUARTER_DAYS = (80, 100)


@dataclass(frozen=True)
class Value:
    period_start: Optional[date]
    period_end: date
    value: float
    concept: str
    filed: date
    derived: bool = False


class Statements:
    def __init__(self, facts: Iterable[Fact], splits: Sequence[Tuple[date, float]] = ()) -> None:
        self.by_concept: Dict[str, List[Fact]] = {}
        for f in facts:
            self.by_concept.setdefault(f.concept, []).append(f)
        self.splits = sorted(splits)

    # -- primitives -------------------------------------------------------

    def _scale(self, item: str, f: Fact, as_of: date) -> float:
        if item not in SHARE_ITEMS:
            return f.value
        factor = 1.0
        for ex_date, ratio in self.splits:
            if f.filed < ex_date <= as_of:
                factor *= ratio
        return f.value * factor

    def _periods(self, item: str, as_of: date, span: Optional[Tuple[int, int]]) -> Dict[Tuple, Value]:
        """{(start, end): Value} — per period, first concept with a visible row, latest filed."""
        out: Dict[Tuple, Value] = {}
        for concept in LINE_ITEMS[item]:
            best: Dict[Tuple, Fact] = {}
            for f in self.by_concept.get(concept, []):
                if not f.filed < as_of:
                    continue
                if span is None:
                    if f.period_start is not None:
                        continue
                else:
                    if f.period_start is None:
                        continue
                    days = (f.period_end - f.period_start).days
                    if not span[0] <= days <= span[1]:
                        continue
                key = (f.period_start, f.period_end)
                if key not in best or f.filed > best[key].filed or (f.filed == best[key].filed and f.accn > best[key].accn):
                    best[key] = f
            for key, f in best.items():
                if key in out:
                    continue  # a higher-priority concept already covers this period
                if span is not None and any(k[1] == key[1] for k in out):
                    continue  # same period end from a higher-priority concept, slightly different start
                out[key] = Value(f.period_start, f.period_end, self._scale(item, f, as_of), f.concept, f.filed)
        return out

    # -- public ------------------------------------------------------------

    def annual(self, item: str, as_of: date) -> List[Value]:
        return sorted(self._periods(item, as_of, ANNUAL_DAYS).values(), key=lambda v: v.period_end)

    def quarterly(self, item: str, as_of: date) -> List[Value]:
        """Filed quarters plus each fiscal year's derived Q4 (FY minus Q1-Q3)."""
        quarters = self._periods(item, as_of, QUARTER_DAYS)
        out = list(quarters.values())
        if item in SHARE_ITEMS:
            return sorted(out, key=lambda v: v.period_end)  # averages do not subtract
        for fy in self.annual(item, as_of):
            inside = [q for q in quarters.values() if q.period_start >= fy.period_start and q.period_end <= fy.period_end]
            if len(inside) != 3 or any(q.period_end == fy.period_end for q in quarters.values()):
                continue
            last = max(q.period_end for q in inside)
            out.append(Value(date.fromordinal(last.toordinal() + 1), fy.period_end,
                             fy.value - sum(q.value for q in inside), fy.concept, fy.filed, derived=True))
        return sorted(out, key=lambda v: v.period_end)

    def _ttm_quarters(self, item: str, as_of: date) -> Optional[Tuple[float, List[Value]]]:
        qs = self.quarterly(item, as_of)[-4:]
        if len(qs) < 4:
            return None
        for a, b in zip(qs, qs[1:]):
            if (b.period_start - a.period_end).days > 7:
                return None
        return sum(q.value for q in qs), qs

    def _ttm_ytd(self, item: str, as_of: date) -> Optional[Tuple[float, List[Value]]]:
        """FY + YTD(current) - YTD(prior year, same span): [prior, FY, current]."""
        annual = self.annual(item, as_of)
        if not annual:
            return None
        fy = annual[-1]
        ytd = list(self._periods(item, as_of, (80, 300)).values())
        current = [v for v in ytd if v.period_start is not None and (v.period_start - fy.period_end).days in (0, 1)]
        if not current:
            return None
        cur = max(current, key=lambda v: v.period_end)
        target = cur.period_end.toordinal() - 364
        prior = [v for v in ytd if v.period_start == fy.period_start and abs(v.period_end.toordinal() - target) <= 7]
        if not prior:
            return None
        pri = prior[0]
        return fy.value + cur.value - pri.value, [pri, fy, cur]

    def ttm(self, item: str, as_of: date) -> Optional[Tuple[float, List[Value]]]:
        """
        Trailing twelve months ending at the latest visible period: four
        contiguous quarters, or FY + YTD - prior YTD, whichever ends later.
        The last Value in the list is the period the TTM ends with.
        """
        options = [o for o in (self._ttm_quarters(item, as_of), self._ttm_ytd(item, as_of)) if o is not None]
        if not options:
            return None
        return max(options, key=lambda o: o[1][-1].period_end)

    def instant(self, item: str, as_of: date) -> Optional[Value]:
        """The latest balance as of the latest period end visible at `as_of`."""
        values = self._periods(item, as_of, None)
        if not values:
            return None
        return max(values.values(), key=lambda v: (v.period_end, v.filed))

    def at_period(self, item: str, period_end: date, as_of: date) -> Optional[Value]:
        """One period's value as known at `as_of` (an instant, or an annual or quarterly duration)."""
        spans = (None,) if item in INSTANTS else (ANNUAL_DAYS, QUARTER_DAYS)
        candidates = [v for sp in spans for v in self._periods(item, as_of, sp).values() if v.period_end == period_end]
        return max(candidates, key=lambda v: v.period_start or date.min) if candidates else None

    # -- derived -------------------------------------------------------------

    def free_cash_flow_annual(self, as_of: date) -> List[Tuple[date, float]]:
        cfo = {v.period_end: v.value for v in self.annual("cfo", as_of)}
        capex = {v.period_end: v.value for v in self.annual("capex", as_of)}
        return [(end, cfo[end] - capex[end]) for end in sorted(cfo) if end in capex]

    def total_debt(self, as_of: date) -> Optional[float]:
        """Long-term debt (current + noncurrent) plus commercial paper, latest balance date."""
        total = self.instant("long_term_debt", as_of)
        parts = None
        if total is None:
            nc, cur = self.instant("long_term_debt_noncurrent", as_of), self.instant("long_term_debt_current", as_of)
            if nc is None:
                return None
            parts = nc.value + (cur.value if cur and cur.period_end == nc.period_end else 0.0)
        base = total.value if total is not None else parts
        end = total.period_end if total is not None else nc.period_end
        cp = self.instant("commercial_paper", as_of)
        return base + (cp.value if cp and cp.period_end == end else 0.0)

    def net_debt(self, as_of: date) -> Optional[float]:
        debt, cash = self.total_debt(as_of), self.instant("cash", as_of)
        if debt is None or cash is None:
            return None
        # Marketable securities, current AND noncurrent, net against debt: both
        # are liquid investments, not operating assets (a choice, recorded in
        # the plan; Apple's noncurrent book is $77.7B on 2025-09-27).
        liquid = 0.0
        for item in ("securities_current", "securities_noncurrent"):
            v = self.instant(item, as_of)
            if v and v.period_end == cash.period_end:
                liquid += v.value
        return debt - cash.value - liquid

    def effective_tax_rate(self, as_of: date) -> Optional[float]:
        tax, pre = self.annual("income_tax", as_of), self.annual("pretax_income", as_of)
        if not tax or not pre or tax[-1].period_end != pre[-1].period_end or pre[-1].value == 0:
            return None
        return tax[-1].value / pre[-1].value
