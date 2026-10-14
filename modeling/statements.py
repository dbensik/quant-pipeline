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

import math
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from core.fundamentals import Fact

LINE_ITEMS: Dict[str, Tuple[str, ...]] = {
    # durations
    # IncludingAssessedTax: KHC and SJM file revenue only under it (KHC's
    # latest revenue on the list was 2014, so its growth was terminal growth).
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
    ),
    "cost_of_revenue": ("CostOfGoodsAndServicesSold",),
    "operating_income": ("OperatingIncomeLoss",),
    "pretax_income": (
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    ),
    "income_tax": ("IncomeTaxExpenseBenefit",),
    "interest_expense": (
        "InterestExpense",
        "InterestExpenseNonoperating",
        "InterestExpenseDebt",
        "InterestAndDebtExpense",
    ),
    #: Cash interest paid: a PROXY for expense, used only when none is reported.
    "interest_paid": ("InterestPaidNet", "InterestPaid"),
    "net_income": ("NetIncomeLoss",),
    # Noncontrolling interests (decision 17, 2026-10-13): the partners' share
    # of consolidated profit, subtracted from base FCF in valuation.py. Seven
    # valued filers tag only the redeemable and nonredeemable parts (MPC,
    # SPGI, ICE, WBD, SRE, ACN, EL), summed there over one window.
    "nci_income": ("NetIncomeLossAttributableToNoncontrollingInterest",),
    "nci_income_redeemable": ("NetIncomeLossAttributableToRedeemableNoncontrollingInterest",),
    "nci_income_nonredeemable": ("NetIncomeLossAttributableToNonredeemableNoncontrollingInterest",),
    #: Consolidated profit, partners included: the parent's share of it
    #: scales the exit-multiple EBITDA.
    "consolidated_net_income": ("ProfitLoss",),
    "d_and_a": (
        "DepreciationDepletionAndAmortization",
        "DepreciationAmortizationAndAccretionNet",
        "DepreciationAndAmortization",
    ),
    #: The two halves, for the 77 of 104 filers without a D&A total that tag
    #: Depreciation (MSFT among them); summed per period in valuation.py.
    "depreciation": ("Depreciation",),
    "amortization_intangibles": ("AmortizationOfIntangibleAssets",),
    "share_based_comp": ("ShareBasedCompensation",),
    # Continuing operations FIRST (2026-10-13), to match capex: CARR, HON and
    # OXY present capex inside continuing investing, with discontinued
    # operations on lines of their own, so total CFO less capex counted the
    # cash flow of businesses being sold (CARR +$424M, HON +$333M, OXY +$926M
    # in FY2025). A filer without discontinued operations tags only the total.
    "cfo": (
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        "NetCashProvidedByUsedInOperatingActivities",
    ),
    "capex": (
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquireOilAndGasPropertyAndEquipment",
        "PaymentsToExploreAndDevelopOilAndGasProperties",
        "PaymentsToAcquireOilAndGasProperty",
        # From a census of the 26 refused for capex (2026-10-11): each the
        # filer's only capex line. LLY, WEC, LNT, ADP; VZ ($17.0B), INCY, ROP;
        # ED ($4.8B, construction); GLW, IT, SNA; RL. D ($12.7B) reports it
        # net of disposal proceeds. The other 13 tag capex only under their
        # own extension concepts (COP, NEE, PSX, VLO, URI, ...) and stay refused.
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
        "PaymentsToAcquireOtherProductiveAssets",
        "PaymentsForConstructionInProcess",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireMachineryAndEquipment",
        "PaymentsForProceedsFromProductiveAssets",
    ),
    # Capital spending OUTSIDE property, plant and equipment, added to capex
    # in valuation.py (2026-10-13). By rule only the software concepts, which
    # name what they are: all 17 valued filers that tag them (FIS "Additions
    # to software" $835M beside $154M of capex, JKHY, ICE, DASH, AKAM, ...)
    # show them as a line of their own, outside capex. The broader
    # PaymentsToAcquireIntangibleAssets is software at CMCSA, spectrum at
    # TMUS, an acquisition at SNPS ($16.7B, Ansys) and emission allowances at
    # VST, so it is read only by name (settings.LINE_CONCEPTS_BY_SYMBOL).
    "intangible_capex": ("PaymentsToDevelopSoftware", "PaymentsForSoftware"),
    "diluted_shares": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
    # instants
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ),
    "securities_current": ("MarketableSecuritiesCurrent",),
    "securities_noncurrent": ("MarketableSecuritiesNoncurrent",),
    # Debt, in three shapes (see `debt`). The *AndCapitalLeaseObligations
    # variants include finance leases; 67 of the 97 non-financial filers that
    # pay interest but tagged no current LongTermDebt[Noncurrent] use them
    # (census 2026-10-11): CMCSA's whole $90B is DebtAndCapitalLeaseObligations.
    #
    # Added 2026-10-12, census of the 19 refused for interest with no debt
    # balance, each scope read from the filer's own series rather than from
    # the taxonomy label, because the two disagree. SeniorLongTermNotes and
    # UnsecuredLongTermDebt are TOTALS (VRSN's stayed $1.785B when $0.549B
    # turned current; CDNS). ConvertibleLongTermNotesPayable is labelled
    # "including the current portion" but all four filers use it for the
    # NONCURRENT part: AKAM's fell $3.540B -> $2.394B in 2024-06 as $1.148B
    # moved to ConvertibleNotesPayableCurrent; DXCM, DDOG and ZS likewise.
    # Last in each list, so they only fill dates no other concept covers.
    "long_term_debt": (
        "LongTermDebt",
        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
        "SeniorLongTermNotes",
        "UnsecuredLongTermDebt",
    ),
    "long_term_debt_noncurrent": (
        "LongTermDebtNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
        "ConvertibleLongTermNotesPayable",
    ),
    "long_term_debt_current": (
        "LongTermDebtCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "ConvertibleNotesPayableCurrent",
    ),
    #: Long AND short term in one figure.
    "debt_combined": ("DebtLongtermAndShorttermCombinedAmount", "DebtAndCapitalLeaseObligations"),
    #: All current debt: current maturities, short-term borrowings and paper.
    "debt_current": ("DebtCurrent",),
    "short_term_borrowings": ("ShortTermBorrowings",),
    "commercial_paper": ("CommercialPaper",),
    "equity": ("StockholdersEquity",),
    #: Noncontrolling interests on the balance sheet, in equity and in
    #: temporary (mezzanine) equity: parts, summed. A balance with no NCI
    #: income on file refuses the valuation; it is never read as none.
    "nci_balance": ("MinorityInterest",),
    "nci_redeemable_balance": ("RedeemableNoncontrollingInterestEquityCarryingAmount",),
    # Preferred stock (decision 18), three shapes of one claim: the issued
    # carrying value, the outstanding value, the liquidation preference (MCHP
    # and NRG tag only that). The largest at the newest balance date is used:
    # some filers carry preferred at par. Temporary equity attributable to
    # the parent is not on the list: J's was redeemable NCI.
    "preferred_stock": ("PreferredStockValue",),
    "preferred_outstanding": ("PreferredStockValueOutstanding",),
    "preferred_liquidation": ("PreferredStockLiquidationPreferenceValue",),
    #: Noted, never added: its dividends are already inside CFO.
    "equity_method_investments": ("EquityMethodInvestments",),
    #: Unfunded pensions and retiree benefits (decision 19), noted, never
    #: subtracted. Balance-sheet lines, so each is already net of its own
    #: plans' assets; a combined pension-and-OPEB line, else the two parts.
    "pension_liability": (
        "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent",
        "PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent",
    ),
    "pension_liability_pension": ("DefinedBenefitPensionPlanLiabilitiesNoncurrent",),
    "pension_liability_opeb": ("OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent",),
    "pension_liability_current": (
        "PensionAndOtherPostretirementDefinedBenefitPlansCurrentLiabilities",
        "DefinedBenefitPensionPlanLiabilitiesCurrent",
    ),
    #: Overfunded plans' surplus, an asset: IBM's $7,544M beside $9,018M owed.
    "pension_assets": ("DefinedBenefitPlanAssetsForPlanBenefitsNoncurrent",),
    "shares_outstanding": ("EntityCommonStockSharesOutstanding",),
}
INSTANTS = {
    "cash",
    "securities_current",
    "securities_noncurrent",
    "long_term_debt",
    "long_term_debt_noncurrent",
    "long_term_debt_current",
    "debt_combined",
    "debt_current",
    "short_term_borrowings",
    "commercial_paper",
    "equity",
    "nci_balance",
    "nci_redeemable_balance",
    "preferred_stock",
    "preferred_outstanding",
    "preferred_liquidation",
    "equity_method_investments",
    "pension_liability",
    "pension_liability_pension",
    "pension_liability_opeb",
    "pension_liability_current",
    "pension_assets",
    "shares_outstanding",
}
SHARE_ITEMS = {"diluted_shares", "shares_outstanding"}
#: Annual reports: pension balances are read only at a date one of these
#: states (see Statements.pension_deficit).
ANNUAL_FORMS = {"10-K", "10-K/A", "10-KT", "10-KT/A", "20-F", "20-F/A", "40-F", "40-F/A"}
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
    def __init__(
        self,
        facts: Iterable[Fact],
        splits: Sequence[Tuple[date, float]] = (),
        line_concepts: Optional[Dict[str, Tuple[str, ...]]] = None,
    ) -> None:
        """
        line_concepts: for this filer only, a line read as the SUM of these
        concepts per period instead of the first of LINE_ITEMS' list
        (settings.LINE_CONCEPTS_BY_SYMBOL, each entry checked against the
        filer's statement). A period missing any part is dropped:
        a missing part is not zero.
        """
        self.by_concept: Dict[str, List[Fact]] = {}
        for f in facts:
            self.by_concept.setdefault(f.concept, []).append(f)
        self.splits = sorted(splits)
        self.line_concepts = dict(line_concepts or {})

    # -- primitives -------------------------------------------------------

    def _scale(self, item: str, f: Fact, as_of: date) -> float:
        if item not in SHARE_ITEMS:
            return f.value
        factor = 1.0
        for ex_date, ratio in self.splits:
            if f.filed < ex_date <= as_of:
                factor *= ratio
        return f.value * factor

    def _periods(
        self,
        item: str,
        as_of: date,
        span: Optional[Tuple[int, int]],
        concepts: Optional[Tuple[str, ...]] = None,
    ) -> Dict[Tuple, Value]:
        if concepts is None and item in self.line_concepts:
            return self._summed(item, as_of, span, self.line_concepts[item])
        """
        {(start, end): Value} — per period, first concept with a visible row,
        latest filed.

        A lower-priority concept may not add a period whose END a higher one
        already covers. Within ONE concept every period is kept: a 10-Q files
        the quarter and the year to date with the same end and the same filing
        date, and both are needed. Until 2026-10-12 the end check ran within
        the concept too, so which of the two survived was the order the
        database returned them in, and DAL's TTM capex was $4.475B or $1.534B
        depending on which valuations had run before it in the process.
        """
        out: Dict[Tuple, Value] = {}
        for concept in concepts if concepts is not None else LINE_ITEMS[item]:
            higher_ends = {k[1] for k in out}
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
                if (
                    key not in best
                    or f.filed > best[key].filed
                    or (f.filed == best[key].filed and f.accn > best[key].accn)
                ):
                    best[key] = f
            for key, f in best.items():
                if key in out:
                    continue  # a higher-priority concept already covers this period
                if span is not None and key[1] in higher_ends:
                    continue  # same period end from a higher-priority concept, slightly different start
                out[key] = Value(
                    f.period_start, f.period_end, self._scale(item, f, as_of), f.concept, f.filed
                )
        return out

    def _summed(
        self, item: str, as_of: date, span: Optional[Tuple[int, int]], parts: Tuple[str, ...]
    ) -> Dict[Tuple, Value]:
        """Per period, the sum of every part; a period any part lacks is left out."""
        series = [self._periods(item, as_of, span, (c,)) for c in parts]
        keys = set(series[0]).intersection(*series[1:])
        return {
            k: Value(
                k[0],
                k[1],
                sum(s[k].value for s in series),
                " + ".join(parts),
                max(s[k].filed for s in series),
            )
            for k in sorted(keys, key=lambda k: (k[1], k[0] or date.min))
        }

    def _concept_series(
        self, item: str, as_of: date, span: Tuple[int, int]
    ) -> Dict[str, List[Value]]:
        """{concept: its own periods}: one entry, the summed line, for a mapped filer."""
        if item in self.line_concepts:
            values = list(self._periods(item, as_of, span).values())
            return {" + ".join(self.line_concepts[item]): values}
        return {c: list(self._periods(item, as_of, span, (c,)).values()) for c in LINE_ITEMS[item]}

    # -- public ------------------------------------------------------------

    def annual(self, item: str, as_of: date) -> List[Value]:
        return sorted(self._periods(item, as_of, ANNUAL_DAYS).values(), key=lambda v: v.period_end)

    def annual_before(self, item: str, latest: Value, years: int, as_of: date) -> Optional[Value]:
        """
        The fiscal year ending `years` years before `latest`, on its basis.

        By date, not by position in annual(): EMR and AMCR file off-cycle
        twelve-month periods beside their fiscal years, and EXE has no 2021.
        From latest's own concept if it reports that year; otherwise from
        another concept only if the two agree (0.5%) on every period both
        report, which is a rename: ADM's FY2020 Revenues ($64.4B, the total)
        set against its FY2025 contract revenue ($25.0B, a part) read as a
        17% yearly decline.
        """
        target = latest.period_end.toordinal() - round(365.25 * years)
        series = self._concept_series(item, as_of, ANNUAL_DAYS)
        own = {(v.period_start, v.period_end): v.value for v in series.get(latest.concept, [])}

        def near(values: List[Value]) -> Optional[Value]:
            hits = [v for v in values if abs(v.period_end.toordinal() - target) <= 30]
            return min(hits, key=lambda v: abs(v.period_end.toordinal() - target), default=None)

        hit = near(series.get(latest.concept, []))
        if hit is not None:
            return hit
        for concept, values in series.items():
            if concept == latest.concept or (hit := near(values)) is None:
                continue
            common = [
                (own[k], v.value) for v in values if (k := (v.period_start, v.period_end)) in own
            ]
            if all(math.isclose(a, b, rel_tol=0.005) for a, b in common):
                return hit
        return None

    def quarterly(self, item: str, as_of: date) -> List[Value]:
        """Filed quarters plus each fiscal year's derived Q4 (FY minus Q1-Q3)."""
        quarters = self._periods(item, as_of, QUARTER_DAYS)
        out = list(quarters.values())
        if item in SHARE_ITEMS:
            return sorted(out, key=lambda v: v.period_end)  # averages do not subtract
        for fy in self.annual(item, as_of):
            inside = [
                q
                for q in quarters.values()
                if q.period_start >= fy.period_start and q.period_end <= fy.period_end
            ]
            if len(inside) != 3 or any(q.period_end == fy.period_end for q in quarters.values()):
                continue
            last = max(q.period_end for q in inside)
            out.append(
                Value(
                    date.fromordinal(last.toordinal() + 1),
                    fy.period_end,
                    fy.value - sum(q.value for q in inside),
                    fy.concept,
                    fy.filed,
                    derived=True,
                )
            )
        return sorted(out, key=lambda v: v.period_end)

    def discrete_quarters(self, item: str, as_of: date) -> List[Value]:
        """
        Each fiscal quarter of a FLOW line as its own figure, for display (the
        Research page). 10-Qs file cash flows year-to-date only, so Q2 = H1 -
        Q1, Q3 = 9M - H1 and Q4 = FY - 9M, derived=True; the year in progress
        gets the quarters filed so far. One concept per year's chain, by the
        _ttm_ytd rule: a 10-Q concept that also reports the fiscal year is a
        separate line (DAL's two capex lines) and is not chained with it; a
        step that is not one quarter long (a missing YTD) ends the chain rather
        than labelling six months as a quarter.

        `quarterly()` is NOT this: it feeds the TTM, and is left alone.
        """
        annual = self.annual(item, as_of)
        if not annual:
            return []
        starts = [fy.period_start for fy in annual]
        starts.append(date.fromordinal(annual[-1].period_end.toordinal() + 1))
        by_end = {fy.period_start: fy for fy in annual}
        out: List[Value] = []
        series = self._concept_series(item, as_of, (80, 300))
        for start in starts:
            fy = by_end.get(start)
            for concept, values in series.items():
                ytds = sorted(
                    (
                        v
                        for v in values
                        if v.period_start is not None
                        and abs((v.period_start - start).days) <= 1
                        and (fy is None or v.period_end < fy.period_end)
                    ),
                    key=lambda v: v.period_end,
                )
                if not ytds:
                    continue
                # The year in progress has no annual yet: judge the concept by
                # the latest fiscal year, or DAL's ground-equipment line would
                # stand in for its capex the year it stopped tagging aircraft.
                reference = fy or annual[-1]
                if concept != reference.concept:
                    if self._reports_period(item, concept, reference, as_of):
                        continue  # a separate line, not this one under another name
                prev: Optional[Value] = None
                for y in ytds:
                    begin = (
                        y.period_start
                        if prev is None
                        else date.fromordinal(prev.period_end.toordinal() + 1)
                    )
                    if not QUARTER_DAYS[0] <= (y.period_end - begin).days <= QUARTER_DAYS[1]:
                        break
                    value = y.value if prev is None else y.value - prev.value
                    out.append(
                        Value(begin, y.period_end, value, concept, y.filed, prev is not None)
                    )
                    prev = y
                if fy is not None and prev is not None:
                    begin = date.fromordinal(prev.period_end.toordinal() + 1)
                    if QUARTER_DAYS[0] <= (fy.period_end - begin).days <= QUARTER_DAYS[1]:
                        out.append(
                            Value(
                                begin,
                                fy.period_end,
                                fy.value - prev.value,
                                fy.concept,
                                fy.filed,
                                True,
                            )
                        )
                break
        return sorted(out, key=lambda v: v.period_end)

    def _ttm_quarters(self, item: str, as_of: date) -> Optional[Tuple[float, List[Value]]]:
        qs = self.quarterly(item, as_of)[-4:]
        if len(qs) < 4:
            return None
        for a, b in zip(qs, qs[1:], strict=False):
            if (b.period_start - a.period_end).days > 7:
                return None
        return sum(q.value for q in qs), qs

    def _ttm_ytd(self, item: str, as_of: date) -> Optional[Tuple[float, List[Value]]]:
        """
        FY + YTD(current) - YTD(prior year, same span): [prior, FY, current].

        The current and prior YTD come from one concept, the one the latest
        10-Q used. Many filers tag a line one way in the 10-K and another in
        the 10-Qs (APD's operating cash flow, WEC's capex), and those mix
        freely. But a 10-Q concept that ALSO reports the same fiscal year is a
        separate line, not another name for this one, and mixes only if its
        prior-year YTD equals the fiscal-year concept's (a rename re-files the
        same comparative). Otherwise an older YTD is used.

        DAL is why: it files capex as PaymentsToAcquireProductiveAssets
        (aircraft, FY2025 $4.499B) beside PaymentsToAcquireOtherProductiveAssets
        (ground equipment, FY2025 $0.978B) and in 2026 stopped tagging the
        first in its 10-Qs. Mixing them gave FY $4.499B + 9M $0.627B - prior
        9M $3.592B = $1.534B a year; their prior 9Ms ($0.680B vs $3.592B)
        show they are parts.
        """
        annual = self.annual(item, as_of)
        if not annual:
            return None
        fy = annual[-1]
        span = (80, 300)

        def prior_of(values: List[Value], cur: Value) -> Optional[Value]:
            target = cur.period_end.toordinal() - 364
            for v in values:
                if (
                    v.period_start == fy.period_start
                    and abs(v.period_end.toordinal() - target) <= 7
                ):
                    return v
            return None

        by_concept = self._concept_series(item, as_of, span)
        current = sorted(
            (
                v
                for values in by_concept.values()
                for v in values
                if v.period_start is not None and (v.period_start - fy.period_end).days in (0, 1)
            ),
            key=lambda v: (v.period_end, v.concept == fy.concept),
            reverse=True,
        )
        for cur in current:
            pri = prior_of(by_concept[cur.concept], cur)
            if pri is None:
                continue
            if cur.concept != fy.concept and self._reports_period(item, cur.concept, fy, as_of):
                pri_fy = prior_of(by_concept[fy.concept], cur)
                same_prior = pri_fy is not None and math.isclose(
                    pri.value, pri_fy.value, rel_tol=0.005
                )
                # Or the two state the same fiscal year: DHR, LH and ROP tag
                # continuing-operations CFO in the 10-K and only the total
                # in the 10-Qs, equal in a year with no discontinued ones.
                fy_cur = self._periods(item, as_of, ANNUAL_DAYS, (cur.concept,))[
                    (fy.period_start, fy.period_end)
                ]
                if not same_prior and not math.isclose(fy_cur.value, fy.value, rel_tol=0.005):
                    continue
            return fy.value + cur.value - pri.value, [pri, fy, cur]
        return None

    def _reports_period(self, item: str, concept: str, period: Value, as_of: date) -> bool:
        """
        Whether `concept` alone has a value for `period`'s exact span. Exact,
        not "same end": both lines come from the same 10-K, which states one
        fiscal year with one start date.
        """
        key = (period.period_start, period.period_end)
        return key in self._periods(item, as_of, ANNUAL_DAYS, (concept,))

    def ttm(self, item: str, as_of: date) -> Optional[Tuple[float, List[Value]]]:
        """
        Trailing twelve months ending at the latest visible period: four
        contiguous quarters, or FY + YTD - prior YTD, whichever ends later.
        The last Value in the list is the period the TTM ends with.
        """
        options = [
            o
            for o in (self._ttm_quarters(item, as_of), self._ttm_ytd(item, as_of))
            if o is not None
        ]
        if not options:
            return None
        return max(options, key=lambda o: o[1][-1].period_end)

    def balances(self, item: str, as_of: date) -> List[Value]:
        """Every balance-sheet date of an instant line visible at `as_of`, oldest first."""
        return sorted(self._periods(item, as_of, None).values(), key=lambda v: v.period_end)

    def instant(self, item: str, as_of: date) -> Optional[Value]:
        """The latest balance as of the latest period end visible at `as_of`."""
        values = self._periods(item, as_of, None)
        if not values:
            return None
        return max(values.values(), key=lambda v: (v.period_end, v.filed))

    def at_period(self, item: str, period_end: date, as_of: date) -> Optional[Value]:
        """One period's value as known at `as_of` (an instant, or an annual or quarterly duration)."""
        spans = (None,) if item in INSTANTS else (ANNUAL_DAYS, QUARTER_DAYS)
        candidates = [
            v
            for sp in spans
            for v in self._periods(item, as_of, sp).values()
            if v.period_end == period_end
        ]
        return max(candidates, key=lambda v: v.period_start or date.min) if candidates else None

    # -- derived -------------------------------------------------------------

    def free_cash_flow_annual(self, as_of: date) -> List[Tuple[date, float]]:
        cfo = {v.period_end: v.value for v in self.annual("cfo", as_of)}
        capex = {v.period_end: v.value for v in self.annual("capex", as_of)}
        return [(end, cfo[end] - capex[end]) for end in sorted(cfo) if end in capex]

    def _instant_at(self, item: str, end: date, as_of: date) -> Optional[Value]:
        return self._periods(item, as_of, None).get((None, end))

    def debt(self, as_of: date) -> Optional[Value]:
        """
        Total debt at the NEWEST balance date any of three shapes reaches,
        the largest shape at that date:
        a combined long-and-short figure; total long-term debt plus short-term
        borrowings (or commercial paper, never both: paper is usually inside
        short-term borrowings); noncurrent plus current. Every part comes from
        that one balance date. `concept` names the parts used.

        Taking the newest date matters: LDOS last tagged LongTermDebt for
        2015-10-02 ($1.1B) and now tags the capital-lease variants ($6.0B);
        VZ's combined figure is a year older than its noncurrent + current.
        """

        def short(end: date) -> Tuple[float, List[Value]]:
            v = self._instant_at("short_term_borrowings", end, as_of) or self._instant_at(
                "commercial_paper", end, as_of
            )
            return (v.value, [v]) if v else (0.0, [])

        options: List[Tuple[date, int, float, List[Value]]] = []
        combined = self.instant("debt_combined", as_of)
        if combined:
            options.append((combined.period_end, 0, combined.value, [combined]))
        total = self.instant("long_term_debt", as_of)
        if total:
            s, parts = short(total.period_end)
            options.append((total.period_end, 1, total.value + s, [total, *parts]))
        nc = self.instant("long_term_debt_noncurrent", as_of)
        if nc:
            cur_all = self._instant_at("debt_current", nc.period_end, as_of)
            if cur_all:
                s, parts = cur_all.value, [cur_all]
            else:
                cur = self._instant_at("long_term_debt_current", nc.period_end, as_of)
                s, parts = short(nc.period_end)
                if cur:
                    s, parts = s + cur.value, [cur, *parts]
            options.append((nc.period_end, 2, nc.value + s, [nc, *parts]))
        if not options:
            return None
        # At one balance date the LARGEST shape wins: a filer may tag one shape
        # for a component only (ON's DebtAndCapitalLeaseObligations is $1.6M of
        # finance leases beside $4.5B LongTermDebt), and no shape can exceed the
        # whole. Each shape is formed alone, so taking the largest double-counts nothing.
        end, _, value, parts = max(options, key=lambda o: (o[0], o[2], -o[1]))
        return Value(
            None, end, value, " + ".join(p.concept for p in parts), max(p.filed for p in parts)
        )

    def total_debt(self, as_of: date) -> Optional[float]:
        d = self.debt(as_of)
        return d.value if d else None

    def preferred_stock(self, as_of: date) -> Optional[Value]:
        """
        Preferred stock at the newest balance date any of its three shapes
        reports, the LARGEST shape at that date: a filer that carries
        preferred at par files a value far below its liquidation preference.
        """
        shapes = ("preferred_stock", "preferred_outstanding", "preferred_liquidation")
        latest = [v for v in (self.instant(item, as_of) for item in shapes) if v is not None]
        if not latest:
            return None
        end = max(v.period_end for v in latest)
        at_end = [
            v for v in (self._instant_at(item, end, as_of) for item in shapes) if v is not None
        ]
        return max(at_end, key=lambda v: v.value)

    def pension_deficit(self, as_of: date) -> Optional[Value]:
        """
        Unfunded pensions and retiree benefits on the balance sheet at the
        newest date an ANNUAL report states a noncurrent line for: the
        combined line, else pension plus OPEB, plus the current portion,
        less overfunded plans' assets at that date. A 10-Q may tag the
        liability alone: TAP's 2026-06-30 shows $411M owed and none of the
        $485M surplus its 10-K nets it against. None when no line is tagged;
        that is NOT "no deficit" (filers that tag per plan have no total).
        """
        lines = ("pension_liability", "pension_liability_pension", "pension_liability_opeb")
        annual_ends = {
            f.period_end
            for item in lines
            for concept in LINE_ITEMS[item]
            for f in self.by_concept.get(concept, [])
            if f.period_start is None and f.filed < as_of and f.form in ANNUAL_FORMS
        }
        if not annual_ends:
            return None
        end = max(annual_ends)
        at = {item: self._instant_at(item, end, as_of) for item in lines}
        owed = (
            [at["pension_liability"]]
            if at["pension_liability"]
            else [v for v in (at["pension_liability_pension"], at["pension_liability_opeb"]) if v]
        )
        total, parts = sum(v.value for v in owed), [v.concept for v in owed]
        current = self._instant_at("pension_liability_current", end, as_of)
        if current:
            total, parts = total + current.value, parts + [current.concept]
        name = " + ".join(parts)
        surplus = self._instant_at("pension_assets", end, as_of)
        if surplus:
            total, name = total - surplus.value, f"{name} - {surplus.concept}"
        return Value(None, end, total, name, max(v.filed for v in owed))

    def liquid_assets(self, as_of: date) -> Optional[Value]:
        """Cash plus marketable securities at the latest cash balance date."""
        cash = self.instant("cash", as_of)
        if cash is None:
            return None
        # Marketable securities, current AND noncurrent, net against debt: both
        # are liquid investments, not operating assets (a choice, recorded in
        # the plan; Apple's noncurrent book is $77.7B on 2025-09-27).
        liquid, parts = cash.value, [cash.concept]
        for item in ("securities_current", "securities_noncurrent"):
            v = self._instant_at(item, cash.period_end, as_of)
            if v:
                liquid, parts = liquid + v.value, parts + [v.concept]
        return Value(None, cash.period_end, liquid, " + ".join(parts), cash.filed)

    def net_debt(self, as_of: date) -> Optional[float]:
        debt, liquid = self.total_debt(as_of), self.liquid_assets(as_of)
        if debt is None or liquid is None:
            return None
        return debt - liquid.value

    def effective_tax_rate(self, as_of: date) -> Optional[float]:
        tax, pre = self.annual("income_tax", as_of), self.annual("pretax_income", as_of)
        if not tax or not pre or tax[-1].period_end != pre[-1].period_end or pre[-1].value == 0:
            return None
        return tax[-1].value / pre[-1].value
