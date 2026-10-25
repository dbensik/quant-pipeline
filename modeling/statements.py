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
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
    # Read only as witnesses to a share count's scale (decision 62).
    "basic_shares": ("WeightedAverageNumberOfSharesOutstandingBasic",),
    "diluted_eps": ("EarningsPerShareDiluted",),
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
SHARE_ITEMS = {"diluted_shares", "basic_shares", "shares_outstanding"}
#: Lines read only as witnesses to a share count's unit (decision 62).
WITNESS_ITEMS = {"basic_shares", "diluted_eps"}
#: Annual reports: pension balances are read only at a date one of these
#: states (see Statements.pension_deficit).
ANNUAL_FORMS = {"10-K", "10-K/A", "10-KT", "10-KT/A", "20-F", "20-F/A", "40-F", "40-F/A"}
ANNUAL_DAYS = (350, 380)
QUARTER_DAYS = (80, 100)
#: The cover-page count, and how far past its filing its date may fall
#: (decision 61; see Statements.__init__).
COVER_CONCEPT = "EntityCommonStockSharesOutstanding"
COVER_DATE_GRACE_DAYS = 31
#: Capex concepts that name a payment, an outflow: one filed below zero is
#: in the wrong sign and set aside (decision 67; see Statements.__init__).
#: PaymentsForProceedsFromProductiveAssets is a net of sale proceeds.
GROSS_CAPEX_CONCEPTS = frozenset(LINE_ITEMS["capex"]) - {"PaymentsForProceedsFromProductiveAssets"}
#: A share count this many times (or this fraction of) both of its filing's
#: other witnesses, which agree within SCALE_AGREE of each other, is in the
#: wrong unit and set aside (decision 62; see Statements.__init__).
SCALE_OFF = 200.0
SCALE_AGREE = 5.0
DILUTED_CONCEPT = "WeightedAverageNumberOfDilutedSharesOutstanding"
SHARE_WITNESS_DAYS = 400
MIN_WITNESS_SHARES = 1e6
#: Diluted EPS this small is too rounded to imply a share count.
MIN_WITNESS_EPS = 0.05


def map_part(part: str) -> Tuple[str, float]:
    """
    One part of a per-filer map (settings.LINE_CONCEPTS_BY_SYMBOL): "Concept"
    adds, "-Concept" subtracts (VST's allowance resales, decision 23).
    """
    return (part[1:], -1.0) if part.startswith("-") else (part, 1.0)


def map_label(parts: Tuple[str, ...]) -> str:
    """'A + B - C' for a mapped line's source."""
    out = ""
    for c, sign in map(map_part, parts):
        out += (c if not out else f" + {c}") if sign > 0 else (f"-{c}" if not out else f" - {c}")
    return out


class FirstOf(tuple):
    """
    A per-filer ORDER for a line, not a sum (settings.LINE_CONCEPTS_FIRST_BY_SYMBOL,
    decision 64): these concepts are read ahead of the item's list, the rest
    of the list after them, per period as for any filer. ETR tags its
    "Construction/capital expenditures" as PaymentsForCapitalImprovements and
    a smaller line ("Payment for purchase of plant") under a concept the list
    ranks first; from FY2022 it tags capex under the list's first concept. A
    sum would drop those years, and the list alone read $0 for FY2009.
    """


class SumWhereFiled(tuple):
    """
    A per-filer SUM read for every period all its parts report, and the
    item's list for the others (settings.LINE_CONCEPTS_SUM_WHERE_FILED_BY_SYMBOL,
    decision 71). CSGP's statement has two purchase lines from its FY2021
    10-K: the campus purchases (PaymentsToAcquirePropertyPlantAndEquipment)
    and the ordinary ones (its own concept). A plain sum drops every year
    before that 10-K; an order reads one line of the two.
    """


def _far(a: float, b: float) -> bool:
    return a >= b * SCALE_OFF or a * SCALE_OFF <= b


def _median(xs: List[float]) -> float:
    xs = sorted(xs)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def _share_witnesses(by_concept: Dict[str, List[Fact]]) -> Dict[str, Dict[str, float]]:
    """
    Per filing (accession): the median positive cover count, the median
    positive weighted average (basic and diluted together), and the median
    net income / diluted EPS over the periods it reports both.
    """
    pools: Dict[str, Dict[str, List[float]]] = {}

    def add(accn: str, kind: str, v: float) -> None:
        pools.setdefault(accn, {}).setdefault(kind, []).append(v)

    for f in by_concept.get(COVER_CONCEPT, []):
        if f.value > 0:
            add(f.accn, "cover", f.value)
    for c in (DILUTED_CONCEPT, "WeightedAverageNumberOfSharesOutstandingBasic"):
        for f in by_concept.get(c, []):
            if f.value > 0:
                add(f.accn, "averages", f.value)
    eps = {
        (f.accn, f.period_start, f.period_end): f.value
        for f in by_concept.get("EarningsPerShareDiluted", [])
        if abs(f.value) >= MIN_WITNESS_EPS
    }
    for f in by_concept.get("NetIncomeLoss", []):
        e = eps.get((f.accn, f.period_start, f.period_end))
        if e is not None and f.value / e > 0:
            add(f.accn, "earnings", f.value / e)
    return {a: {k: _median(vs) for k, vs in kinds.items()} for a, kinds in pools.items()}


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
        spinoffs: Sequence[date] = (),
        reviewed_aside: Optional[Dict[Tuple[str, str], str]] = None,
    ) -> None:
        """
        splits: (ex-date, share ratio), spin-offs already taken out
        (core/valuation_service.share_splits): a spin leaves the share count
        alone. spinoffs: their ex-dates, for the valuation's refusal.

        reviewed_aside: (concept, accession) -> what the filing's statement
        shows, for this filer only (settings.FACTS_SET_ASIDE_BY_SYMBOL,
        decision 66): a concept the filing used for another line. Its facts
        from that filing are not read.

        line_concepts: for this filer only, a line read as the SUM of these
        concepts per period instead of the first of LINE_ITEMS' list
        (settings.LINE_CONCEPTS_BY_SYMBOL, each entry checked against the
        filer's statement). A period missing any part is dropped:
        a missing part is not zero. A FirstOf entry is an order instead
        (settings.LINE_CONCEPTS_FIRST_BY_SYMBOL); a SumWhereFiled entry is
        the sum where every part reports, else the list.
        """
        self.by_concept: Dict[str, List[Fact]] = {}
        # A period cannot end after the filing that reports it (decision 61):
        # ROP's Q1 and Q2 2015 10-Qs date their 2014-12-31 cash "2015-12-31",
        # which then read as the newest balance. Such a fact is set aside,
        # never moved to another date; dividends declared ahead are the only
        # real case, and no line here reads them. A cover count is dated by
        # the filer's "latest practicable date", which 22 of the 24 such rows
        # put 1-19 days past the filing (NEM's 10-K filed 2011-02-24 says
        # 2011-02-28): a real count, kept. ADBE's 144 and CL's 309 days are not.
        self.dated_after_filing: List[Fact] = []
        # EQT's 10-Ks filed 2015 and 2016 tag "Capital expenditures for
        # acquisitions" ($174M FY2014) as PaymentsToAcquirePropertyPlantAnd-
        # Equipment, which every other EQT filing uses for capex ($2,277M
        # FY2014 in the 10-K filed 2017). No concept order serves both.
        self.reviewed_aside_reasons = dict(reviewed_aside or {})
        self.reviewed_aside: List[Fact] = []
        # A payment filed below zero (decision 67): CSX's 10-K filed 2010
        # tags FY2009 capex -1,447 ($M), which the next two 10-Ks file as
        # 1,426-1,427; so do GWW, HON, OMC, LUV, WBA, EMR, PCG, WMB and CNP
        # in 2009-2011. Read, it ADDED to free cash flow. Set aside, never
        # flipped: at the time nothing on file says the sign was the error
        # (CVX files one capex concept negative in three 10-Ks running).
        self.filed_negative: List[Fact] = []
        for f in facts:
            if (f.concept, f.accn) in self.reviewed_aside_reasons:
                self.reviewed_aside.append(f)
                continue
            if f.concept in GROSS_CAPEX_CONCEPTS and f.period_start is not None and f.value < 0:
                self.filed_negative.append(f)
                continue
            grace = COVER_DATE_GRACE_DAYS if f.concept == COVER_CONCEPT else 0
            if (f.period_end - f.filed).days > grace:
                self.dated_after_filing.append(f)
                continue
            self.by_concept.setdefault(f.concept, []).append(f)
        self.splits = sorted(splits)
        # A share count in the wrong unit (decision 62): ON's covers for 2012
        # say 455 trillion shares, CRM's 2011 cover 134 (millions), COP's
        # diluted averages are in thousands for a decade, MCD's in millions.
        # A count is checked against witnesses in its own split basis: those
        # filed WITH it (the cover page; the weighted averages, basic and
        # diluted together, as they share a table and its unit; net income
        # over diluted EPS) and the newest count of its kind kept from an
        # earlier filing within SHARE_WITNESS_DAYS. Off all of them by
        # SCALE_OFF while they agree, at least two, it is set aside, never
        # rescaled; otherwise kept (it cannot be told). A witness under
        # MIN_WITNESS_SHARES condemns nothing: the averages and EPS share a
        # table, so when BOTH are in millions or thousands they agree against
        # a right cover (ATO's 2011 10-K: 90.6M on the cover, 91 thousand in
        # the averages and from earnings; LUV, COHR, SW), and a shell's count
        # is real but not the company's (Linde plc's 25,000 before its 551M,
        # SW's 100 before its 519M). ECHO files no cover and tags earnings
        # rarely: its 10-Qs' averages in thousands (271,519) have one
        # witness, the 10-K before, and are kept.
        # A count under MIN_WITNESS_SHARES is itself set aside (decision 63):
        # no registry filer has so few shares (NVR, the fewest, ~3M), so it is
        # a shell's (Fox's 1 share from its 2019 registration, Linde plc's
        # 25,000), a subsidiary's (Apache's 1,000, owned by APA) or a table in
        # thousands (ECHO's 271,519; ATO's 2011 averages).
        self.off_scale: List[Fact] = []
        self.too_small: List[Fact] = []
        witnesses = _share_witnesses(self.by_concept)
        for concept, item, own in (
            (DILUTED_CONCEPT, "diluted_shares", "averages"),
            (COVER_CONCEPT, "shares_outstanding", "cover"),
        ):
            kept: List[Fact] = []
            # Some filers tag an average as an instant (CZR, GEV, MTCH).
            in_filing_order = sorted(
                self.by_concept.get(concept, []),
                key=lambda f: (f.filed, f.accn, f.period_end, f.period_start or date.min),
            )
            for f in in_filing_order:
                if 0 < f.value < MIN_WITNESS_SHARES:
                    self.too_small.append(f)
                    continue
                earlier = [
                    g
                    for g in kept
                    if g.value > 0 and 0 < (f.filed - g.filed).days <= SHARE_WITNESS_DAYS
                ]
                prior = max(earlier, key=lambda g: (g.filed, g.period_end), default=None)
                others = [v for k, v in witnesses.get(f.accn, {}).items() if k != own]
                if prior is not None:
                    others.append(self._scale(item, prior, f.filed))
                others = [v for v in others if v >= MIN_WITNESS_SHARES]
                if (
                    f.value > 0
                    and len(others) >= 2
                    and max(others) / min(others) <= SCALE_AGREE
                    and all(_far(f.value, v) for v in others)
                ):
                    self.off_scale.append(f)
                else:
                    kept.append(f)
            if concept in self.by_concept:
                self.by_concept[concept] = kept
        self.line_concepts = dict(line_concepts or {})
        self.spinoffs = sorted(spinoffs)

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
        if concepts is None and self._is_sum(item):
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
        mapped = self.line_concepts.get(item)
        # A SumWhereFiled sum is read under its label, ranked as a concept.
        label = map_label(mapped) if isinstance(mapped, SumWhereFiled) else None
        order = concepts if concepts is not None else self._order(item)
        return self._first_per_period(
            [
                self._summed(item, as_of, span, mapped)
                if c == label
                else self._one(item, as_of, span, c)
                for c in order
            ],
            span,
        )

    @staticmethod
    def _first_per_period(
        ranked: List[Dict[Tuple, Value]], span: Optional[Tuple[int, int]]
    ) -> Dict[Tuple, Value]:
        """Per period, the first series in `ranked` that has it."""
        out: Dict[Tuple, Value] = {}
        for series in ranked:
            higher_ends = {k[1] for k in out}
            for key, v in series.items():
                if key in out:
                    continue  # a higher-priority concept already covers this period
                if span is not None and key[1] in higher_ends:
                    continue  # same period end from a higher-priority concept, slightly different start
                out[key] = v
        return out

    def _one(
        self, item: str, as_of: date, span: Optional[Tuple[int, int]], concept: str
    ) -> Dict[Tuple, Value]:
        """{(start, end): Value} of one concept: per period, the latest filed."""
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
        return {
            key: Value(
                f.period_start, f.period_end, self._scale(item, f, as_of), f.concept, f.filed
            )
            for key, f in best.items()
        }

    def _summed(
        self, item: str, as_of: date, span: Optional[Tuple[int, int]], parts: Tuple[str, ...]
    ) -> Dict[Tuple, Value]:
        """Per period, the signed sum of every part; a period any part lacks is left out."""
        signed = [map_part(c) for c in parts]
        series = [self._periods(item, as_of, span, (c,)) for c, _ in signed]
        keys = set(series[0]).intersection(*series[1:])
        return {
            k: Value(
                k[0],
                k[1],
                sum(sign * s[k].value for (_, sign), s in zip(signed, series, strict=True)),
                map_label(parts),
                max(s[k].filed for s in series),
            )
            for k in sorted(keys, key=lambda k: (k[1], k[0] or date.min))
        }

    def _concept_series(
        self, item: str, as_of: date, span: Tuple[int, int]
    ) -> Dict[str, List[Value]]:
        """{concept: its own periods}: one entry, the summed line, for a mapped filer."""
        if self._is_sum(item):
            values = list(self._periods(item, as_of, span).values())
            return {map_label(self.line_concepts[item]): values}
        return {c: list(self._periods(item, as_of, span, (c,)).values()) for c in self._order(item)}

    def _is_sum(self, item: str) -> bool:
        return item in self.line_concepts and not isinstance(
            self.line_concepts[item], (FirstOf, SumWhereFiled)
        )

    def _order(self, item: str) -> Tuple[str, ...]:
        """The item's list: for a FirstOf entry after its concepts, for a SumWhereFiled after its sum."""
        first = self.line_concepts.get(item)
        if isinstance(first, SumWhereFiled):
            return (map_label(first), *LINE_ITEMS[item])
        if not isinstance(first, FirstOf):
            return LINE_ITEMS[item]
        return (*first, *(c for c in LINE_ITEMS[item] if c not in first))

    # -- public ------------------------------------------------------------

    def concepts_read(self, item: str) -> Set[str]:
        """Every concept the item can be read from: its sum's parts, or its order."""
        mapped = self.line_concepts.get(item, ())
        if self._is_sum(item):
            return {map_part(c)[0] for c in mapped}
        if isinstance(mapped, SumWhereFiled):
            return {map_part(c)[0] for c in mapped} | set(LINE_ITEMS[item])
        return set(self._order(item))

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

        A FirstOf order (decision 65) is read per period instead, as annual()
        reads it: it was set where one concept means different lines in
        different years. HAS tags its total as Revenues from FY2016 and its
        US and Canada segment as Revenues for FY2013-15, so latest's own
        concept set $1,972M against $4,580M.
        """
        target = latest.period_end.toordinal() - round(365.25 * years)

        def near(values: List[Value]) -> Optional[Value]:
            hits = [v for v in values if abs(v.period_end.toordinal() - target) <= 30]
            return min(hits, key=lambda v: abs(v.period_end.toordinal() - target), default=None)

        if isinstance(self.line_concepts.get(item), FirstOf):
            return near(self.annual(item, as_of))
        series = self._concept_series(item, as_of, ANNUAL_DAYS)
        own = {(v.period_start, v.period_end): v.value for v in series.get(latest.concept, [])}
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
        """
        The last four contiguous quarters, or None when one of them comes from
        a separate line: a concept that also reports that quarter's fiscal
        year, at a different value. `_ttm_ytd`'s rule (decision 68,
        `research/ttm-quarter-mix-2026-10-24.md`). FIS tags property alone in
        its 10-K and Q1 10-Q (FY2022 $268M) and property plus software in its
        Q2 and Q3 10-Qs ($1,453M), so its derived Q4 went below zero and the
        year in progress read the larger line.
        """
        qs = self.quarterly(item, as_of)[-4:]
        if len(qs) < 4:
            return None
        for a, b in zip(qs, qs[1:], strict=False):
            if (b.period_start - a.period_end).days > 7:
                return None
        annual = self.annual(item, as_of)
        if any(self._separate_line(item, q, annual, as_of) for q in qs):
            return None
        return sum(q.value for q in qs), qs

    def _separate_line(self, item: str, q: Value, annual: List[Value], as_of: date) -> bool:
        """Whether quarter `q`'s concept states its fiscal year 0.5% or more off the year's own."""
        fy = next(
            (
                a
                for a in annual
                if a.period_start <= q.period_start and q.period_end <= a.period_end
            ),
            None,
        )
        if fy is None or q.concept == fy.concept:
            return False
        own = self._periods(item, as_of, ANNUAL_DAYS, (q.concept,)).get(
            (fy.period_start, fy.period_end)
        )
        return own is not None and not math.isclose(own.value, fy.value, rel_tol=0.005)

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

    def set_aside(self, as_of: date) -> List[Fact]:
        """
        Facts dated after their own filing (decision 61), visible at `as_of`,
        of a concept some line reads, that would have been that concept's
        newest of their shape: the ones whose absence a reader can notice.
        """
        read = {c for item, cs in LINE_ITEMS.items() if item not in WITNESS_ITEMS for c in cs}
        read |= {map_part(c)[0] for cs in self.line_concepts.values() for c in cs}
        return self._would_be_newest(
            [f for f in self.dated_after_filing if f.concept in read], as_of
        )

    def reviewed_aside_unreplaced(self, as_of: date) -> List[Fact]:
        """
        Facts set aside by review (decision 66), filed before `as_of`, whose
        period no kept fact of the same concept reports yet: the ones a
        reader would otherwise have had.
        """
        return self._unreplaced(self.reviewed_aside, as_of)

    def filed_negative_unreplaced(self, as_of: date) -> List[Fact]:
        """Payments filed below zero (decision 67), the same way."""
        return self._unreplaced(self.filed_negative, as_of)

    def _unreplaced(self, aside: List[Fact], as_of: date) -> List[Fact]:
        kept = {
            (g.concept, g.period_start, g.period_end)
            for fs in self.by_concept.values()
            for g in fs
            if g.filed < as_of
        }
        return sorted(
            (
                f
                for f in aside
                if f.filed < as_of and (f.concept, f.period_start, f.period_end) not in kept
            ),
            key=lambda f: (f.concept, f.period_end, f.filed),
        )

    def off_scale_newest(self, as_of: date) -> List[Fact]:
        """Share counts set aside as in the wrong unit (decision 62), the same way."""
        return self._would_be_newest(self.off_scale, as_of)

    def too_small_newest(self, as_of: date) -> List[Fact]:
        """Share counts set aside as under MIN_WITNESS_SHARES (decision 63), the same way."""
        return self._would_be_newest(self.too_small, as_of)

    def _would_be_newest(self, facts: List[Fact], as_of: date) -> List[Fact]:
        out = []
        for f in facts:
            if not f.filed < as_of:
                continue
            kept = [
                g.period_end
                for g in self.by_concept.get(f.concept, [])
                if g.filed < as_of and (g.period_start is None) == (f.period_start is None)
            ]
            if not kept or f.period_end > max(kept):
                out.append(f)
        return sorted(out, key=lambda f: (f.concept, f.period_end, f.filed))

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

    def reports_period_ending(self, day: date, as_of: date) -> bool:
        """
        Whether a 10-Q or 10-K for a period ending on or after `day` was filed
        before `as_of`. Durations only: the cover page's share count is dated
        weeks after the period (FDX's FY2026 10-K, period to 2026-05-31, dates
        it 2026-07-16, after its 2026-06-01 spin).
        """
        return any(
            f.period_start is not None
            and f.period_end >= day
            and f.filed < as_of
            and f.form.split("/")[0] in ("10-Q", "10-K")
            for facts in self.by_concept.values()
            for f in facts
        )

    def discontinued_cash(self, as_of: date) -> Optional[Tuple[float, float, Value, Value]]:
        """
        Discontinued operating cash (total CFO less continuing) in the latest
        fiscal year and in the prior-year YTD the trailing identity subtracts
        from it: (fy amount, prior amount, fy total, prior total), or None
        when either has no total. Each from its own latest filing, so a 10-Q
        that moves a business into discontinued operations after the 10-K
        shows as a prior YTD carrying more than the whole year did.

        HON on 2026-10-23 (decision 58): FY2025 $333M (Solstice only, 10-K
        of 2026-02-17), 9M 2025 $2,594M (Aerospace too, 10-Q of 2026-10-22).
        FY + YTD - prior YTD then counted Aerospace's 2025 cash as Honeywell's.

        A filer without discontinued operations tags only the total, so a
        missing continuing figure is an amount of 0. A mapped cfo line has no
        two concepts to compare: None.
        """
        if "cfo" in self.line_concepts:
            return None
        cont_c, total_c = LINE_ITEMS["cfo"]
        annual = self.annual("cfo", as_of)
        if not annual:
            return None
        fy = annual[-1]
        fy_key = (fy.period_start, fy.period_end)
        ytd = {c: self._periods("cfo", as_of, (80, 300), (c,)) for c in (cont_c, total_c)}
        ends = sorted(
            k[1]
            for c in ytd
            for k in ytd[c]
            if k[0] is not None and (k[0] - fy.period_end).days in (0, 1)
        )
        if not ends:
            return None
        target = ends[-1].toordinal() - 364
        prior_keys = [
            k
            for k in ytd[total_c]
            if k[0] == fy.period_start and abs(k[1].toordinal() - target) <= 7
        ]
        if not prior_keys:
            return None
        prior_key = min(prior_keys, key=lambda k: abs(k[1].toordinal() - target))

        def disc(total: Optional[Value], cont: Optional[Value]) -> Optional[float]:
            if total is None:
                return None
            return total.value - (cont.value if cont is not None else total.value)

        annual_by = {
            c: self._periods("cfo", as_of, ANNUAL_DAYS, (c,)).get(fy_key) for c in (cont_c, total_c)
        }
        fy_disc = disc(annual_by[total_c], annual_by[cont_c])
        prior = ytd[total_c][prior_key]
        prior_disc = disc(prior, ytd[cont_c].get(prior_key))
        if fy_disc is None or prior_disc is None:
            return None
        return fy_disc, prior_disc, annual_by[total_c], prior

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
