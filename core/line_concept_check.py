"""
core/line_concept_check.py

Which DCF line a filer's statements may be reading wrong: the pure half of
scripts/check_line_concepts.py (the script values; this decides).

The DCF reads each line from the first concept of an ordered list, or from
settings.LINE_CONCEPTS_BY_SYMBOL where a filer's 10-K showed the list picks
wrong. Both are claims about filings that keep arriving. A filer can start
tagging a PART of a line under the concept ranked first (CHTR's $889M of
"revenue" beside its $54,774M total), or stop filing a concept its map names.
Found by hand on 2026-10-12/13; this is the standing check.

Only the lines whose concept choice can move a value are checked. D&A feeds
only the exit-multiple cross-check, debt already takes the largest shape, and
every cash alternative is restricted cash, which is correctly left out.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

from config import settings
from modeling.statements import ANNUAL_DAYS, LINE_ITEMS, Statements, Value

#: line item -> the valuation `lines` entry it moves.
CHECKED_LINES: Dict[str, str] = {
    "revenue": "growth_first",
    "capex": "capex",
    "intangible_capex": "capex",
    "cfo": "cfo",
    "interest_expense": "after_tax_interest",
    "interest_paid": "after_tax_interest",
}
#: Another concept "reports the line differently" above this relative gap.
DIFFERENT_REL = 0.01


@dataclass(frozen=True)
class Alternative:
    item: str
    period_end: date
    used: str
    used_value: float
    concept: str
    value: float


def alternatives(
    st: Statements, item: str, as_of: date, min_share: float = 0.0
) -> List[Alternative]:
    """
    Concepts on the item's list (or outside its map) that report the line's
    newest fiscal year with a value more than DIFFERENT_REL from the one used
    and at least `min_share` of it. A line under a tenth of the chosen one is
    a component, not the whole (PKG's $2M beside $829M of capex), and swapping
    it in only inflates free cash flow; the fault worth finding is the chosen
    figure being the part (CHTR's $889M of revenue beside $54,774M).
    """
    annual = st.annual(item, as_of)
    if not annual:
        return []
    used = annual[-1]
    parts = set(st.line_concepts.get(item, (used.concept,)))
    key = (used.period_start, used.period_end)
    out = []
    for concept in LINE_ITEMS[item]:
        if concept in parts:
            continue
        v = st._periods(item, as_of, ANNUAL_DAYS, (concept,)).get(key)
        if (
            v is not None
            and not math.isclose(v.value, used.value, rel_tol=DIFFERENT_REL)
            and abs(v.value) >= min_share * abs(used.value)
        ):
            out.append(
                Alternative(item, used.period_end, used.concept, used.value, concept, v.value)
            )
    return out


def with_line(st: Statements, item: str, concepts: Tuple[str, ...]) -> Statements:
    """The same filer's statements with one line read from `concepts`."""
    facts = [f for fs in st.by_concept.values() for f in fs]
    return Statements(facts, st.splits, {**st.line_concepts, item: concepts}, st.spinoffs)


def stale_map(st: Statements, item: str, as_of: date) -> Optional[str]:
    """
    Why a mapped line is behind its unmapped list, or None. A map names
    concepts; a filer that stops using one leaves the mapped line ending
    years ago while the list reports this year (or refuses the valuation).
    """
    if item not in st.line_concepts:
        return None
    mapped = st.annual(item, as_of)
    unmapped = _unmapped(st, item, as_of)
    if not unmapped:
        return None
    if not mapped:
        return f"mapped {item} has no fiscal year; the list reports FY {unmapped[-1].period_end}"
    if mapped[-1].period_end < unmapped[-1].period_end:
        return (
            f"mapped {item} ends FY {mapped[-1].period_end}; the list reports "
            f"FY {unmapped[-1].period_end} ({unmapped[-1].concept})"
        )
    return None


def _unmapped(st: Statements, item: str, as_of: date) -> List[Value]:
    facts = [f for fs in st.by_concept.values() for f in fs]
    rest = {k: v for k, v in st.line_concepts.items() if k != item}
    return Statements(facts, st.splits, rest, st.spinoffs).annual(item, as_of)


def unreviewed_splits(symbol: str, splits: Sequence[Tuple[date, float]]) -> List[str]:
    """
    Split rows with an odd ratio (settings.odd_split_ratio) not yet read into
    settings.SPLIT_ROWS_REVIEWED: probably a spin-off Yahoo serves as a
    split, which the DCF multiplies share counts by until it is listed.
    """
    return [
        f"SPLIT? {symbol} {ex_date} ratio {ratio:g}: not in SPLIT_ROWS_REVIEWED; read "
        "it (a spin-off, or a real share change) and list it"
        for ex_date, ratio in splits
        if settings.odd_split_ratio(ratio)
        and (symbol.upper(), ex_date.isoformat()) not in settings.SPLIT_ROWS_REVIEWED
    ]


def material(base: float, alt: float, min_move: float, min_rel: float) -> bool:
    """A value/price move worth a human reading the filing."""
    move = abs(alt - base)
    return move >= min_move and move >= min_rel * abs(base)


def same_basis(base_source: str, alt_source: str) -> bool:
    """
    Both lines trailing (TTM) or both fiscal-year. An alternative filed only
    in 10-Ks falls back from a TTM to the fiscal year, and the value then
    moves with the WINDOW, not the concept: LYV and KR on 2026-10-13.
    """

    def kind(source: str) -> str:
        # Growth has no window ("lower revenue CAGR ... to FY ..."): comparable.
        return "TTM" if "TTM to" in source else "FY" if "FY to" in source else "-"

    return kind(base_source) == kind(alt_source)
