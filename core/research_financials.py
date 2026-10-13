"""
core/research_financials.py

The Research page's three statements, read from SEC filings: the same
point-in-time statement layer the Valuation page uses (modeling/statements.py),
not yfinance's four restated periods with no filing date.

Every cell carries the concept it was read from and the day it was filed; a
figure is visible from the day AFTER its filing, as everywhere else. Columns
are the newest MAX_ANNUAL fiscal years or MAX_QUARTERS quarters, the same for
every line of a statement (the table takes its columns from the first line),
and a line with no value in any of them is left out.

Quarterly cash flows are derived from the year-to-date figures 10-Qs file
(Statements.discrete_quarters) and marked derived; so is a Q4 from the annual.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Sequence, Tuple

from modeling.statements import Statements, Value

MAX_ANNUAL = 10
MAX_QUARTERS = 8

SEC_SOURCE = (
    "SEC filings (XBRL company facts), as filed; each figure visible from the day after its filing"
)

#: (key, label) per statement, in display order.
INCOME: Sequence[Tuple[str, str]] = (
    ("revenue", "Revenue"),
    ("cost_of_revenue", "Cost of revenue"),
    ("operating_income", "Operating income"),
    ("interest_expense", "Interest expense"),
    ("pretax_income", "Pretax income"),
    ("income_tax", "Income tax"),
    ("net_income", "Net income"),
    ("diluted_shares", "Diluted shares (weighted)"),
)
BALANCE: Sequence[Tuple[str, str]] = (
    ("cash", "Cash and equivalents"),
    ("securities_current", "Marketable securities, current"),
    ("securities_noncurrent", "Marketable securities, noncurrent"),
    ("short_term_borrowings", "Short-term borrowings"),
    ("debt_current", "Debt, current"),
    ("long_term_debt_current", "Long-term debt, current portion"),
    ("long_term_debt_noncurrent", "Long-term debt, noncurrent"),
    ("long_term_debt", "Long-term debt, total"),
    ("equity", "Stockholders' equity"),
    ("shares_outstanding", "Shares outstanding (cover page)"),
)
CASH_FLOW: Sequence[Tuple[str, str]] = (
    ("cfo", "Operating cash flow"),
    ("capex", "Capital expenditure"),
    ("d_and_a", "Depreciation and amortization"),
    ("share_based_comp", "Share-based compensation"),
    ("interest_paid", "Interest paid"),
)
#: Flow lines whose 10-Q figures are year-to-date only.
YTD_ONLY = {"cfo", "capex", "d_and_a", "share_based_comp", "interest_paid"}


def _flows(st: Statements, key: str, as_of: date, quarterly: bool) -> List[Value]:
    if not quarterly:
        return st.annual(key, as_of)
    if key in YTD_ONLY:
        return st.discrete_quarters(key, as_of)
    return st.quarterly(key, as_of)


def _balances(st: Statements, key: str, as_of: date) -> List[Value]:
    return st.balances(key, as_of)


def _statement(
    st: Statements, items: Sequence[Tuple[str, str]], as_of: date, quarterly: bool, balance: bool
) -> List[Dict[str, Any]]:
    series = {
        key: {
            v.period_end.isoformat(): v
            for v in (_balances(st, key, as_of) if balance else _flows(st, key, as_of, quarterly))
        }
        for key, _ in items
    }
    if balance:
        # Balance sheets at the period ends the flow statements report, so
        # the three tables line up; a filer's other balance dates are dropped.
        ends = {
            v.period_end.isoformat()
            for key in ("revenue", "cfo", "net_income")
            for v in _flows(st, key, as_of, quarterly)
        } or {end for values in series.values() for end in values}
    else:
        ends = {end for values in series.values() for end in values}
    columns = sorted(ends, reverse=True)[: MAX_QUARTERS if quarterly else MAX_ANNUAL]
    lines = []
    for key, label in items:
        values = series[key]
        if not any(end in values for end in columns):
            continue
        lines.append(
            {
                "line_item": label,
                "values": {end: values[end].value if end in values else None for end in columns},
                "filed": {end: values[end].filed if end in values else None for end in columns},
                "concept": {end: values[end].concept if end in values else None for end in columns},
                "derived": {
                    end: values[end].derived if end in values else False for end in columns
                },
            }
        )
    return lines


#: Newest revenue or operating cash flow must end within this many days for
#: the filings to be shown; the same limits the DCF treats as unreported.
RECENT_DAYS = {False: 460, True: 200}


def unusable_reason(st: Statements, as_of: date, quarterly: bool) -> str:
    """
    Why the filings are not worth showing for this view, or "" if they are.
    Needs a RECENT revenue or operating cash flow: CCEP and GFS file IFRS,
    outside the us-gaap allow-list, so they have facts but almost no lines;
    ASML files a US GAAP 20-F but no 10-Qs, so it has years but no quarters;
    a filer that left us-gaap years ago would show only stale years.
    """
    newest = [
        v.period_end for key in ("revenue", "cfo") for v in _flows(st, key, as_of, quarterly)[-1:]
    ]
    if not newest:
        kind = "quarterly" if quarterly else "annual"
        return f"its SEC filings carry no standard {kind} revenue or cash flow"
    if (as_of - max(newest)).days > RECENT_DAYS[quarterly]:
        return f"its newest standard SEC figures end {max(newest)}"
    return ""


def build(st: Statements, symbol: str, as_of: date, quarterly: bool) -> Dict[str, Any]:
    return {
        "symbol": symbol.upper(),
        "quarterly": quarterly,
        "source": SEC_SOURCE,
        "income_statement": _statement(st, INCOME, as_of, quarterly, balance=False),
        "balance_sheet": _statement(st, BALANCE, as_of, quarterly, balance=True),
        "cash_flow": _statement(st, CASH_FLOW, as_of, quarterly, balance=False),
    }
