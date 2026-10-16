"""
core/insiders.py

SEC Form 4 insider transactions for registry issuers, insert-only (decision
40a, research/desk-pack-2026-10-16.md). The Equity Research Desk's Management
& incentives seat asks what insiders are paid to do versus what they say;
what they bought and sold is the observable half.

WHERE FILINGS ARE FOUND. Each Form 4 appears ONCE in EDGAR's daily form index,
under whichever CIK filed it — often the insider's own ("ABSHER JODY K"), not
the issuer's (checked on 2026-10-15's index: 605 Form 4 rows, none repeated).
So the index cannot map filings to issuers without opening every one. The
issuer's own submissions list does: its `recent` array names every Form 4
about it, with the accession and the XML document, back several years
(MSFT's reached 2020). One request per issuer, then one per new filing.

THE DOCUMENT. The submissions list names the RENDERED view
("xslF345X06/form4.xml"); the raw XML is the same name without the style
directory. Values sit in <value> children; a figure given only as a footnote
has no <value> and is stored as NULL, never 0 — missing is never zero, as in
the DCF.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

#: Forms stored: the report and its amendment.
FORMS = ("4", "4/A")

_STYLE_DIR = re.compile(r"^xsl[^/]*/")


@dataclass(frozen=True)
class InsiderRow:
    issuer_cik: int
    accn: str
    row_no: int
    form: str
    filed: date
    reporter_cik: Optional[int]
    reporter_name: Optional[str]
    is_director: bool
    is_officer: bool
    is_ten_pct_owner: bool
    officer_title: Optional[str]
    is_derivative: bool
    security_title: Optional[str]
    transaction_date: Optional[date]
    code: Optional[str]
    acquired_disposed: Optional[str]
    shares: Optional[float]
    price: Optional[float]
    shares_after: Optional[float]
    ownership: Optional[str]
    plan_10b5_1: Optional[bool]


def raw_document(primary_document: str) -> str:
    """The raw XML's file name, from the rendered view the submissions list names."""
    return _STYLE_DIR.sub("", primary_document)


def document_url(issuer_cik: int, accn: str, primary_document: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(issuer_cik)}/"
        f"{accn.replace('-', '')}/{raw_document(primary_document)}"
    )


def form4_filings(submissions: Dict[str, Any], since: date) -> List[Dict[str, Any]]:
    """Every Form 4 or 4/A in a submissions document filed on or after `since`."""
    recent = submissions["filings"]["recent"]
    out = []
    for form, accn, filed, doc in zip(
        recent["form"],
        recent["accessionNumber"],
        recent["filingDate"],
        recent["primaryDocument"],
        strict=True,
    ):
        if form in FORMS and date.fromisoformat(filed) >= since:
            out.append({"form": form, "accn": accn, "filed": date.fromisoformat(filed), "doc": doc})
    return out


def oldest_recent_filing(submissions: Dict[str, Any]) -> Optional[date]:
    """How far back `recent` reaches — older filings are in paged files."""
    days = submissions["filings"]["recent"]["filingDate"]
    return date.fromisoformat(min(days)) if days else None


def _text(node: Optional[ET.Element], path: str) -> Optional[str]:
    if node is None:
        return None
    found = node.find(path)
    if found is None or found.text is None:
        return None
    value = found.text.strip()
    return value or None


def _value(node: Optional[ET.Element], path: str) -> Optional[str]:
    """The <value> under `path` — absent when the filer gave only a footnote."""
    return _text(node, f"{path}/value")


def _flag(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true"}


def _number(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def _day(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def parse_form4(xml_text: str, accn: str, form: str, filed: date) -> List[InsiderRow]:
    """
    Every transaction line of one Form 4, derivative and not, in document order.
    Holdings lines (no transaction) are skipped. Several reporting owners on one
    filing (a fund and its general partner) share its lines: their names are
    joined and their roles OR'd, with the first owner's CIK.
    """
    root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)
    issuer_cik = int(_text(root, "issuer/issuerCik") or 0)
    owners = root.findall("reportingOwner")
    names = [_text(o, "reportingOwnerId/rptOwnerName") for o in owners]
    ciks = [_text(o, "reportingOwnerId/rptOwnerCik") for o in owners]
    rel = [o.find("reportingOwnerRelationship") for o in owners]
    titles = [t for t in (_text(r, "officerTitle") for r in rel) if t]
    plan_raw = _text(root, "aff10b5One")

    common = dict(
        issuer_cik=issuer_cik,
        accn=accn,
        form=form,
        filed=filed,
        reporter_cik=int(ciks[0]) if ciks and ciks[0] else None,
        reporter_name="; ".join(n for n in names if n) or None,
        is_director=any(_flag(_text(r, "isDirector")) for r in rel),
        is_officer=any(_flag(_text(r, "isOfficer")) for r in rel),
        is_ten_pct_owner=any(_flag(_text(r, "isTenPercentOwner")) for r in rel),
        officer_title="; ".join(titles) or None,
        plan_10b5_1=None if plan_raw is None else _flag(plan_raw),
    )

    rows: List[InsiderRow] = []
    for derivative, path in (
        (False, "nonDerivativeTable/nonDerivativeTransaction"),
        (True, "derivativeTable/derivativeTransaction"),
    ):
        for tx in root.findall(path):
            rows.append(
                InsiderRow(
                    **common,
                    row_no=len(rows),
                    is_derivative=derivative,
                    security_title=_value(tx, "securityTitle"),
                    transaction_date=_day(_value(tx, "transactionDate")),
                    code=_text(tx, "transactionCoding/transactionCode"),
                    acquired_disposed=_value(
                        tx, "transactionAmounts/transactionAcquiredDisposedCode"
                    ),
                    shares=_number(_value(tx, "transactionAmounts/transactionShares")),
                    price=_number(_value(tx, "transactionAmounts/transactionPricePerShare")),
                    shares_after=_number(
                        _value(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction")
                    ),
                    ownership=_value(tx, "ownershipNature/directOrIndirectOwnership"),
                )
            )
    return rows


#: Transaction codes, from the Form 4 instructions — for reading, not filtering.
CODES = {
    "P": "open-market or private purchase",
    "S": "open-market or private sale",
    "A": "grant or award",
    "M": "exercise or conversion of a derivative",
    "F": "shares withheld for tax or exercise price",
    "G": "gift",
    "C": "conversion of a derivative",
    "D": "disposition to the issuer",
    "X": "exercise of an in-the-money derivative",
}


def summarise(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """
    The discretionary trades — open-market purchases (P) and sales (S) of the
    common stock, which are the insider's own decision — counted and valued,
    split by whether they were made under a 10b5-1 plan. Grants, tax
    withholding and exercises are mechanics and are left out of the totals.
    """
    out: Dict[str, Any] = {}
    for code, label in (("P", "purchases"), ("S", "sales")):
        trades = [r for r in rows if r["code"] == code and not r["is_derivative"]]
        valued = [r for r in trades if r["shares"] is not None and r["price"] is not None]
        out[label] = {
            "transactions": len(trades),
            "insiders": len({r["reporter_name"] for r in trades}),
            "shares": sum(r["shares"] or 0 for r in trades),
            "value": sum(r["shares"] * r["price"] for r in valued),
            "unpriced": len(trades) - len(valued),
            "under_10b5_1_plan": sum(1 for r in trades if r["plan_10b5_1"]),
        }
    return out


def fetched_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Storage and refresh
# ---------------------------------------------------------------------------


async def stored_accessions(session: Any, issuer_cik: int) -> set:
    from sqlalchemy import select

    from db.models import InsiderTransactionORM

    result = await session.execute(
        select(InsiderTransactionORM.accn).where(InsiderTransactionORM.issuer_cik == issuer_cik)
    )
    return set(result.scalars().all())


async def insert_rows(session: Any, rows: List[InsiderRow]) -> int:
    """Insert-only: a line already stored (same accession, same position) is skipped."""
    if not rows:
        return 0
    from dataclasses import asdict

    from sqlalchemy.dialects.postgresql import insert

    from db.models import InsiderTransactionORM

    now = fetched_now()
    stmt = (
        insert(InsiderTransactionORM)
        .values([{**asdict(r), "fetched_at": now} for r in rows])
        .on_conflict_do_nothing(constraint="uq_insider_transaction")
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0


@dataclass
class RefreshResult:
    issuer_cik: int
    filings_seen: int
    filings_fetched: int
    rows_inserted: int
    reaches_back_to: Optional[date]
    failed: List[str]


async def refresh_issuer(gateway: Any, session: Any, issuer_cik: int, since: date) -> RefreshResult:
    """
    Fetch and store every Form 4 about `issuer_cik` filed on or after `since`
    that is not stored yet. One request for the submissions list, then one per
    new filing. A filing that fails to parse is named, not fatal.
    """
    submissions = gateway.submissions(issuer_cik)
    filings = form4_filings(submissions, since)
    have = await stored_accessions(session, issuer_cik)
    fetched, inserted, failed = 0, 0, []
    for f in filings:
        if f["accn"] in have:
            continue
        try:
            text = gateway.get_text(document_url(issuer_cik, f["accn"], f["doc"]))
            rows = parse_form4(text, f["accn"], f["form"], f["filed"])
        except Exception as exc:  # noqa: BLE001 — one bad document must not stop the issuer
            failed.append(f"{f['accn']}: {str(exc)[:80]}")
            continue
        fetched += 1
        # Stored under the issuer whose submissions list named the filing, so
        # the "already stored" check finds it next time (a document naming a
        # predecessor's CIK would otherwise be fetched again on every refresh).
        inserted += await insert_rows(session, [replace(r, issuer_cik=issuer_cik) for r in rows])
    return RefreshResult(
        issuer_cik=issuer_cik,
        filings_seen=len(filings),
        filings_fetched=fetched,
        rows_inserted=inserted,
        reaches_back_to=oldest_recent_filing(submissions),
        failed=failed,
    )
