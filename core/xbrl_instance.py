"""
core/xbrl_instance.py

One filing's XBRL instance read into the shape of a companyfacts document,
for the filings companyfacts leaves out (decision 59,
research/sec-companyfacts-gap-2026-10-23.md). The result goes through
core.fundamentals.parse_company_facts like any companyfacts document: the
same allow-list, the same insert-only key.

WHAT COMPANYFACTS KEEPS, AND SO WHAT THIS KEEPS
    Facts of the us-gaap and dei taxonomies with a unit, on a context of
    the filer's own CIK (a successor's contexts listed under its
    predecessor's CIK, as XOM's 2026-08-03 10-Q is, are not the
    predecessor's; checked: none of the 68 filings loaded 2026-10-23 had
    another identifier) with no segment or scenario (a dimension is a part,
    never the total), not nil. Two rules, each found by comparing against companyfacts:
      - a duration that starts and ends on the same day is served as an
        INSTANT (MSFT's GoodwillImpairmentLoss "for" 2026-05-01);
      - a fact tagged twice in one filing (a rounded mention in the text and
        its table) keeps the higher `decimals` (AMZN's Depreciation 30.2B in
        the text, 30,225M in the table).
    With both, 78 stored filings parsed to exactly the rows companyfacts
    served for them, the held-out 30 holding 3,861 facts.

    `fy` and `fp` are the filing's DocumentFiscalYearFocus and
    DocumentFiscalPeriodFocus, as companyfacts labels them; `frame` is SEC's
    own calendar alignment and is absent here. Nothing reads any of the three
    (checked 2026-10-23: modeling/, core/, api/).

A FILER'S OWN CONCEPTS (decision 70 B)
    `extensions` names (prefix, concept) pairs of the filer's extension
    taxonomy to keep as well, under taxonomy = the prefix. The prefix is
    resolved to a namespace from the instance's own declarations, since the
    namespace changes with every filing (COP's is dated by period). An
    instance that does not declare the prefix contributes none.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Tuple

from lxml import etree

XBRLI = "{http://www.xbrl.org/2003/instance}"
XSI_NIL = "{http://www.w3.org/2001/XMLSchema-instance}nil"
TAXONOMIES = {"us-gaap": "http://fasb.org/us-gaap/", "dei": "http://xbrl.sec.gov/dei/"}

# An instance is data from outside: no entities, no network, large files allowed.
_PARSER = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True)


def _measure(el) -> str:
    prefix, _, name = el.text.strip().partition(":")
    return name if name else prefix


def _unit(el) -> str:
    """'USD', 'shares', 'pure', or 'USD/shares' for a divide, as companyfacts names them."""
    measure = el.find(f"{XBRLI}measure")
    if measure is not None:
        return _measure(measure)
    num = el.find(f"{XBRLI}divide/{XBRLI}unitNumerator/{XBRLI}measure")
    den = el.find(f"{XBRLI}divide/{XBRLI}unitDenominator/{XBRLI}measure")
    return f"{_measure(num)}/{_measure(den)}"


def _contexts(root, cik: int) -> Dict[str, Tuple[Optional[str], str]]:
    """Context id -> (start or None, end), for the filer's own contexts without dimensions."""
    out: Dict[str, Tuple[Optional[str], str]] = {}
    for c in root.iter(f"{XBRLI}context"):
        if c.find(f".//{XBRLI}segment") is not None or c.find(f".//{XBRLI}scenario") is not None:
            continue
        identifier = c.find(f"{XBRLI}entity/{XBRLI}identifier")
        if identifier is None or not identifier.text.strip().isdigit():
            continue
        if int(identifier.text) != int(cik):
            continue
        period = c.find(f"{XBRLI}period")
        instant = period.find(f"{XBRLI}instant")
        if instant is not None:
            out[c.get("id")] = (None, instant.text.strip())
            continue
        start = period.find(f"{XBRLI}startDate").text.strip()
        end = period.find(f"{XBRLI}endDate").text.strip()
        out[c.get("id")] = (None if start == end else start, end)
    return out


def _precision(el) -> float:
    d = el.get("decimals")
    return float("inf") if d in (None, "INF") else float(d)


def instance_company_facts(
    xml: bytes,
    cik: int,
    accn: str,
    form: str,
    filed: str,
    extensions: Iterable[Tuple[str, str]] = (),
) -> Dict[str, Any]:
    """A companyfacts-shaped document of one filing's facts, as companyfacts would serve them."""
    root = etree.fromstring(xml, _PARSER)
    own = {
        (root.nsmap[prefix], concept): prefix
        for prefix, concept in extensions
        if prefix in root.nsmap
    }
    contexts = _contexts(root, cik)
    units = {u.get("id"): _unit(u) for u in root.iter(f"{XBRLI}unit")}
    dei: Dict[str, str] = {}
    best: Dict[Tuple, Tuple[float, Dict[str, Any]]] = {}
    for el in root:
        if not isinstance(el.tag, str) or el.get("contextRef") is None:
            continue
        namespace, _, concept = el.tag[1:].partition("}")
        taxonomy = next((t for t, ns in TAXONOMIES.items() if namespace.startswith(ns)), None)
        taxonomy = own.get((namespace, concept), taxonomy)
        if taxonomy is None:
            continue
        if el.get("unitRef") is None:
            if taxonomy == "dei":
                dei[concept] = (el.text or "").strip()
            continue
        if el.get(XSI_NIL) == "true" or el.get("contextRef") not in contexts:
            continue
        start, end = contexts[el.get("contextRef")]
        unit = units[el.get("unitRef")]
        key = (taxonomy, concept, unit, start, end)
        precision = _precision(el)
        if key in best and best[key][0] >= precision:
            continue
        row: Dict[str, Any] = {"end": end, "val": float(el.text), "accn": accn}
        if start is not None:
            row["start"] = start
        best[key] = (precision, row)

    fy = dei.get("DocumentFiscalYearFocus")
    fp = dei.get("DocumentFiscalPeriodFocus") or None
    facts: Dict[str, Dict[str, Any]] = {}
    for (taxonomy, concept, unit, _, _), (_, row) in best.items():
        row.update(fy=int(fy) if fy else None, fp=fp, form=form, filed=filed)
        units_of = facts.setdefault(taxonomy, {}).setdefault(concept, {"units": {}})["units"]
        units_of.setdefault(unit, []).append(row)
    return {"cik": int(cik), "entityName": dei.get("EntityRegistrantName", ""), "facts": facts}
