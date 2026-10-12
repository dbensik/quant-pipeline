"""
core/cik_resolution.py

Ticker -> SEC CIK for the registry's equities. Pure; scripts/resolve_ciks.py
does the I/O. Tier 3 phase 0 of research/dcf-plan-2026-10-08.md.

A CIK on an asset is a CLAIM, like a crypto provider ticker: the ticker map
says which filer holds a ticker TODAY, which is wrong for a renamed or
re-issued one. So the order is explicit and every result records how it was
found:

    1. `renamed_from` in the asset's metadata is ignored for lookup: the
       asset row already carries the CURRENT ticker (BK, FI, MMC repaired).
    2. the symbol as written, then its dot/dash/bare variants (BRK-B, BRK.B);
    3. the rename CSV (research/sp500-ticker-renames-2026-10-04.csv), whose
       rows carry the CIK they were confirmed against (same_cik evidence).

A ticker the registry marks as RE-ISSUED (`history_valid_until` in
metadata) is never looked up: measured 2026-10-09, PARA in today's map is CIK
1826011 "Banzai International" — the penny stock that took the key, not
Paramount. It is reported as `reassigned_ticker`.

settings.CIK_OVERRIDES wins over all of the above (WBA: the rename CSV's CIK
was the pre-2014 Walgreen Co). settings.CIK_PREDECESSORS adds earlier filers
of the same business, read together with the asset's CIK (XOM's history is
under 34088; the ticker moved to holding company 2115436 on 2026-07-01).

Anything else is unresolved and listed, never guessed. The CIK is checked
against the filer's own entity name on the first live fetch, not here: the
registry holds no company names (measured 2026-10-09: 0 of 517 equities).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from config.settings import CIK_OVERRIDES, CIK_PREDECESSORS


@dataclass(frozen=True)
class Resolution:
    symbol: str
    cik: Optional[int]
    how: str  # 'override' | 'ticker_map' | 'ticker_map_variant:<v>' | 'rename_csv' | 'reassigned_ticker' | 'unresolved'
    sec_title: Optional[str] = None
    predecessors: Tuple[int, ...] = ()


def ticker_map(company_tickers: Mapping[str, Mapping]) -> Dict[str, Tuple[int, str]]:
    """company_tickers.json -> {TICKER: (cik, title)}."""
    out: Dict[str, Tuple[int, str]] = {}
    for row in company_tickers.values():
        out[str(row["ticker"]).upper()] = (int(row["cik_str"]), str(row.get("title", "")))
    return out


def variants(symbol: str) -> List[str]:
    s = symbol.upper()
    out = [s.replace("-", "."), s.replace(".", "-"), s.replace("-", "").replace(".", "")]
    return [v for v in dict.fromkeys(out) if v != s]


def resolve(
    symbols: Iterable[str],
    tickers: Mapping[str, Tuple[int, str]],
    renames: Mapping[str, int],
    reassigned: Iterable[str] = (),
    overrides: Mapping[str, int] = CIK_OVERRIDES,
    predecessors: Mapping[str, Tuple[int, ...]] = CIK_PREDECESSORS,
) -> List[Resolution]:
    reissued = {r.upper() for r in reassigned}
    out: List[Resolution] = []
    for symbol in symbols:
        r = _resolve_one(symbol.upper(), tickers, renames, reissued, overrides)
        if r.cik is not None and r.symbol in predecessors:
            r = Resolution(
                r.symbol, r.cik, r.how, r.sec_title, tuple(int(c) for c in predecessors[r.symbol])
            )
        out.append(r)
    return out


def _resolve_one(s, tickers, renames, reissued, overrides) -> Resolution:
    if s in overrides:
        return Resolution(s, int(overrides[s]), "override")
    if s in reissued:
        return Resolution(s, None, "reassigned_ticker")
    if s in tickers:
        cik, title = tickers[s]
        return Resolution(s, cik, "ticker_map", title)
    hit = next((v for v in variants(s) if v in tickers), None)
    if hit is not None:
        cik, title = tickers[hit]
        return Resolution(s, cik, f"ticker_map_variant:{hit}", title)
    if s in renames:
        return Resolution(s, renames[s], "rename_csv")
    return Resolution(s, None, "unresolved")


def renames_from_csv(rows: Iterable[Mapping[str, str]]) -> Dict[str, int]:
    """Both old and new symbols of a same-CIK rename map to that CIK."""
    out: Dict[str, int] = {}
    for row in rows:
        cik = (row.get("cik") or "").strip()
        if not cik or row.get("evidence", "same_cik") != "same_cik":
            continue
        for key in ("old_symbol", "new_symbol"):
            sym = (row.get(key) or "").strip().upper()
            if sym:
                out[sym] = int(cik)
    return out
