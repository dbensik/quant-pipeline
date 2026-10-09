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

Anything else is unresolved and listed, never guessed. The CIK is checked
against the filer's own entity name on the first live fetch, not here: the
registry holds no company names (measured 2026-10-09: 0 of 517 equities).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Tuple


@dataclass(frozen=True)
class Resolution:
    symbol: str
    cik: Optional[int]
    how: str  # 'ticker_map' | 'ticker_map_variant:<variant>' | 'rename_csv' | 'reassigned_ticker' | 'unresolved'
    sec_title: Optional[str] = None


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
) -> List[Resolution]:
    out: List[Resolution] = []
    reissued = {r.upper() for r in reassigned}
    for symbol in symbols:
        s = symbol.upper()
        if s in reissued:
            out.append(Resolution(s, None, "reassigned_ticker"))
            continue
        if s in tickers:
            cik, title = tickers[s]
            out.append(Resolution(s, cik, "ticker_map", title))
            continue
        hit = next((v for v in variants(s) if v in tickers), None)
        if hit is not None:
            cik, title = tickers[hit]
            out.append(Resolution(s, cik, f"ticker_map_variant:{hit}", title))
            continue
        if s in renames:
            out.append(Resolution(s, renames[s], "rename_csv"))
            continue
        out.append(Resolution(s, None, "unresolved"))
    return out


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
