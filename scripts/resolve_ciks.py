"""
scripts/resolve_ciks.py

Propose an SEC CIK for every registered equity. DRY RUN by default: prints
what it would record and what it cannot resolve. --write records
`cik`, `cik_source` and `cik_sec_title` in assets.metadata.

    python scripts/resolve_ciks.py --tickers-file company_tickers.json   # offline
    python scripts/resolve_ciks.py                                       # fetch the map (needs SEC_USER_AGENT)
    python scripts/resolve_ciks.py --write

See core/cik_resolution.py for the order and why a CIK is a claim until the
first live company-facts fetch checks it against the filer's entity name.
ETFs and crypto have no filer and are not considered.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402

from core.cik_resolution import renames_from_csv, resolve, ticker_map  # noqa: E402

RENAMES = ROOT / "research" / "sp500-ticker-renames-2026-10-04.csv"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--tickers-file", type=Path, help="A saved company_tickers.json; no network"
    )
    parser.add_argument("--write", action="store_true", help="Record the CIKs in assets.metadata")
    args = parser.parse_args()

    if args.tickers_file:
        raw = json.loads(args.tickers_file.read_text())
        source = f"company_tickers.json file {args.tickers_file.name}"
    else:
        from core.sec import SECGateway

        raw = SECGateway().company_tickers()
        source = "company_tickers.json (live)"
    tickers = ticker_map(raw)
    with open(RENAMES) as fh:
        renames = renames_from_csv(csv.DictReader(fh))

    from db.session import settings as db_settings

    conn = psycopg2.connect(db_settings.SYNC_DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        "SELECT symbol, delisted_at IS NOT NULL, COALESCE(metadata ? 'history_valid_until', false) "
        "FROM assets WHERE asset_class = 'equity' ORDER BY symbol"
    )
    registry = cur.fetchall()
    delisted = {s for s, d, _ in registry if d}
    reassigned = {s for s, _, r in registry if r}
    results = resolve([s for s, _, _ in registry], tickers, renames, reassigned)

    how = Counter(r.how.split(":")[0] for r in results)
    print(
        f"{len(results)} equities against {len(tickers)} tickers ({source}), {len(renames)} rename symbols"
    )
    for k, v in how.most_common():
        print(f"  {k}: {v}")
    unresolved = [
        f"{r.symbol}{' [reassigned ticker]' if r.how == 'reassigned_ticker' else ''}"
        for r in results
        if r.cik is None
    ]
    print(
        "unresolved:",
        ", ".join(f"{s}{' (delisted)' if s.split(' ')[0] in delisted else ''}" for s in unresolved)
        or "none",
    )
    for r in results:
        if (
            r.how.startswith("ticker_map_variant")
            or r.how in ("rename_csv", "override")
            or r.predecessors
        ):
            pred = f" + predecessors {list(r.predecessors)}" if r.predecessors else ""
            print(f"  {r.symbol}: CIK {r.cik} via {r.how}{pred} {r.sec_title or ''}")

    # The signature of history left under a predecessor CIK: the filer's first
    # 10-K/10-Q comes long after the stock's first stored price (XOM's holdco
    # filed first in 2026; its bars start in 2015). Needs facts loaded.
    from config.settings import CIK_HISTORY_GAP_ACCEPTED, CIK_HISTORY_GAP_DAYS

    chain = {r.symbol: [r.cik, *r.predecessors] for r in results if r.cik is not None}
    cur.execute(
        "SELECT cik, min(filed) FROM fundamental_facts "
        "WHERE form IN ('10-K', '10-Q', '20-F', '40-F', '10-K/A', '20-F/A', '40-F/A') GROUP BY cik"
    )
    first_filed = dict(cur.fetchall())
    cur.execute(
        "SELECT a.symbol, min(m.time)::date FROM market_data m JOIN assets a ON a.id = m.asset_id "
        "WHERE a.asset_class = 'equity' GROUP BY a.symbol"
    )
    first_bar = dict(cur.fetchall())
    gaps = []
    for sym, ciks in chain.items():
        filed = [first_filed[c] for c in ciks if c in first_filed]
        if sym in CIK_HISTORY_GAP_ACCEPTED:
            continue
        if filed and sym in first_bar and (min(filed) - first_bar[sym]).days > CIK_HISTORY_GAP_DAYS:
            gaps.append(f"{sym} (filings from {min(filed)}, bars from {first_bar[sym]})")
    print(
        f"history gap > {CIK_HISTORY_GAP_DAYS} days (possible predecessor CIK): {', '.join(gaps) or 'none'}"
    )

    if not args.write:
        print("dry run: nothing written (pass --write)")
        return 0
    n = 0
    for r in results:
        if r.cik is None:
            continue
        cur.execute(
            "UPDATE assets SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb "
            "WHERE symbol = %s AND asset_class = 'equity'",
            (
                json.dumps(
                    {
                        "cik": r.cik,
                        "cik_source": f"{r.how}; {source}",
                        "cik_sec_title": r.sec_title,
                        "cik_predecessors": list(r.predecessors),
                    }
                ),
                r.symbol,
            ),
        )
        n += cur.rowcount
    conn.commit()
    print(f"wrote cik on {n} assets")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
