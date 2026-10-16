"""
scripts/load_insiders.py

Load SEC Form 4 insider transactions for registry issuers into
`insider_transactions`, insert-only (decision 40a). Needs SEC_USER_AGENT.

    python scripts/load_insiders.py --symbols MSFT AAPL     # these issuers
    python scripts/load_insiders.py --all                   # every registry equity with a CIK
    python scripts/load_insiders.py --all --since 2024-10-16

Default --since is two years back. One request per issuer for its submissions
list, then one per Form 4 not stored yet, paced by the SEC gateway; a re-run
fetches only what is new. Do not run beside another SEC loader: each paces
itself, and two together can exceed SEC's 10 requests a second.

Exit 1 if any issuer failed or any filing could not be parsed.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from core.insiders import refresh_issuer  # noqa: E402
from core.sec import SECGateway  # noqa: E402
from db.models import AssetORM  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402


async def issuers(symbols: list[str] | None) -> list[tuple[str, int]]:
    """(symbol, CIK) for each CIK an equity carries: its own and any predecessor.
    A reorganised filer's older Form 4s are under the predecessor (XOM, BLK)."""
    async with AsyncSessionLocal() as s:
        query = select(AssetORM.symbol, AssetORM.metadata_).where(
            AssetORM.asset_class == "equity", AssetORM.delisted_at.is_(None)
        )
        if symbols:
            query = query.where(AssetORM.symbol.in_([x.upper() for x in symbols]))
        rows = (await s.execute(query.order_by(AssetORM.symbol))).all()
    out = []
    for sym, meta in rows:
        meta = meta or {}
        for cik in [meta.get("cik"), *(meta.get("cik_predecessors") or [])]:
            if cik and (sym, int(cik)) not in out:
                out.append((sym, int(cik)))
    return out


async def run(args: argparse.Namespace) -> int:
    gateway = SECGateway()
    targets = await issuers(args.symbols if not args.all else None)
    seen = fetched = inserted = 0
    problems: list[str] = []
    reach: dict[str, date] = {}  # earliest filing any of a symbol's CIKs lists
    for i, (symbol, cik) in enumerate(targets, 1):
        try:
            async with AsyncSessionLocal() as session:
                r = await refresh_issuer(gateway, session, cik, args.since)
        except Exception as exc:  # noqa: BLE001 — report and carry on with the next issuer
            problems.append(f"{symbol}: {str(exc)[:120]}")
            print(f"[{i}/{len(targets)}] {symbol}: FAILED {str(exc)[:120]}", flush=True)
            continue
        seen += r.filings_seen
        fetched += r.filings_fetched
        inserted += r.rows_inserted
        problems += [f"{symbol} {f}" for f in r.failed]
        if r.reaches_back_to:
            reach[symbol] = min(reach.get(symbol, r.reaches_back_to), r.reaches_back_to)
        print(
            f"[{i}/{len(targets)}] {symbol} CIK {cik}: {r.filings_seen} Form 4s since {args.since}, "
            f"{r.filings_fetched} fetched, {r.rows_inserted} rows",
            flush=True,
        )
    for symbol, earliest in sorted(reach.items()):
        if earliest > args.since:
            # Even across its CIKs and their paged files, the symbol's filings do
            # not reach --since. Said, not hidden: a predecessor missing from
            # cik_predecessors looks exactly like this.
            problems.append(f"{symbol}: filings start {earliest}, after --since")
    print(
        f"Done: {len(targets)} issuers, {seen} filings seen, {fetched} fetched, "
        f"{inserted} rows inserted, {gateway.requests_made} SEC requests."
    )
    for p in problems:
        print(f"  ! {p}")
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--symbols", nargs="+")
    group.add_argument("--all", action="store_true")
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=730),
        help="Earliest filing date (default: two years back)",
    )
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
