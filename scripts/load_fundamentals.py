"""
scripts/load_fundamentals.py

Load SEC company facts into `fundamental_facts`, insert-only.

    python scripts/load_fundamentals.py --from-file facts.json[.gz] ...   # offline
    python scripts/load_fundamentals.py --symbols AAPL MSFT               # live: needs SEC_USER_AGENT
    python scripts/load_fundamentals.py --all                             # every equity with a CIK

Live loads read the CIK recorded by scripts/resolve_ciks.py --write and
report the filer's own entityName beside the symbol: that is the identity
check a CIK claim gets (a mismatch is printed as MISMATCH? and the rows are
still stored under the CIK, which is what they belong to — it is the
SYMBOL->CIK mapping that would be wrong).

`--fetched-at` sets the provenance timestamp for an offline file (default:
the file's modification time). Exit 1 if any load failed.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.fundamentals import ingest_company  # noqa: E402
from db.repositories.fundamentals import TimescaleFactRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402


def read_doc(path: Path) -> dict:
    raw = gzip.open(path).read() if path.suffix == ".gz" else path.read_bytes()
    return json.loads(raw)


async def run(args: argparse.Namespace) -> int:
    failed = 0
    async with AsyncSessionLocal() as session:
        repo = TimescaleFactRepo(session)
        if args.from_file:
            for path in args.from_file:
                fetched = args.fetched_at or datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                report = await ingest_company(repo, read_doc(path), fetched_at=fetched)
                print(f"{path.name}: CIK {report.cik} {report.entity_name!r}: {report.served} facts in "
                      f"{report.concepts} concepts, {report.inserted} new")
            return 0
        from sqlalchemy import text

        from core.sec import SECGateway

        gateway = SECGateway()  # raises, naming SEC_USER_AGENT, if it is not set
        where = "" if args.all else "AND symbol = ANY(:syms)"
        rows = (await session.execute(text(
            "SELECT symbol, (metadata->>'cik')::int, metadata->>'cik_sec_title' FROM assets "
            f"WHERE asset_class = 'equity' AND metadata ? 'cik' {where} ORDER BY symbol"
        ), {"syms": [s.upper() for s in (args.symbols or [])]})).all()
        for symbol, cik, title in rows:
            try:
                doc = await asyncio.to_thread(gateway.company_facts, cik)
            except Exception as exc:  # a block stops everything; others are per symbol
                from core.sec import SECBlockedError, SECThrottledError

                if isinstance(exc, (SECBlockedError, SECThrottledError)):
                    print(f"STOPPED at {symbol}: {exc}")
                    return 1
                failed += 1
                print(f"{symbol} CIK {cik}: FAILED {exc}")
                continue
            report = await ingest_company(repo, doc)
            flag = "" if not title or title.lower()[:12] == report.entity_name.lower()[:12] else "  MISMATCH?"
            print(f"{symbol} CIK {cik} {report.entity_name!r}: {report.served} facts, {report.inserted} new{flag}")
    return 1 if failed else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from-file", nargs="+", type=Path)
    p.add_argument("--fetched-at", type=lambda s: datetime.fromisoformat(s))
    p.add_argument("--symbols", nargs="+")
    p.add_argument("--all", action="store_true")
    args = p.parse_args()
    if not (args.from_file or args.symbols or args.all):
        p.error("give --from-file, --symbols or --all")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
