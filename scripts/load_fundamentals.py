"""
scripts/load_fundamentals.py

Load SEC company facts into `fundamental_facts`, insert-only.

    python scripts/load_fundamentals.py --from-file facts.json[.gz] ...   # offline
    python scripts/load_fundamentals.py --symbols AAPL MSFT               # live: needs SEC_USER_AGENT
    python scripts/load_fundamentals.py --all                             # every equity with a CIK
    python scripts/load_fundamentals.py --recent [DAYS]                   # 06:00 job: filers of the last 7 days
    python scripts/load_fundamentals.py --instances-since 2024-01-01      # sweep: filings companyfacts lacks

`--recent DAYS` reads EDGAR's daily form index for each business day in the
window (one small file per day, the days named by the quarter's listing) and
reloads only registry companies — own CIK or a predecessor — that filed a
10-K, 10-Q, 20-F, 40-F or an amendment. A whole reload is ~500 requests and
2 GB; a day's is the index plus the handful that filed. The window overlaps
on purpose: a morning the Mac slept through is caught up, and a reload is
insert-only, so a repeat inserts nothing.

Live loads read the CIK recorded by scripts/resolve_ciks.py --write and
report the filer's own entityName beside the symbol: that is the identity
check a CIK claim gets (a mismatch is printed as MISMATCH? and the rows are
still stored under the CIK, which is what they belong to — it is the
SYMBOL->CIK mapping that would be wrong).

companyfacts leaves out whole filings (decision 59,
research/sec-companyfacts-gap-2026-10-23.md). `--recent` also reads
SEC_INSTANCE_LOOKBACK_DAYS (30) of the index: a registry filing at least
SEC_INSTANCE_GRACE_DAYS (3) old with no stored row is read from its own XBRL
instance (`INSTANCE` lines; source 'instance'). `--instances-since DATE`
does the same for every filing since DATE from each filer's submissions list
(the one-time sweep). A companyfacts row that later differs from the
instance row under its key is printed as DISAGREE; the instance row stays.

`--fetched-at` sets the provenance timestamp for an offline file (default:
the file's modification time). Exit 1 if any load failed, else 2 if any
DISAGREE, else 0.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings  # noqa: E402
from core.fundamentals import ingest_company, ingest_instance  # noqa: E402
from db.repositories.fundamentals import TimescaleFactRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402


def read_doc(path: Path) -> dict:
    raw = gzip.open(path).read() if path.suffix == ".gz" else path.read_bytes()
    return json.loads(raw)


def periodic_filings(
    gateway, cik: int, since: date, until: date
) -> List[Tuple[str, int, date, str]]:
    """(form, cik, filed, accn) of a filer's SEC_FACT_FORMS filings in [since, until], paged files included."""
    from core.insiders import SUBMISSIONS_PAGE_URL, pages_to_read

    submissions = gateway.submissions(cik)
    pages = [submissions["filings"]["recent"]] + [
        gateway.get_json(SUBMISSIONS_PAGE_URL.format(name=f["name"]))
        for f in pages_to_read(submissions, since)
    ]
    out = []
    for page in pages:
        for form, filed, accn in zip(
            page["form"], page["filingDate"], page["accessionNumber"], strict=True
        ):
            day = date.fromisoformat(filed)
            if form in settings.SEC_FACT_FORMS and since <= day <= until:
                out.append((form, cik, day, accn))
    return out


async def run(args: argparse.Namespace) -> int:
    failed = 0
    async with AsyncSessionLocal() as session:
        repo = TimescaleFactRepo(session)
        if args.from_file:
            for path in args.from_file:
                fetched = args.fetched_at or datetime.fromtimestamp(
                    path.stat().st_mtime, tz=timezone.utc
                )
                report = await ingest_company(repo, read_doc(path), fetched_at=fetched)
                print(
                    f"{path.name}: CIK {report.cik} {report.entity_name!r}: {report.served} facts in "
                    f"{report.concepts} concepts, {report.inserted} new"
                )
            return 0
        from sqlalchemy import text

        from core.sec import SECBlockedError, SECGateway, SECThrottledError

        gateway = SECGateway()  # raises, naming SEC_USER_AGENT, if it is not set
        where = "AND symbol = ANY(:syms)" if args.symbols else ""
        assets = (
            await session.execute(
                text(
                    "SELECT symbol, (metadata->>'cik')::int, metadata->>'cik_sec_title', "
                    "COALESCE(metadata->'cik_predecessors', '[]'::jsonb) FROM assets "
                    f"WHERE asset_class = 'equity' AND metadata ? 'cik' {where} ORDER BY symbol"
                ),
                {"syms": [s.upper() for s in (args.symbols or [])]},
            )
        ).all()
        # Each asset's own CIK, then its predecessors (titled as such); a CIK
        # shared by two share classes is fetched once.
        rows, seen = [], set()
        for symbol, cik, title, preds in assets:
            for c, t in [(cik, title)] + [(int(p), None) for p in (preds or [])]:
                if c not in seen:
                    seen.add(c)
                    rows.append((symbol if c == cik else f"{symbol}<-pred", c, t))
        label = {c: s for s, c, _ in rows}
        today = date.today()
        # Filings companyfacts may have left out, read from their instance
        # once SEC_INSTANCE_GRACE_DAYS old if no row of theirs is stored.
        candidates: List[Tuple[str, int, date, str]] = []
        if args.recent:
            span = max(args.recent, settings.SEC_INSTANCE_LOOKBACK_DAYS)
            filings, days = await asyncio.to_thread(
                gateway.recent_fact_filings, today - timedelta(days=span), today
            )
            in_window = {
                cik for _, cik, filed, _ in filings if filed >= today - timedelta(days=args.recent)
            }
            candidates = [f for f in filings if f[1] in label]
            rows = [r for r in rows if r[1] in in_window]
            listed = [d for d in days if d >= today - timedelta(days=args.recent)]
            print(
                f"daily index read for {len(listed)} business days "
                f"({listed[0] if listed else '-'}..{listed[-1] if listed else '-'}): {len(in_window)} filers of "
                f"10-K/10-Q/20-F/40-F, {len(rows)} in the registry"
            )
        elif args.instances_since:
            for _, cik, _ in rows:
                try:
                    candidates += await asyncio.to_thread(
                        periodic_filings, gateway, cik, args.instances_since, today
                    )
                except (SECBlockedError, SECThrottledError) as exc:
                    print(f"STOPPED at {label[cik]}: {exc}")
                    return 1
            rows = []  # a sweep reads instances only; companyfacts is the daily step's
        disagreements = 0
        for symbol, cik, title in rows:
            try:
                doc = await asyncio.to_thread(gateway.company_facts, cik)
            except Exception as exc:  # a block stops everything; others are per symbol
                if isinstance(exc, (SECBlockedError, SECThrottledError)):
                    print(f"STOPPED at {symbol}: {exc}")
                    return 1
                failed += 1
                print(f"{symbol} CIK {cik}: FAILED {exc}")
                continue
            report = await ingest_company(repo, doc)
            flag = (
                ""
                if not title or title.lower()[:12] == report.entity_name.lower()[:12]
                else "  MISMATCH?"
            )
            print(
                f"{symbol} CIK {cik} {report.entity_name!r}: {report.served} facts, {report.inserted} new{flag}"
            )
            for fact, stored in report.disagreements:
                disagreements += 1
                period = f"{fact.period_start or ''}..{fact.period_end}"
                print(
                    f"DISAGREE {symbol} {fact.accn} {fact.concept} {fact.unit} {period}: "
                    f"companyfacts {fact.value:g}, instance {stored:g} (instance row kept)"
                )

        due = today - timedelta(days=settings.SEC_INSTANCE_GRACE_DAYS)
        stored = await repo.stored_accessions({f[3] for f in candidates})
        missing = sorted(
            {f for f in candidates if f[2] <= due and f[3] not in stored},
            key=lambda f: (f[2], f[1], f[3]),
        )
        if args.recent or args.instances_since:
            print(
                f"{len(missing)} filing(s) at least {settings.SEC_INSTANCE_GRACE_DAYS} days old "
                "with no stored row: read from their XBRL instance"
            )
        for form, cik, filed, accn in missing:
            try:
                name, xml = await asyncio.to_thread(gateway.filing_instance, cik, accn)
                report = await ingest_instance(repo, xml, cik, accn, form, filed)
            except (SECBlockedError, SECThrottledError) as exc:
                print(f"STOPPED at {label[cik]} {accn}: {exc}")
                return 1
            except Exception as exc:  # noqa: BLE001 — one filing must not stop the rest
                failed += 1
                print(f"INSTANCE {label[cik]} {form} {filed} {accn}: FAILED {exc}")
                continue
            print(
                f"INSTANCE {label[cik]} {form} {filed} {accn} {name}: {report.served} facts, "
                f"{report.inserted} new"
                + (
                    ""
                    if report.served
                    else " (no allow-listed facts: a cover-only amendment, or IFRS)"
                )
            )
    if failed:
        return 1
    return 2 if disagreements else 0


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--from-file", nargs="+", type=Path)
    p.add_argument("--fetched-at", type=lambda s: datetime.fromisoformat(s))
    p.add_argument("--symbols", nargs="+")
    p.add_argument("--all", action="store_true")
    p.add_argument(
        "--recent",
        type=int,
        metavar="DAYS",
        nargs="?",
        const=settings.SEC_REFRESH_LOOKBACK_DAYS,
        help=f"default {settings.SEC_REFRESH_LOOKBACK_DAYS} (SEC_REFRESH_LOOKBACK_DAYS)",
    )
    p.add_argument(
        "--instances-since",
        type=date.fromisoformat,
        metavar="DATE",
        help="sweep: every filing since DATE with no stored row, from its instance "
        "(--symbols narrows it; companyfacts is not reloaded)",
    )
    args = p.parse_args()
    if not (args.from_file or args.symbols or args.all or args.recent or args.instances_since):
        p.error("give --from-file, --symbols, --all, --recent DAYS or --instances-since DATE")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
