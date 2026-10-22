"""
scripts/archive_news.py

Store Yahoo search headlines for registry symbols, insert-only (decision 57 B,
research/news-archive-2026-10-22.md, core/news_archive.py).

    python scripts/archive_news.py                  # every equity, ETF and coin (06:00 job, ~70 s)
    python scripts/archive_news.py --due            # only symbols whose last page would be outrun (hourly job)
    python scripts/archive_news.py --symbols NVDA ETH-USD

Each symbol's search is one transaction and one `news_fetches` row, failures
included. An empty answer is retried twice a minute apart before it counts.

Exit 0: every symbol answered (empty after retries is an answer: NO NEWS).
Exit 1: some symbols FAILED after retries (named); the rest are stored.
Exit 2: every search failed, or the run could not start.
TRUNCATED lines name symbols whose stories were missed since their last
fetch; that is recorded in news_fetches, not a flag.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from config.settings import (  # noqa: E402
    NEWS_ARCHIVE_HOURLY_INTERVAL_HOURS,
    NEWS_ARCHIVE_PER_SYMBOL,
)
from core.news_archive import (  # noqa: E402
    due,
    fetch_all,
    last_fetches,
    now_utc,
    search_term,
    store_fetch,
)
from db.models import AssetORM  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402


def yahoo_search(term: str):
    import yfinance as yf

    return yf.Search(term, news_count=NEWS_ARCHIVE_PER_SYMBOL, max_results=1).response


async def registry(symbols: list[str] | None) -> dict[str, str]:
    """{symbol: search term} for every listed equity, ETF and coin."""
    async with AsyncSessionLocal() as s:
        query = select(AssetORM.symbol, AssetORM.asset_class).where(
            AssetORM.asset_class.in_(["equity", "etf", "crypto"]), AssetORM.delisted_at.is_(None)
        )
        if symbols:
            query = query.where(AssetORM.symbol.in_([x.upper() for x in symbols]))
        rows = (await s.execute(query.order_by(AssetORM.symbol))).all()
    return {sym: search_term(sym, cls) for sym, cls in rows}


async def run(args: argparse.Namespace) -> int:
    targets = await registry(args.symbols)
    if not targets:
        print("No registry symbols to search.")
        return 2
    async with AsyncSessionLocal() as s:
        previous = await last_fetches(s, list(targets))
    if args.due:
        now = now_utc()
        interval = timedelta(hours=NEWS_ARCHIVE_HOURLY_INTERVAL_HOURS)
        targets = {k: v for k, v in targets.items() if due(previous.get(k), now, interval)}
        if not targets:
            print("Done: no symbol due.")
            return 0

    started = time.monotonic()
    fetches = await asyncio.to_thread(fetch_all, targets, yahoo_search, time.sleep)

    stored = []
    for fetch in fetches:
        async with AsyncSessionLocal() as s:
            stored.append(await store_fetch(s, fetch, previous.get(fetch.symbol)))

    failed = [r for r in stored if r.error]
    empty = [r.symbol for r in stored if not r.error and r.returned == 0]
    truncated = [r for r in stored if r.window_truncated]
    for r in failed:
        print(f"FETCH FAILED {r.symbol}: {r.error}")
    for r in truncated:
        hours = r.span.total_seconds() / 3600 if r.span else float("nan")
        print(
            f"TRUNCATED {r.symbol}: a full page spans {hours:.1f} h; stories since the last fetch were missed"
        )
    if empty:
        print(f"NO NEWS after retries ({len(empty)}): {' '.join(empty)}")
    print(
        f"Done: {len(stored)} symbols searched in {time.monotonic() - started:.0f} s, "
        f"{sum(r.new_stories for r in stored)} new symbol-story rows, "
        f"{len(failed)} failed, {len(empty)} empty, {len(truncated)} truncated."
    )
    if failed and len(failed) == len(stored):
        return 2
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--due", action="store_true", help="Only symbols the hourly pass needs")
    try:
        return asyncio.run(run(parser.parse_args()))
    except Exception as exc:  # noqa: BLE001 — the database, most likely; one line, exit 2
        print(f"FAILED: {type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
