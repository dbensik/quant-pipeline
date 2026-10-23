"""
scripts/archive_news.py

Store Yahoo search headlines for registry symbols, insert-only (decision 57 B,
research/news-archive-2026-10-22.md, core/news_archive.py).

    python scripts/archive_news.py                  # every equity, ETF and coin (06:00 job, ~70 s)
    python scripts/archive_news.py --due            # only pages about to be outrun, or empty last time (hourly job)
    python scripts/archive_news.py --symbols NVDA ETH-USD

A symbol is searched by every term in core.news_archive.search_terms (a coin
also by its name, a word ticker also by its NEWS_SEARCH_TERMS entry). Each
search is one transaction and one `news_fetches` row, failures included. At
06:00 an empty answer is retried twice a minute apart; the hourly pass does
not retry within the run, it retries next hour (step 2).

Exit 0: every symbol answered (empty after retries is an answer: NO NEWS).
Exit 1: some symbols FAILED after retries (named); the rest are stored.
Exit 2: every search failed, or the run could not start.
TRUNCATED lines name symbols whose stories were missed since their last
fetch; that is recorded in news_fetches, not a flag. WORD TICKER? names an
equity or ETF whose full ticker page is mostly other tickers' stories and that
has no NEWS_SEARCH_TERMS entry; WEAK TERM a listed term that came back empty
or mostly someone else's. Both are printed for review, never a flag.
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
    NEWS_ARCHIVE_EMPTY_RETRIES,
    NEWS_ARCHIVE_FULL_PAGE,
    NEWS_ARCHIVE_HOURLY_INTERVAL_HOURS,
    NEWS_ARCHIVE_OWN_SHARE,
    NEWS_ARCHIVE_PER_SYMBOL,
    NEWS_SEARCH_TERMS,
    NEWS_TICKER_PAGES_REVIEWED,
)
from core.news_archive import (  # noqa: E402
    Stored,
    due,
    fetch_all,
    last_fetches,
    now_utc,
    search_terms,
    store_fetch,
)
from db.models import AssetORM  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402


def yahoo_search(term: str):
    import yfinance as yf

    return yf.Search(term, news_count=NEWS_ARCHIVE_PER_SYMBOL, max_results=1).response


async def registry(symbols: list[str] | None) -> list[tuple[str, str]]:
    """(symbol, search term) for every term of every listed equity, ETF and coin."""
    async with AsyncSessionLocal() as s:
        query = select(
            AssetORM.symbol,
            AssetORM.asset_class,
            AssetORM.metadata_["verified_name"].astext,
        ).where(
            AssetORM.asset_class.in_(["equity", "etf", "crypto"]), AssetORM.delisted_at.is_(None)
        )
        if symbols:
            query = query.where(AssetORM.symbol.in_([x.upper() for x in symbols]))
        rows = (await s.execute(query.order_by(AssetORM.symbol))).all()
    return [(sym, term) for sym, cls, name in rows for term in search_terms(sym, cls, name)]


def mostly_others(r: Stored) -> bool:
    return r.own_tagged is not None and r.own_tagged < NEWS_ARCHIVE_OWN_SHARE * r.returned


def review_lines(stored: list[Stored]) -> list[str]:
    """WORD TICKER? and WEAK TERM lines: for review, never a flag."""
    lines = []
    for r in stored:
        if r.error:
            continue
        listed = NEWS_SEARCH_TERMS.get(r.symbol)
        if r.term == listed and (r.returned == 0 or mostly_others(r)):
            lines.append(
                f"WEAK TERM {r.symbol} '{r.term}': {r.own_tagged} of {r.returned} tagged {r.symbol}"
            )
        elif (
            r.term == r.symbol
            and listed is None
            and r.symbol not in NEWS_TICKER_PAGES_REVIEWED
            and r.returned >= NEWS_ARCHIVE_FULL_PAGE
            and mostly_others(r)
        ):
            lines.append(
                f"WORD TICKER? {r.symbol}: {r.own_tagged} of {r.returned} tagged {r.symbol}; "
                "no NEWS_SEARCH_TERMS entry"
            )
    return lines


async def run(args: argparse.Namespace) -> int:
    targets = await registry(args.symbols)
    if not targets:
        print("No registry symbols to search.")
        return 2
    async with AsyncSessionLocal() as s:
        previous = await last_fetches(s, sorted({sym for sym, _ in targets}))
    if args.due:
        now = now_utc()
        interval = timedelta(hours=NEWS_ARCHIVE_HOURLY_INTERVAL_HOURS)
        targets = [t for t in targets if due(previous.get(t), now, interval)]
        if not targets:
            print("Done: no symbol due.")
            return 0

    started = time.monotonic()
    # The hourly pass retries an empty answer NEXT hour: answers stick for
    # minutes, so pausing a minute inside the run only lengthens it.
    retries = 0 if args.due else NEWS_ARCHIVE_EMPTY_RETRIES
    fetches = await asyncio.to_thread(fetch_all, targets, yahoo_search, time.sleep, retries=retries)

    stored = []
    for fetch in fetches:
        async with AsyncSessionLocal() as s:
            stored.append(await store_fetch(s, fetch, previous.get((fetch.symbol, fetch.term))))

    failed = [r for r in stored if r.error]
    empty = [label(r) for r in stored if not r.error and r.returned == 0]
    truncated = [r for r in stored if r.window_truncated]
    for r in failed:
        print(f"FETCH FAILED {label(r)}: {r.error}")
    for r in truncated:
        hours = r.span.total_seconds() / 3600 if r.span else float("nan")
        print(
            f"TRUNCATED {label(r)}: a full page spans {hours:.1f} h; stories since the last fetch were missed"
        )
    if empty:
        print(f"NO NEWS ({len(empty)}): {' '.join(empty)}")
    for line in review_lines(stored):
        print(line)
    print(
        f"Done: {len(stored)} searches in {time.monotonic() - started:.0f} s, "
        f"{sum(r.new_stories for r in stored)} new symbol-story rows, "
        f"{len(failed)} failed, {len(empty)} empty, {len(truncated)} truncated."
    )
    if failed and len(failed) == len(stored):
        return 2
    return 1 if failed else 0


def label(r: Stored) -> str:
    """NVDA or ETH-USD for its own ticker's search; ETH-USD 'Ethereum' for a name's."""
    own = r.term == r.symbol or r.symbol.startswith(f"{r.term}-")
    return r.symbol if own else f"{r.symbol} '{r.term}'"


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
