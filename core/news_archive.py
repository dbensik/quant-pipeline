"""
core/news_archive.py

A headline archive from Yahoo's search, insert-only (decision 57 B,
research/news-archive-2026-10-22.md). Search serves only the newest 50 stories
a ticker, so a story not stored goes out of reach: for NVDA the 50 spanned
1.8 hours. Sentiment is scored later, from the stored text.

STORED UNFILTERED. Every story a symbol's search returns is kept with Yahoo's
tags as served; the relevance rule (keep a story tagged with the ticker or
another share class, api/routers/research.py) is a READ rule, so an amended
rule applies to the past too.

COVERAGE IS LOGGED, NEVER INFERRED. Each search is a `news_fetches` row, a
failed one included. `window_truncated` says stories were MISSED: the page was
full and even its oldest story is newer than this symbol's previous fetch.

Crypto is searched by its base symbol: `ETH-USD` returns nothing, `ETH`
returns stories Yahoo tags `ETH-USD` (34 coins with news instead of 3).

Pure helpers first (tested without a network or database), then the writes.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from config.settings import (
    NEWS_ARCHIVE_EMPTY_RETRIES,
    NEWS_ARCHIVE_FULL_PAGE,
    NEWS_ARCHIVE_RETRY_PAUSE_SECONDS,
    NEWS_ARCHIVE_WORKERS,
)
from core.news import normalise_news_item

logger = logging.getLogger(__name__)

#: term -> Yahoo's search response. Raises on a transport failure.
Search = Callable[[str], Any]


def search_term(symbol: str, asset_class: str) -> str:
    """What to search for: the ticker, or a coin's base symbol (ETH for ETH-USD)."""
    if asset_class == "crypto":
        return symbol.split("-")[0]
    return symbol


@dataclass
class Fetch:
    symbol: str
    term: str
    items: List[Dict[str, Any]] = field(default_factory=list)
    empty_attempts: int = 0
    error: Optional[str] = None
    #: When the answer kept was received; a retry is up to two minutes later.
    fetched_at: Optional[datetime] = None


def _search_once(search: Search, fetch: Fetch) -> None:
    fetch.fetched_at = now_utc()
    try:
        response = search(fetch.term)
    except Exception as exc:  # a transport failure: retried like an empty answer
        fetch.error = f"{type(exc).__name__}: {exc}"[:500]
        fetch.items = []
        return
    if not isinstance(response, dict) or "news" not in response:
        # Never "no news": a search response always carries the key (decision 57).
        fetch.error = "Yahoo's search returned no news field"
        fetch.items = []
        return
    fetch.error = None
    fetch.items = list(response["news"] or [])
    if not fetch.items:
        fetch.empty_attempts += 1


def fetch_all(
    targets: Dict[str, str],
    search: Search,
    sleep: Callable[[float], None],
    retries: int = NEWS_ARCHIVE_EMPTY_RETRIES,
    pause: float = NEWS_ARCHIVE_RETRY_PAUSE_SECONDS,
    workers: int = NEWS_ARCHIVE_WORKERS,
) -> List[Fetch]:
    """
    Search every symbol ({symbol: term}), then retry the empty and failed ones
    `retries` times, `pause` seconds apart. Empty answers are mostly transient
    (42 of 51 on 2026-10-22), so one is never taken at its word.
    """
    fetches = [Fetch(symbol, term) for symbol, term in targets.items()]

    def run(batch: List[Fetch]) -> None:
        with ThreadPoolExecutor(max(1, workers)) as pool:
            list(pool.map(lambda f: _search_once(search, f), batch))

    run(fetches)
    for _ in range(retries):
        pending = [f for f in fetches if f.error or not f.items]
        if not pending:
            break
        sleep(pause)
        run(pending)
    return fetches


def story_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Each raw item as a `news_stories` row, without first_seen_at. One without an id or title is skipped."""
    rows = []
    for raw in items:
        story = normalise_news_item(raw)
        if not story or not story["id"] or not story["title"]:
            continue
        rows.append(
            {
                "story_id": story["id"],
                "title": story["title"],
                "publisher": story["publisher"],
                "url": story["url"],
                "story_type": raw.get("type") if isinstance(raw, dict) else None,
                "published_at": story["published_at"],
                "related_tickers": story["related_tickers"],
            }
        )
    return rows


@dataclass(frozen=True)
class LastFetch:
    """A symbol's newest successful fetch, as the coverage log has it."""

    fetched_at: datetime
    returned: int
    oldest_published_at: Optional[datetime]


def window_truncated(
    previous: Optional[LastFetch], returned: int, oldest: Optional[datetime]
) -> Optional[bool]:
    """
    Were stories missed since the previous fetch? Only a FULL page can hide
    older stories; a short one is everything Yahoo has. None: nothing to
    compare against (a first fetch, or an undated page).
    """
    if previous is None or oldest is None:
        return None
    return returned >= NEWS_ARCHIVE_FULL_PAGE and oldest > previous.fetched_at


def due(last: Optional[LastFetch], now: datetime, interval: timedelta) -> bool:
    """
    The hourly pass refetches a symbol whose last full page would be outrun
    before the NEXT pass: elapsed + interval reaches the page's span. NVDA's
    span was 1.8 h, so it is refetched every hour; KO's (2.3 days) never is
    between the 06:00 runs. A symbol never fetched successfully is due.
    """
    if last is None:
        return True
    if last.returned < NEWS_ARCHIVE_FULL_PAGE or last.oldest_published_at is None:
        return False
    span = last.fetched_at - last.oldest_published_at
    return (now - last.fetched_at) + interval >= span


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


async def last_fetches(session: Any, symbols: List[str]) -> Dict[str, LastFetch]:
    """Each symbol's newest fetch that did not fail."""
    from sqlalchemy import select

    from db.models import NewsFetchORM

    f = NewsFetchORM
    rows = await session.execute(
        select(f.symbol, f.fetched_at, f.returned, f.oldest_published_at)
        .where(f.symbol.in_(symbols), f.error.is_(None))
        .distinct(f.symbol)
        .order_by(f.symbol, f.fetched_at.desc())
    )
    return {s: LastFetch(at, n, oldest) for s, at, n, oldest in rows.all()}


@dataclass(frozen=True)
class Stored:
    symbol: str
    returned: int
    new_stories: int
    window_truncated: Optional[bool]
    span: Optional[timedelta]
    error: Optional[str]


async def store_fetch(session: Any, fetch: Fetch, previous: Optional[LastFetch]) -> Stored:
    """
    One symbol's fetch in one transaction: its stories (a story already stored
    keeps its first sighting), the symbol's sightings, and its log row.
    """
    from sqlalchemy.dialects.postgresql import insert

    from db.models import NewsFetchORM, NewsStoryORM, NewsStorySymbolORM

    fetched_at = fetch.fetched_at or now_utc()
    rows = [] if fetch.error else story_rows(fetch.items)
    # One story twice in a page would make the multi-row insert hit its own key.
    rows = list({r["story_id"]: r for r in rows}.values())
    new = 0
    if rows:
        await session.execute(
            insert(NewsStoryORM)
            .values([{**r, "first_seen_at": fetched_at} for r in rows])
            .on_conflict_do_nothing(index_elements=["story_id"])
        )
        result = await session.execute(
            insert(NewsStorySymbolORM)
            .values(
                [
                    {
                        "story_id": r["story_id"],
                        "symbol": fetch.symbol,
                        "search_term": fetch.term,
                        "first_seen_at": fetched_at,
                    }
                    for r in rows
                ]
            )
            .on_conflict_do_nothing(constraint="uq_news_story_symbol")
            .returning(NewsStorySymbolORM.id)
        )
        new = len(result.all())
    dated = [r["published_at"] for r in rows if r["published_at"] is not None]
    oldest = min(dated) if dated else None
    newest = max(dated) if dated else None
    returned = len(fetch.items)
    truncated = None if fetch.error else window_truncated(previous, returned, oldest)
    session.add(
        NewsFetchORM(
            symbol=fetch.symbol,
            search_term=fetch.term,
            fetched_at=fetched_at,
            returned=returned,
            new_stories=new,
            oldest_published_at=oldest,
            newest_published_at=newest,
            empty_attempts=fetch.empty_attempts,
            window_truncated=truncated,
            error=fetch.error,
        )
    )
    await session.commit()
    return Stored(
        symbol=fetch.symbol,
        returned=returned,
        new_stories=new,
        window_truncated=truncated,
        span=(fetched_at - oldest) if oldest else None,
        error=fetch.error,
    )
