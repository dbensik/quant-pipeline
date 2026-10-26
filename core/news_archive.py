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
returns stories Yahoo tags `ETH-USD` (34 coins with news instead of 3). Since
step 2 (research/news-archive-gaps-2026-10-22.md) a coin is ALSO searched by
its verified name (349 -> 749 tagged stories: `BTC` returned none when
`Bitcoin` returned 43), and an equity whose ticker is a word by its measured
term in NEWS_SEARCH_TERMS. Each (symbol, term) is its own page, with its own
coverage history.

Pure helpers first (tested without a network or database), then the writes.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from config.settings import (
    CRYPTO_PROVIDER_SYMBOLS,
    NEWS_ARCHIVE_EMPTY_RETRIES,
    NEWS_ARCHIVE_FULL_PAGE,
    NEWS_ARCHIVE_NEVER_ANSWERED_HOURS,
    NEWS_ARCHIVE_OWN_SHARE,
    NEWS_ARCHIVE_RETRY_PAUSE_SECONDS,
    NEWS_ARCHIVE_SPAN_LOOKBACK_DAYS,
    NEWS_ARCHIVE_WORKERS,
    NEWS_NAME_WORDS,
    NEWS_SEARCH_TERMS,
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


def search_terms(symbol: str, asset_class: str, verified_name: Optional[str] = None) -> List[str]:
    """
    Every term a symbol is searched by: its search_term, then a coin's
    verified name or an equity's NEWS_SEARCH_TERMS entry. Both are kept, not
    one for the other: for ETH the name found 38 stories `ETH` did not.
    """
    terms = [search_term(symbol, asset_class)]
    extra = verified_name if asset_class == "crypto" else NEWS_SEARCH_TERMS.get(symbol)
    if extra and extra.strip() and extra.strip().casefold() != terms[0].casefold():
        terms.append(extra.strip())
    return terms


def own_tickers(symbol: str) -> Set[str]:
    """
    The tags that make a story the symbol's own: the symbol, and for a coin
    fetched under another Yahoo ticker (CRYPTO_PROVIDER_SYMBOLS) that ticker
    too. Measured 2026-10-22: all 10 'Uniswap' stories were tagged
    UNI7083-USD and 3 of them not UNI-USD; 142 archived stories on 22 mapped
    coins carried only the provider ticker
    (research/news-fallbacks-2026-10-22.md).
    """
    provider = CRYPTO_PROVIDER_SYMBOLS.get(symbol)
    return {symbol, provider} if provider else {symbol}


def own_tagged(symbol: str, items: Iterable[Any]) -> int:
    """
    How many stories Yahoo tagged with the symbol's own tickers (own_tickers).
    Counting the symbol alone had judged Hyperliquid's page (49 stories, 34 of
    the 40 stored tagged HYPE32196-USD) mostly someone else's, so the hourly
    pass never refetched it and its losses were never recorded.
    """
    own = own_tickers(symbol)
    return sum(
        1
        for raw in items
        if isinstance(raw, dict) and not own.isdisjoint(raw.get("relatedTickers") or [])
    )


_LEGAL_FORM = r"(?:inc|incorporated|corp|corporation|co|company|ltd|limited|plc|llc|lp|l\.p|n\.v|nv|s\.a|sa|ag|se)"
_GENERIC_TAIL = frozenset(
    "group holdings holding companies international technologies technology industries "
    "enterprises bancorp financial brands systems trust".split()
)


def name_forms(name: str) -> Set[str]:
    """
    The forms a company is named by in a headline: the name without legal
    form, state tag, parentheses or a leading "The" ("ULTA BEAUTY, INC." ->
    "ULTA BEAUTY"), and that again without generic trailing words ("PDD
    Holdings Inc." -> "PDD Holdings", "PDD").
    """
    s = unicodedata.normalize("NFKC", name).replace("’", "'").strip()
    s = re.sub(r"\s*[/\\][A-Za-z]*[/\\]?\s*", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"^the\s+", "", s, flags=re.I)
    for _ in range(3):
        s = re.sub(rf"[\s,]+(?:&|and)?\s*{_LEGAL_FORM}\.?\s*$", "", s, flags=re.I).strip(" ,.")
    full = re.sub(r"\s+", " ", s)
    words = full.split()
    while len(words) > 1 and words[-1].lower() in _GENERIC_TAIL:
        words = words[:-1]
    return {f for f in (full, " ".join(words)) if len(f) >= 2}


def is_distinctive(form: str) -> bool:
    """
    A form that names one company in a headline: several words, or one word
    that is neither an acronym nor in NEWS_NAME_WORDS. Measured 2026-10-26
    (decision 77): on stories Yahoo had not tagged, a distinctive name was
    about its company 14% of the time, a dictionary word 2% ("price target"),
    a short acronym ("US", "Dow") little better.
    """
    if len(form) <= 3 or (form.isupper() and len(form.replace(" ", "")) <= 4):
        return False
    return len(form.split()) > 1 or form.casefold() not in NEWS_NAME_WORDS


def company_names(
    symbol: str,
    asset_class: str,
    sec_title: Optional[str] = None,
    verified_name: Optional[str] = None,
    crypto_class: Optional[str] = None,
) -> List[str]:
    """
    What a headline calls the symbol's company: an equity's EDGAR title and
    NEWS_SEARCH_TERMS entry, a coin's verified name (not a stablecoin's or a
    tokenized asset's: "Tether Gold" names Tether). An ETF has none.
    """
    if asset_class == "equity":
        names = [sec_title, NEWS_SEARCH_TERMS.get(symbol)]
    elif asset_class == "crypto" and crypto_class == "crypto":
        names = [verified_name]
    else:
        names = []
    return [n for n in names if n and n.strip()]


def name_patterns(names: Iterable[str]) -> List[re.Pattern]:
    """Each distinctive form of `names`, matched whole-word, any case, with a trailing 's."""
    forms = sorted({f for n in names for f in name_forms(n) if is_distinctive(f)})
    bodies = [re.escape(f).replace(r"\ ", r"\s+") for f in forms]
    return [re.compile(rf"(?<![\w$]){b}(?:'s)?(?!\w)", re.I) for b in bodies]


def names_company(title: str, patterns: Iterable[re.Pattern]) -> bool:
    title = unicodedata.normalize("NFKC", title or "").replace("’", "'")
    return any(p.search(title) for p in patterns)


def is_full(returned: int, own: Optional[int]) -> bool:
    """
    Did the page hide older stories of THIS symbol? Only if it is full and
    mostly the symbol's own: a page of other tickers' news (Yahoo's generic
    feed, or KEY's 1 of 50) says nothing about how fast KEY's news moves.
    `own` None is a row from before it was counted: judged by size alone.
    """
    if returned < NEWS_ARCHIVE_FULL_PAGE:
        return False
    return own is None or own >= NEWS_ARCHIVE_OWN_SHARE * returned


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
    targets: Iterable[Tuple[str, str]],
    search: Search,
    sleep: Callable[[float], None],
    retries: int = NEWS_ARCHIVE_EMPTY_RETRIES,
    pause: float = NEWS_ARCHIVE_RETRY_PAUSE_SECONDS,
    workers: int = NEWS_ARCHIVE_WORKERS,
) -> List[Fetch]:
    """
    Search every (symbol, term), then retry the empty and failed ones
    `retries` times, `pause` seconds apart. Empty answers are mostly transient
    (42 of 51 on 2026-10-22), so one is never taken at its word.
    """
    fetches = [Fetch(symbol, term) for symbol, term in targets]

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
    """A (symbol, term)'s newest successful fetch, as the coverage log has it."""

    fetched_at: datetime
    returned: int
    oldest_published_at: Optional[datetime]
    own_tagged: Optional[int] = None
    #: The newest fetch that RETURNED stories, when it is older than this one
    #: (this one was empty). Truncation is measured from it: after
    #: real 05:00, empty 06:00, the 07:00 page must reach back to 05:00.
    stories_fetched_at: Optional[datetime] = None
    #: The (symbol, term)'s first successful fetch: how long a page that never
    #: answered has been asked (decision 69).
    first_fetched_at: Optional[datetime] = None
    #: The shortest span of the page's FULL fetches in the last
    #: NEWS_ARCHIVE_SPAN_LOOKBACK_DAYS (decision 75): how dense its news gets.
    shortest_span: Optional[timedelta] = None

    @property
    def never_answered(self) -> bool:
        """No fetch of this page has ever returned a story."""
        return self.returned == 0 and self.stories_fetched_at is None

    @property
    def stories_baseline(self) -> Optional[datetime]:
        """Since when stories could have been missed; None if none ever came."""
        if self.returned:
            return self.fetched_at
        return self.stories_fetched_at


def window_truncated(
    previous: Optional[LastFetch],
    returned: int,
    oldest: Optional[datetime],
    own: Optional[int] = None,
) -> Optional[bool]:
    """
    Were stories missed since the previous fetch? Only a FULL page (is_full)
    can hide older stories; a short one is everything Yahoo has. None:
    nothing to compare against (a first fetch, or an undated page).
    """
    if previous is None or oldest is None or previous.stories_baseline is None:
        return None
    return is_full(returned, own) and oldest > previous.stories_baseline


def due(
    last: Optional[LastFetch],
    now: datetime,
    interval: timedelta,
    never_answered_after: timedelta = timedelta(hours=NEWS_ARCHIVE_NEVER_ANSWERED_HOURS),
) -> bool:
    """
    The hourly pass refetches a symbol whose last full page would be outrun
    before the NEXT pass: elapsed + interval reaches the page's span. NVDA's
    span was 1.8 h, so it is refetched every hour; KO's (2.3 days) never is
    between the 06:00 runs. A term never fetched successfully is due, and so
    is one whose last answer was EMPTY: Yahoo's answer sticks for minutes, so
    the 06:00 retries a minute apart can all miss, but 5 of 23 empties had
    stories an hour later (COP, O, OMC, SBAC, NWSA).

    Except a page that has NEVER answered and was first fetched
    `never_answered_after` ago (decision 69): 51 such pages were 98% of the
    hourly searches. The 06:00 run still searches it, so one that comes alive
    is found the next morning.

    The span is the shorter of the last page's and the page's shortest full
    span in the last week (decision 75): the 06:00 page spans the quiet night,
    and NVDA waited 12 h on it while the day's 50 stories covered 3-5 h.
    """
    if last is None:
        return True
    if last.returned == 0:
        return not (
            last.never_answered
            and last.first_fetched_at is not None
            and now - last.first_fetched_at >= never_answered_after
        )
    if not is_full(last.returned, last.own_tagged) or last.oldest_published_at is None:
        return False
    span = last.fetched_at - last.oldest_published_at
    if last.shortest_span is not None:
        span = min(span, last.shortest_span)
    return (now - last.fetched_at) + interval >= span


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


async def last_fetches(session: Any, symbols: List[str]) -> Dict[Tuple[str, str], LastFetch]:
    """
    Each (symbol, term)'s newest fetch that did not fail, carrying when it
    last returned stories if that was earlier (an empty answer in between)
    and when it was first fetched.
    """
    from sqlalchemy import func, select

    from db.models import NewsFetchORM

    f = NewsFetchORM
    newest = await session.execute(
        select(
            f.symbol, f.search_term, f.fetched_at, f.returned, f.oldest_published_at, f.own_tagged
        )
        .where(f.symbol.in_(symbols), f.error.is_(None))
        .distinct(f.symbol, f.search_term)
        .order_by(f.symbol, f.search_term, f.fetched_at.desc())
    )
    with_stories = await session.execute(
        select(f.symbol, f.search_term, f.fetched_at)
        .where(f.symbol.in_(symbols), f.error.is_(None), f.returned > 0)
        .distinct(f.symbol, f.search_term)
        .order_by(f.symbol, f.search_term, f.fetched_at.desc())
    )
    first = await session.execute(
        select(f.symbol, f.search_term, func.min(f.fetched_at))
        .where(f.symbol.in_(symbols), f.error.is_(None))
        .group_by(f.symbol, f.search_term)
    )
    full_since = now_utc() - timedelta(days=NEWS_ARCHIVE_SPAN_LOOKBACK_DAYS)
    shortest = await session.execute(
        select(f.symbol, f.search_term, func.min(f.fetched_at - f.oldest_published_at))
        .where(
            f.symbol.in_(symbols),
            f.error.is_(None),
            f.fetched_at >= full_since,
            f.oldest_published_at.is_not(None),
            f.returned >= NEWS_ARCHIVE_FULL_PAGE,
            (f.own_tagged.is_(None)) | (f.own_tagged >= NEWS_ARCHIVE_OWN_SHARE * f.returned),
        )
        .group_by(f.symbol, f.search_term)
    )
    stories_at = {(s, term): at for s, term, at in with_stories.all()}
    first_at = {(s, term): at for s, term, at in first.all()}
    shortest_span = {(s, term): span for s, term, span in shortest.all()}
    return {
        (s, term): LastFetch(
            at,
            n,
            oldest,
            own,
            None if n else stories_at.get((s, term)),
            first_at.get((s, term)),
            shortest_span.get((s, term)),
        )
        for s, term, at, n, oldest, own in newest.all()
    }


@dataclass(frozen=True)
class Stored:
    symbol: str
    term: str
    returned: int
    new_stories: int
    window_truncated: Optional[bool]
    span: Optional[timedelta]
    error: Optional[str]
    own_tagged: Optional[int] = None


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
    own = None if fetch.error else own_tagged(fetch.symbol, fetch.items)
    truncated = None if fetch.error else window_truncated(previous, returned, oldest, own)
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
            own_tagged=own,
        )
    )
    await session.commit()
    return Stored(
        symbol=fetch.symbol,
        term=fetch.term,
        returned=returned,
        new_stories=new,
        window_truncated=truncated,
        span=(fetched_at - oldest) if oldest else None,
        error=fetch.error,
        own_tagged=own,
    )
