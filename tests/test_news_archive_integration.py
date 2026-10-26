"""
core.news_archive.store_fetch against the real database: insert-only, first
sighting kept, and the coverage log.

    poetry run pytest -m integration tests/test_news_archive_integration.py
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text

from core.news_archive import Fetch, LastFetch, due, last_fetches, store_fetch
from db.session import get_session

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

SYMBOLS = ("ZZNEWS1", "ZZNEWS2")  # not registry symbols
T0 = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def item(uuid, title, hours_ago, tags):
    return {
        "uuid": uuid,
        "title": title,
        "publisher": "Wire",
        "link": f"https://x/{uuid}",
        "providerPublishTime": (T0 - timedelta(hours=hours_ago)).timestamp(),
        "type": "STORY",
        "relatedTickers": tags,
    }


async def clean(session):
    await session.execute(
        text("delete from news_story_symbols where symbol in ('ZZNEWS1', 'ZZNEWS2')")
    )
    await session.execute(text("delete from news_stories where story_id like 'zz-%'"))
    await session.execute(text("delete from news_fetches where symbol in ('ZZNEWS1', 'ZZNEWS2')"))
    await session.commit()


@pytest_asyncio.fixture(loop_scope="session")
async def session():
    try:
        async with get_session() as s:
            await s.execute(text("select 1 from news_stories limit 1"))
            await clean(s)
            yield s
            await clean(s)
    except OSError:
        pytest.skip("TimescaleDB is not running")


async def test_a_fetch_is_stored_insert_only_and_logged(session):
    first = Fetch(
        "ZZNEWS1",
        "ZZNEWS1",
        [item("zz-1", "Old title", 3, ["ZZNEWS1"]), item("zz-2", "Other", 1, ["FOXF"])],
        fetched_at=T0,
    )
    stored = await store_fetch(session, first, None)
    assert (stored.returned, stored.new_stories, stored.window_truncated) == (2, 2, None)

    # Seen again, retitled and retagged, under a second symbol and twice in one page.
    again = Fetch(
        "ZZNEWS2",
        "ZZNEWS2",
        [item("zz-1", "New title", 3, ["ZZNEWS2"]), item("zz-1", "New title", 3, ["ZZNEWS2"])],
        fetched_at=T0 + timedelta(hours=1),
    )
    stored = await store_fetch(session, again, None)
    assert stored.new_stories == 1

    story = (
        await session.execute(
            text(
                "select title, related_tickers, first_seen_at from news_stories where story_id = 'zz-1'"
            )
        )
    ).one()
    assert story == ("Old title", ["ZZNEWS1"], T0)  # the first sighting, as served
    sightings = (
        await session.execute(
            text(
                "select symbol, first_seen_at from news_story_symbols where story_id = 'zz-1' order by symbol"
            )
        )
    ).all()
    assert sightings == [("ZZNEWS1", T0), ("ZZNEWS2", T0 + timedelta(hours=1))]

    # The unrelated story is stored too: relevance is a read rule.
    assert (
        await session.execute(text("select count(*) from news_stories where story_id = 'zz-2'"))
    ).scalar() == 1

    # A repeat of the first fetch adds nothing but a log row.
    stored = await store_fetch(session, first, LastFetch(T0, 2, T0 - timedelta(hours=3)))
    assert stored.new_stories == 0
    logged = (
        await session.execute(
            text("select count(*), sum(new_stories) from news_fetches where symbol = 'ZZNEWS1'")
        )
    ).one()
    assert logged == (2, 2)


async def test_a_failed_fetch_is_a_log_row_and_stores_nothing(session):
    failed = Fetch(
        "ZZNEWS1", "ZZNEWS1", [], error="Yahoo's search returned no news field", fetched_at=T0
    )
    stored = await store_fetch(
        session, failed, LastFetch(T0 - timedelta(hours=1), 50, T0 - timedelta(hours=9))
    )
    assert (stored.error, stored.window_truncated) == (
        "Yahoo's search returned no news field",
        None,
    )
    row = (
        await session.execute(
            text("select returned, error from news_fetches where symbol = 'ZZNEWS1'")
        )
    ).one()
    assert row == (0, "Yahoo's search returned no news field")


async def test_a_full_page_newer_than_the_last_fetch_is_logged_truncated(session):
    page = [item(f"zz-p{i}", f"Story {i}", 0.5 + i / 100, ["ZZNEWS1"]) for i in range(50)]
    previous = LastFetch(T0 - timedelta(hours=2), 50, T0 - timedelta(hours=4))
    stored = await store_fetch(session, Fetch("ZZNEWS1", "ZZNEWS1", page, fetched_at=T0), previous)
    assert stored.window_truncated is True
    assert (
        await session.execute(
            text("select window_truncated, own_tagged from news_fetches where symbol = 'ZZNEWS1'")
        )
    ).one() == (True, 50)


async def test_a_page_of_other_tickers_is_logged_with_its_own_count_and_not_truncated(session):
    page = [item(f"zz-g{i}", f"Generic {i}", 0.5 + i / 100, ["SPY"]) for i in range(48)]
    page[0] = item("zz-g0", "Generic 0", 0.5, ["ZZNEWS1"])
    previous = LastFetch(T0 - timedelta(hours=2), 48, T0 - timedelta(hours=4), 1)
    stored = await store_fetch(session, Fetch("ZZNEWS1", "ZZNEWS1", page, fetched_at=T0), previous)
    assert (stored.own_tagged, stored.window_truncated) == (1, False)


async def test_each_term_of_a_symbol_has_its_own_last_fetch(session):
    await store_fetch(
        session,
        Fetch("ZZNEWS1", "ZZNEWS1", [item("zz-a", "A", 1, ["ZZNEWS1"])], fetched_at=T0),
        None,
    )
    await store_fetch(session, Fetch("ZZNEWS1", "Zz News Corp", [], fetched_at=T0), None)
    await store_fetch(
        session,
        Fetch("ZZNEWS1", "Zz News Corp", [], error="boom", fetched_at=T0 + timedelta(hours=1)),
        None,
    )
    last = await last_fetches(session, ["ZZNEWS1"])
    assert last == {
        ("ZZNEWS1", "ZZNEWS1"): LastFetch(T0, 1, T0 - timedelta(hours=1), 1, None, T0),
        # the failure is skipped
        ("ZZNEWS1", "Zz News Corp"): LastFetch(T0, 0, None, 0, None, T0),
    }


async def test_a_page_that_never_answered_carries_its_first_fetch(session):
    # Decision 69: the hourly pass drops such a page once its first fetch is 24 h old.
    for hours in (30, 20, 1):
        await store_fetch(
            session,
            Fetch("ZZNEWS1", "ZZNEWS1", [], fetched_at=T0 - timedelta(hours=hours)),
            None,
        )
    last = (await last_fetches(session, ["ZZNEWS1"]))[("ZZNEWS1", "ZZNEWS1")]
    assert last.never_answered
    assert (last.fetched_at, last.first_fetched_at) == (
        T0 - timedelta(hours=1),
        T0 - timedelta(hours=30),
    )
    assert due(last, T0, timedelta(hours=1)) is False


async def test_after_an_empty_answer_truncation_counts_from_the_last_stories(session):
    # 05:00 stories, 06:00 empty, 07:00 a full page back only to 05:30.
    t5 = T0 - timedelta(hours=7)
    await store_fetch(
        session,
        Fetch("ZZNEWS1", "ZZNEWS1", [item("zz-e1", "E", 7.5, ["ZZNEWS1"])], fetched_at=t5),
        None,
    )
    await store_fetch(
        session, Fetch("ZZNEWS1", "ZZNEWS1", [], fetched_at=t5 + timedelta(hours=1)), None
    )
    previous = (await last_fetches(session, ["ZZNEWS1"]))[("ZZNEWS1", "ZZNEWS1")]
    assert (previous.returned, previous.stories_fetched_at) == (0, t5)
    page = [item(f"zz-f{i}", f"F {i}", 6.5 - i / 100, ["ZZNEWS1"]) for i in range(50)]
    stored = await store_fetch(
        session,
        Fetch("ZZNEWS1", "ZZNEWS1", page, fetched_at=t5 + timedelta(hours=2)),
        previous,
    )
    assert stored.window_truncated is True


async def test_last_fetches_carries_the_shortest_full_span_of_the_week(session):
    # Decision 75. Relative to now: the look-back is measured from the clock.
    now = datetime.now(timezone.utc).replace(microsecond=0)

    def page(tag, at, span_hours):
        return [
            {
                **item(f"zz-{tag}{i}", f"{tag} {i}", 0, ["ZZNEWS1"]),
                "providerPublishTime": (at - timedelta(hours=span_hours * i / 49)).timestamp(),
            }
            for i in range(50)
        ]

    for tag, at, span in (
        ("old", now - timedelta(days=10), 1.0),  # denser, but outside the week
        ("day", now - timedelta(days=2), 3.0),
        ("night", now - timedelta(hours=1), 12.0),
    ):
        await store_fetch(
            session, Fetch("ZZNEWS1", "ZZNEWS1", page(tag, at, span), fetched_at=at), None
        )
    last = (await last_fetches(session, ["ZZNEWS1"]))[("ZZNEWS1", "ZZNEWS1")]
    assert last.shortest_span == timedelta(hours=3)
    assert due(last, now + timedelta(hours=1), timedelta(hours=1)) is True
