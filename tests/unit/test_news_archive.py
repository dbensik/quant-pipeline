"""
core/news_archive.py: the headline archive's pure rules (decision 57 B,
research/news-archive-2026-10-22.md). No network, no database; the writes are
in tests/test_news_archive_integration.py.
"""

import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from config.settings import NEWS_ARCHIVE_FULL_PAGE, NEWS_ARCHIVE_PER_SYMBOL
from core.news_archive import (
    LastFetch,
    due,
    fetch_all,
    search_term,
    story_rows,
    window_truncated,
)

T0 = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)


def item(uuid, title="A story", hours_ago=1.0, tags=("AAPL",)):
    return {
        "uuid": uuid,
        "title": title,
        "publisher": "Wire",
        "link": f"https://x/{uuid}",
        "providerPublishTime": (T0 - timedelta(hours=hours_ago)).timestamp(),
        "type": "STORY",
        "relatedTickers": list(tags),
    }


# --- what is searched ------------------------------------------------------


def test_a_coin_is_searched_by_its_base_symbol():
    # ETH-USD returned 0 stories on 2026-10-22; ETH returned 50, 49 tagged ETH-USD.
    assert search_term("ETH-USD", "crypto") == "ETH"


def test_an_equity_with_a_dash_is_searched_whole():
    assert search_term("BRK-B", "equity") == "BRK-B"
    assert search_term("SPY", "etf") == "SPY"


def test_fifty_is_asked_and_never_more(monkeypatch):
    # AMCR: 50 at news_count=50, NOTHING at 100 or 300.
    assert NEWS_ARCHIVE_PER_SYMBOL == 50
    calls = []

    class Search:
        def __init__(self, term, news_count, max_results):
            calls.append((term, news_count))
            self.response = {"news": []}

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Search=Search))
    from scripts.archive_news import yahoo_search

    yahoo_search("AMCR")
    assert calls == [("AMCR", 50)]


# --- fetching and retries --------------------------------------------------


class Scripted:
    """Answers per term, one per call, the last repeated."""

    def __init__(self, answers):
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls = []

    def __call__(self, term):
        self.calls.append(term)
        queue = self.answers[term]
        answer = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


def test_an_empty_answer_is_retried_after_a_pause():
    search = Scripted({"AFL": [{"news": []}, {"news": [item("a")]}], "KO": [{"news": [item("k")]}]})
    pauses = []
    out = {
        f.symbol: f for f in fetch_all({"AFL": "AFL", "KO": "KO"}, search, pauses.append, pause=60)
    }
    assert len(out["AFL"].items) == 1 and out["AFL"].empty_attempts == 1
    assert pauses == [60]
    assert search.calls.count("KO") == 1  # an answered symbol is not asked again


def test_an_empty_answer_stops_after_the_retries():
    search = Scripted({"ATO": [{"news": []}]})
    pauses = []
    (fetch,) = fetch_all({"ATO": "ATO"}, search, pauses.append, retries=2, pause=60)
    assert (fetch.items, fetch.empty_attempts, fetch.error) == ([], 3, None)
    assert pauses == [60, 60]


def test_no_pause_when_nothing_needs_a_retry():
    pauses = []
    fetch_all({"KO": "KO"}, Scripted({"KO": [{"news": [item("k")]}]}), pauses.append)
    assert pauses == []


def test_a_response_without_a_news_field_is_a_failure_not_no_news():
    (fetch,) = fetch_all({"KO": "KO"}, Scripted({"KO": [{"quotes": []}]}), lambda s: None)
    assert fetch.error and "no news field" in fetch.error
    assert fetch.empty_attempts == 0


def test_a_transport_error_is_retried_and_cleared_by_an_answer():
    search = Scripted({"KO": [ConnectionError("reset"), {"news": [item("k")]}]})
    (fetch,) = fetch_all({"KO": "KO"}, search, lambda s: None)
    assert fetch.error is None and len(fetch.items) == 1


def test_a_coin_is_fetched_under_its_term_and_kept_under_its_symbol():
    search = Scripted({"ETH": [{"news": [item("e", tags=("ETH-USD",))]}]})
    (fetch,) = fetch_all({"ETH-USD": "ETH"}, search, lambda s: None)
    assert (fetch.symbol, fetch.term, search.calls) == ("ETH-USD", "ETH", ["ETH"])


# --- what is stored --------------------------------------------------------


def test_a_story_is_stored_with_its_tags_as_served_unfiltered():
    (row,) = story_rows([item("u1", tags=("FOXF",))])  # not FOX's: kept anyway
    assert row["story_id"] == "u1"
    assert row["related_tickers"] == ["FOXF"]
    assert row["story_type"] == "STORY"
    assert row["published_at"] == T0 - HOUR


def test_a_story_without_an_id_or_title_is_skipped():
    assert story_rows([item(""), {**item("u2"), "title": None}, item("u3")]) == story_rows(
        [item("u3")]
    )


# --- coverage --------------------------------------------------------------


def previous(at=T0 - 2 * HOUR, returned=50, oldest=None):
    return LastFetch(at, returned, oldest)


def test_a_full_page_newer_than_the_last_fetch_missed_stories():
    assert window_truncated(previous(), 50, T0 - HOUR) is True


def test_a_full_page_reaching_back_past_the_last_fetch_missed_nothing():
    assert window_truncated(previous(), 50, T0 - 3 * HOUR) is False


def test_a_short_page_is_everything_yahoo_has():
    assert window_truncated(previous(), NEWS_ARCHIVE_FULL_PAGE - 1, T0 - HOUR) is False


def test_forty_nine_of_fifty_is_a_full_page():
    # AAPL and NVDA often return 49.
    assert window_truncated(previous(), 49, T0 - HOUR) is True


def test_a_first_fetch_cannot_say():
    assert window_truncated(None, 50, T0 - HOUR) is None


@pytest.mark.parametrize(
    "span, elapsed, expected",
    [
        (timedelta(hours=1.8), HOUR, True),  # NVDA: outrun before the next pass
        (timedelta(hours=1.8), timedelta(minutes=30), False),
        (timedelta(days=2.3), timedelta(hours=10), False),  # KO: the 06:00 run covers it
        (timedelta(hours=7.6), timedelta(hours=6.6), True),  # MSFT: at the boundary
    ],
)
def test_the_hourly_pass_refetches_a_page_about_to_be_outrun(span, elapsed, expected):
    last = LastFetch(T0 - elapsed, 50, T0 - elapsed - span)
    assert due(last, T0, HOUR) is expected


def test_a_short_page_is_never_due():
    # Twelve stories in two hours is everything Yahoo had, however recent.
    assert due(LastFetch(T0 - HOUR, 12, T0 - 2 * HOUR), T0, HOUR) is False


def test_a_symbol_never_fetched_is_due():
    assert due(None, T0, HOUR) is True
