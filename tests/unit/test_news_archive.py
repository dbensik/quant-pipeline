"""
core/news_archive.py: the headline archive's pure rules (decision 57 B,
research/news-archive-2026-10-22.md). No network, no database; the writes are
in tests/test_news_archive_integration.py.
"""

import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from config.settings import (
    CRYPTO_PROVIDER_SYMBOLS,
    NEWS_ARCHIVE_FULL_PAGE,
    NEWS_ARCHIVE_PER_SYMBOL,
    NEWS_SEARCH_TERMS,
    NEWS_TICKER_PAGES_REVIEWED,
)
from core.news_archive import (
    LastFetch,
    Stored,
    due,
    fetch_all,
    is_full,
    own_tagged,
    own_tickers,
    search_term,
    search_terms,
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


def test_a_coin_is_also_searched_by_its_name():
    # 2026-10-22: BTC returned 0 stories while Bitcoin returned 43 tagged BTC-USD.
    assert search_terms("BTC-USD", "crypto", "Bitcoin") == ["BTC", "Bitcoin"]


def test_a_coin_whose_name_is_its_symbol_is_searched_once():
    assert search_terms("XRP-USD", "crypto", "XRP") == ["XRP"]
    assert search_terms("BNB-USD", "crypto", "bnb") == ["BNB"]
    assert search_terms("PEPE-USD", "crypto", None) == ["PEPE"]
    assert search_terms("PEPE-USD", "crypto", "  ") == ["PEPE"]


def test_a_word_ticker_is_also_searched_by_its_listed_term():
    assert search_terms("KEY", "equity") == ["KEY", NEWS_SEARCH_TERMS["KEY"]]
    assert search_terms("NVDA", "equity") == ["NVDA"]


def test_an_equity_is_never_searched_by_a_name_it_does_not_list():
    # The SEC title is not a term: "HASBRO, INC" returned Yahoo's generic feed.
    assert search_terms("AAPL", "equity", "Apple Inc.") == ["AAPL"]


def test_listed_terms_avoid_the_shapes_measured_to_fail():
    for symbol, term in NEWS_SEARCH_TERMS.items():
        assert "," not in term, symbol  # "HASBRO, INC": the generic feed
        assert term.casefold() != symbol.casefold(), symbol
    assert not set(NEWS_SEARCH_TERMS) & set(NEWS_TICKER_PAGES_REVIEWED)


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
        f.symbol: f
        for f in fetch_all([("AFL", "AFL"), ("KO", "KO")], search, pauses.append, pause=60)
    }
    assert len(out["AFL"].items) == 1 and out["AFL"].empty_attempts == 1
    assert pauses == [60]
    assert search.calls.count("KO") == 1  # an answered symbol is not asked again


def test_an_empty_answer_stops_after_the_retries():
    search = Scripted({"ATO": [{"news": []}]})
    pauses = []
    (fetch,) = fetch_all([("ATO", "ATO")], search, pauses.append, retries=2, pause=60)
    assert (fetch.items, fetch.empty_attempts, fetch.error) == ([], 3, None)
    assert pauses == [60, 60]


def test_no_pause_when_nothing_needs_a_retry():
    pauses = []
    fetch_all([("KO", "KO")], Scripted({"KO": [{"news": [item("k")]}]}), pauses.append)
    assert pauses == []


def test_a_response_without_a_news_field_is_a_failure_not_no_news():
    (fetch,) = fetch_all([("KO", "KO")], Scripted({"KO": [{"quotes": []}]}), lambda s: None)
    assert fetch.error and "no news field" in fetch.error
    assert fetch.empty_attempts == 0


def test_a_transport_error_is_retried_and_cleared_by_an_answer():
    search = Scripted({"KO": [ConnectionError("reset"), {"news": [item("k")]}]})
    (fetch,) = fetch_all([("KO", "KO")], search, lambda s: None)
    assert fetch.error is None and len(fetch.items) == 1


def test_a_coin_is_fetched_under_its_term_and_kept_under_its_symbol():
    search = Scripted({"ETH": [{"news": [item("e", tags=("ETH-USD",))]}]})
    (fetch,) = fetch_all([("ETH-USD", "ETH")], search, lambda s: None)
    assert (fetch.symbol, fetch.term, search.calls) == ("ETH-USD", "ETH", ["ETH"])


def test_two_terms_of_one_symbol_are_two_fetches():
    search = Scripted(
        {"BTC": [{"news": []}], "Bitcoin": [{"news": [item("b", tags=("BTC-USD",))]}]}
    )
    out = fetch_all([("BTC-USD", "BTC"), ("BTC-USD", "Bitcoin")], search, lambda s: None, retries=0)
    assert [(f.symbol, f.term, len(f.items)) for f in out] == [
        ("BTC-USD", "BTC", 0),
        ("BTC-USD", "Bitcoin", 1),
    ]


def test_no_retries_means_one_search_and_no_pause():
    # The hourly pass: an empty answer waits for the next hour, not a minute.
    search = Scripted({"CASY": [{"news": []}]})
    pauses = []
    (fetch,) = fetch_all([("CASY", "CASY")], search, pauses.append, retries=0)
    assert (search.calls, pauses, fetch.empty_attempts) == (["CASY"], [], 1)


# --- what is stored --------------------------------------------------------


def test_a_story_is_stored_with_its_tags_as_served_unfiltered():
    (row,) = story_rows([item("u1", tags=("FOXF",))])  # not FOX's: kept anyway
    assert row["story_id"] == "u1"
    assert row["related_tickers"] == ["FOXF"]
    assert row["story_type"] == "STORY"
    assert row["published_at"] == T0 - HOUR


def test_own_tagged_counts_the_symbol_s_own_tag_only():
    page = [item("a", tags=("KEY",)), item("b", tags=("KEYS", "K")), item("c", tags=())]
    assert own_tagged("KEY", page) == 1


def test_a_mapped_coin_s_provider_ticker_is_its_own_tag():
    # 2026-10-22: 'Uniswap' stories tagged UNI7083-USD, 3 of 10 without UNI-USD.
    assert CRYPTO_PROVIDER_SYMBOLS["UNI-USD"] == "UNI7083-USD"
    page = [
        item("a", tags=("UNI7083-USD",)),
        item("b", tags=("UNI7083-USD", "UNI-USD")),  # one story, counted once
        item("c", tags=("ETH-USD",)),
    ]
    assert own_tickers("UNI-USD") == {"UNI-USD", "UNI7083-USD"}
    assert own_tagged("UNI-USD", page) == 2
    assert own_tickers("ETH-USD") == {"ETH-USD"}  # unmapped: the symbol alone


def test_a_full_page_of_provider_tagged_stories_is_due_hourly():
    # Hyperliquid's page: 49 stories, nearly all tagged HYPE32196-USD alone.
    # Counted by the symbol, it was "mostly others" and never refetched.
    provider = CRYPTO_PROVIDER_SYMBOLS["HYPE-USD"]
    page = [item(str(i), tags=(provider,)) for i in range(49)]
    own = own_tagged("HYPE-USD", page)
    assert own == 49
    last = LastFetch(T0 - HOUR, 49, T0 - 2 * HOUR, own)
    assert due(last, T0, HOUR) is True


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


def test_a_full_page_of_other_tickers_missed_nothing_of_ours():
    # KEY's ticker page: 1 of 50 tagged KEY, spanning 6.4 h.
    assert window_truncated(previous(), 50, T0 - HOUR, own=1) is False
    assert window_truncated(previous(), 50, T0 - HOUR, own=25) is True


def test_is_full_needs_half_the_page_to_be_the_symbol_s():
    assert is_full(50, 25) is True
    assert is_full(50, 24) is False
    assert is_full(48, 0) is False  # Yahoo's generic feed
    assert is_full(50, None) is True  # a row from before own_tagged: by size
    assert is_full(NEWS_ARCHIVE_FULL_PAGE - 1, NEWS_ARCHIVE_FULL_PAGE - 1) is False


def test_an_empty_answer_between_does_not_hide_a_loss():
    # Stories at 05:00 (T0 - 3h), empty at 06:00, a full page reaching back
    # only to 05:30: 05:00-05:30 was missed, whatever the empty one says.
    empty = LastFetch(T0 - 2 * HOUR, 0, None, 0, T0 - 3 * HOUR)
    assert window_truncated(empty, 50, T0 - 2.5 * HOUR, own=50) is True
    assert window_truncated(empty, 50, T0 - 3.5 * HOUR, own=50) is False


def test_a_symbol_that_never_returned_stories_cannot_say():
    assert window_truncated(LastFetch(T0 - HOUR, 0, None, 0), 50, T0 - HOUR / 2, own=50) is None


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


def test_an_empty_last_answer_is_due_next_hour():
    # 5 of 23 empties at 14:55 had stories by 15:45; none changed within a minute.
    assert due(LastFetch(T0 - timedelta(minutes=5), 0, None, 0), T0, HOUR) is True


def never_answered(first_ago):
    """A page whose every fetch was empty, last asked 5 minutes ago."""
    return LastFetch(T0 - timedelta(minutes=5), 0, None, 0, None, T0 - first_ago)


def test_a_new_page_that_never_answered_is_still_due_hourly():
    # 22 new pages started empty and answered, all within 14 h (ICP's name: 14 empties).
    assert due(never_answered(timedelta(hours=23)), T0, HOUR) is True


def test_an_old_page_that_never_answered_is_left_to_the_06_00_run():
    # 51 crypto pages empty for 48 h made 2,376 of 2,431 hourly searches (decision 69).
    assert due(never_answered(timedelta(hours=25)), T0, HOUR) is False
    assert due(never_answered(timedelta(hours=24)), T0, HOUR) is False


def test_a_page_that_answered_once_is_due_hourly_whatever_its_age():
    once = LastFetch(T0 - timedelta(minutes=5), 0, None, 0, T0 - 30 * HOUR, T0 - 40 * HOUR)
    assert due(once, T0, HOUR) is True


def test_a_page_without_a_first_fetch_is_due_when_empty():
    assert due(LastFetch(T0 - timedelta(minutes=5), 0, None, 0), T0, HOUR) is True


def test_a_page_mostly_about_other_tickers_is_never_due():
    # DE's ticker page: 1 of 47 tagged DE over 3.4 h. Refetching stores noise.
    last = LastFetch(T0 - 3 * HOUR, 47, T0 - 3 * HOUR - timedelta(hours=3.4), 1)
    assert due(last, T0, HOUR) is False
    assert due(LastFetch(last.fetched_at, 47, last.oldest_published_at, 47), T0, HOUR) is True


# --- review lines ----------------------------------------------------------


def stored(symbol, term, returned, own, error=None):
    return Stored(symbol, term, returned, 0, None, None, error, own)


def test_an_unlisted_word_ticker_is_reported_for_review():
    from scripts.archive_news import review_lines

    lines = review_lines([stored("IT", "IT", 49, 0)])
    assert len(lines) == 1 and lines[0].startswith("WORD TICKER? IT")


def test_a_ticker_page_is_not_reported_when_listed_reviewed_or_mostly_its_own():
    from scripts.archive_news import review_lines

    assert (
        review_lines(
            [
                stored("KEY", "KEY", 50, 1),  # listed: its term is searched
                stored("GOOGL", "GOOGL", 49, 3),  # reviewed: tagged GOOG
                stored("NVDA", "NVDA", 49, 49),
                stored("EWA", "EWA", 4, 0),  # a short page is all there is
                stored("ETH-USD", "ETH", 50, 0),  # a coin is not a word ticker
            ]
        )
        == []
    )


def test_a_listed_term_that_comes_back_empty_or_others_is_weak():
    from scripts.archive_news import review_lines

    term = NEWS_SEARCH_TERMS["HAS"]
    assert [x.split(":")[0] for x in review_lines([stored("HAS", term, 48, 0)])] == [
        f"WEAK TERM HAS '{term}'"
    ]
    assert len(review_lines([stored("HAS", term, 0, 0)])) == 1
    assert review_lines([stored("HAS", term, 50, 50)]) == []
    assert review_lines([stored("HAS", term, 0, None, error="boom")]) == []
