"""
core/membership_reconstruction.py and scripts/reconstruct_sp500_membership.py.

What is stored here becomes the answer to "who was in the index that month",
and nobody re-reads it. So the behaviour that matters is what happens when a
revision is wrong: vandalised, half-edited, or laid out differently. A wrong
list that looks healthy must be rejected, and an older revision tried.
"""

from datetime import date, datetime, timezone

from core.membership_reconstruction import judge, month_ends, parse_constituents
from scripts import reconstruct_sp500_membership as script

RANGE = (480, 520)


def page(symbol_header: str, symbols: list[str], with_changes: bool = True) -> str:
    """A revision: a constituents table and, optionally, the changes table."""
    rows = "".join(
        f"<tr><td>{s}</td><td>{s} Inc.</td></tr>" for s in symbols
    )
    html = (
        f"<table><tr><th>{symbol_header}</th><th>Security</th></tr>{rows}</table>"
    )
    if with_changes:
        html += (
            "<table><tr><th>Date</th><th>Added</th><th>Removed</th></tr>"
            "<tr><td>2015-01-01</td><td>ZZZZ</td><td>YYYY</td></tr></table>"
        )
    return html


def members(n: int = 500) -> list[str]:
    """n distinct, well-formed tickers."""
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return [letters[i // 26 % 26] + letters[i % 26] + "X" for i in range(n)][:n]


# --- parse_constituents -----------------------------------------------------


def test_finds_the_table_under_the_pre_2018_header():
    assert parse_constituents(page("Ticker symbol", ["MMM", "FB"])) == ["MMM", "FB"]


def test_finds_the_table_under_the_current_header():
    assert parse_constituents(page("Symbol", ["MMM", "META"])) == ["MMM", "META"]


def test_does_not_read_tickers_out_of_the_changes_table():
    symbols = parse_constituents(page("Symbol", ["MMM"]))
    assert "ZZZZ" not in symbols and "YYYY" not in symbols


def test_a_page_with_no_constituents_table_gives_nothing():
    assert parse_constituents("<p>blanked</p>") == []
    assert parse_constituents("<table><tr><th>vte</th></tr><tr><td>x</td></tr></table>") == []


# --- judge ------------------------------------------------------------------


def test_accepts_a_normal_month():
    previous = members(500)
    current = previous[:-3] + ["NEWA", "NEWB", "NEWC"]
    assert judge(current, previous, RANGE, 40) is None


def test_rejects_a_short_list():
    assert "250 symbols" in judge(members(250), None, RANGE, 40)


def test_rejects_a_right_sized_list_with_the_wrong_contents():
    """The case the count check cannot see: 500 rows, but not last month's."""
    previous = members(500)
    vandalised = previous[:450] + [f"Q{s}"[:4] for s in members(50)]
    assert len(vandalised) == 500
    assert "differ from the previous month" in judge(vandalised, previous, RANGE, 40)


def test_rejects_rows_that_are_not_tickers():
    current = members(499) + ["See notes"]
    assert "not tickers" in judge(current, None, RANGE, 40)


def test_rejects_a_repeated_symbol():
    current = members(499) + ["AAX"]
    assert "repeated symbols: AAX" in judge(current, None, RANGE, 40)


def test_share_class_tickers_are_tickers():
    current = members(498) + ["BRK.B", "BF.B"]
    assert judge(current, None, RANGE, 40) is None


def test_month_ends_cover_both_ends():
    assert month_ends(date(2014, 12, 31), date(2015, 2, 28)) == [
        date(2014, 12, 31), date(2015, 1, 31), date(2015, 2, 28),
    ]


# --- reconstruct_month ------------------------------------------------------


class FakeWiki:
    """Revisions newest first, as MediaWiki returns them with rvdir=older."""

    def __init__(self, pages: dict[int, str]):
        self.pages = pages
        self.fetched: list[int] = []

    def revisions_at(self, day, limit):
        stamp = datetime(2020, 1, 30, tzinfo=timezone.utc)
        return [(revision_id, stamp) for revision_id in self.pages][:limit]

    def html(self, revision_id):
        self.fetched.append(revision_id)
        return self.pages[revision_id]


def test_uses_the_newest_revision_when_it_is_sound():
    good = members(500)
    wiki = FakeWiki({30: page("Symbol", good), 20: page("Symbol", members(490))})

    revision_id, _, symbols = script.reconstruct_month(wiki, date(2020, 1, 31), good)

    assert revision_id == 30 and symbols == good
    assert wiki.fetched == [30]


def test_steps_back_past_a_vandalised_revision():
    good = members(500)
    wiki = FakeWiki(
        {
            30: "<p>page blanked</p>",
            20: page("Symbol", good[:100]),
            10: page("Symbol", good),
        }
    )

    revision_id, _, symbols = script.reconstruct_month(wiki, date(2020, 1, 31), good)

    assert revision_id == 10 and symbols == good


def test_gives_up_rather_than_store_a_bad_month():
    wiki = FakeWiki({30: "<p>blanked</p>", 20: page("Symbol", members(100))})
    assert script.reconstruct_month(wiki, date(2020, 1, 31), members(500)) is None
