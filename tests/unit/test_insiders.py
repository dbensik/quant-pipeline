"""
core/insiders.py — SEC Form 4 parsing (decision 40a). No network.

msft_0000789019-26-000228.xml is a real filing (public, EDGAR, fetched
2026-10-16); edge_cases.xml is built by hand for what that one lacks.
"""

from datetime import date
from pathlib import Path

from core import insiders as ins

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "form4"


def parse(name, accn="0000000000-26-000001"):
    return ins.parse_form4((FIXTURES / name).read_text(), accn, "4", date(2026, 10, 15))


def test_a_real_filing_reads_as_filed():
    (row,) = parse("msft_0000789019-26-000228.xml", "0000789019-26-000228")
    assert row.issuer_cik == 789019
    assert row.reporter_name == "Coleman Amy" and row.reporter_cik == 2062356
    assert row.is_officer and not row.is_director
    assert row.officer_title == "EVP, Chief Human Resources Off"
    assert (row.code, row.acquired_disposed) == ("F", "D")
    assert row.shares == 31.944 and row.price == 533.69
    assert row.shares_after == 50925.4591
    assert row.transaction_date == date(2026, 10, 15)
    assert row.plan_10b5_1 is False


def test_a_price_given_only_as_a_footnote_is_missing_not_zero():
    rows = parse("edge_cases.xml")
    sale = rows[0]
    assert sale.code == "S" and sale.price is None
    assert sale.shares == 1000.0, "thousands separators are read"


def test_holdings_lines_are_not_transactions_and_derivatives_are_kept():
    rows = parse("edge_cases.xml")
    assert [(r.row_no, r.is_derivative, r.code) for r in rows] == [(0, False, "S"), (1, True, "M")]


def test_several_owners_share_the_lines():
    row = parse("edge_cases.xml")[0]
    assert row.reporter_name == "Example Fund LP; Example GP LLC"
    assert row.reporter_cik == 111
    assert row.is_ten_pct_owner and row.is_director, "roles are OR'd across owners"
    assert row.plan_10b5_1 is True, "'true' reads as a flag, not only '1'"


def test_the_raw_document_drops_the_style_directory():
    assert ins.raw_document("xslF345X06/form4.xml") == "form4.xml"
    assert ins.raw_document("form4.xml") == "form4.xml"
    assert (
        ins.document_url(789019, "0000789019-26-000228", "xslF345X06/form4.xml")
        == "https://www.sec.gov/Archives/edgar/data/789019/000078901926000228/form4.xml"
    )


def test_only_form_4s_since_the_date_are_listed():
    subs = {
        "filings": {
            "recent": {
                "form": ["4", "10-K", "4/A", "4"],
                "accessionNumber": ["a", "b", "c", "d"],
                "filingDate": ["2026-10-01", "2026-09-01", "2026-08-01", "2023-01-01"],
                "primaryDocument": ["x.xml", "k.htm", "y.xml", "z.xml"],
            }
        }
    }
    assert [f["accn"] for f in ins.form4_filings(subs, date(2024, 1, 1))] == ["a", "c"]
    assert ins.oldest_recent_filing(subs) == date(2023, 1, 1)


def test_the_summary_counts_only_discretionary_trades():
    rows = [
        {
            "code": "P",
            "is_derivative": False,
            "shares": 100,
            "price": 10.0,
            "reporter_name": "A",
            "plan_10b5_1": False,
        },
        {
            "code": "S",
            "is_derivative": False,
            "shares": 50,
            "price": 20.0,
            "reporter_name": "B",
            "plan_10b5_1": True,
        },
        {
            "code": "S",
            "is_derivative": False,
            "shares": 10,
            "price": None,
            "reporter_name": "B",
            "plan_10b5_1": True,
        },
        {
            "code": "A",
            "is_derivative": False,
            "shares": 999,
            "price": 0.0,
            "reporter_name": "C",
            "plan_10b5_1": False,
        },
        {
            "code": "S",
            "is_derivative": True,
            "shares": 5,
            "price": 1.0,
            "reporter_name": "D",
            "plan_10b5_1": False,
        },
    ]
    s = ins.summarise(rows)
    assert s["purchases"] == {
        "transactions": 1,
        "insiders": 1,
        "shares": 100,
        "value": 1000.0,
        "unpriced": 0,
        "under_10b5_1_plan": 0,
    }
    assert s["sales"]["transactions"] == 2 and s["sales"]["value"] == 1000.0
    assert s["sales"]["unpriced"] == 1 and s["sales"]["under_10b5_1_plan"] == 2


def test_paged_files_are_read_only_when_recent_stops_short():
    subs = {
        "filings": {
            "recent": {
                "form": ["4"],
                "accessionNumber": ["a"],
                "filingDate": ["2026-10-01"],
                "primaryDocument": ["x"],
            },
            "files": [
                {"name": "p1.json", "filingFrom": "2026-06-01", "filingTo": "2026-09-30"},
                {"name": "p2.json", "filingFrom": "2023-01-01", "filingTo": "2024-12-31"},
                {"name": "p3.json", "filingFrom": "2019-01-01", "filingTo": "2022-12-31"},
            ],
        }
    }
    assert [f["name"] for f in ins.pages_to_read(subs, date(2024, 10, 16))] == [
        "p1.json",
        "p2.json",
    ]
    assert ins.pages_to_read(subs, date(2026, 10, 1)) == [], "recent already reaches the date"


def test_a_paged_file_is_read_like_recent():
    page = {
        "form": ["4", "424B2"],
        "accessionNumber": ["a", "b"],
        "filingDate": ["2025-01-02"] * 2,
        "primaryDocument": ["x", "y"],
    }
    assert [f["accn"] for f in ins.form4_filings(page, date(2024, 1, 1))] == ["a"]
