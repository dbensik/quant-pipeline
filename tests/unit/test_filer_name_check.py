"""scripts/load_fundamentals.py — the CIK identity line flags names, not spellings.

Until 2026-10-26 it compared the first 12 characters, so CSX, 3M, Netflix and
86 others printed MISMATCH? every morning they filed, and the 3 lines that
name a different entity were buried among them.
"""

import pytest

from scripts.load_fundamentals import same_filer_name

# (EDGAR ticker-file title, companyfacts entityName), as served.
SAME = [
    ("CSX CORP", "CSX CORPORATION"),
    ("COMCAST CORP", "COMCAST\xa0CORPORATION"),
    ("HORTON D R INC /DE/", "D.R. Horton, Inc."),
    ("MOODYS CORP /DE/", "Moody’s Corporation"),
    ("CAMPBELL'S Co", "The Campbell’s Company"),
    ("ELI LILLY & Co", "ELI LILLY AND COMPANY"),
    ("YUM BRANDS INC", "YUM! BRANDS, INC."),
    ("US BANCORP DE", "US BANCORP \\DE\\"),
    ("WELLS FARGO & COMPANY/MN", "WELLS        FARGO & COMPANY/MN"),
    ("CHARTER COMMUNICATIONS, INC. /MO/", "Charter Communications, Inc."),
    ("HUNTINGTON BANCSHARES INC /MD/", "Huntington Bancshares Incorporated"),
]

DIFFERENT = [
    ("BANK OF AMERICA CORP /DE/", "BofA Finance LLC"),
    ("FRANKLIN TEMPLETON INC", "FRANKLIN RESOURCES INC"),
    ("Vistra Corp.", "Vistra Operations Company LLC"),
]


@pytest.mark.parametrize(("title", "entity"), SAME)
def test_one_filer_spelled_two_ways_is_not_flagged(title, entity):
    assert same_filer_name(title, entity)


@pytest.mark.parametrize(("title", "entity"), DIFFERENT)
def test_a_different_name_is_still_flagged(title, entity):
    assert not same_filer_name(title, entity)
