"""
scripts/probe_tiingo_delisted.py — the judgements, not the network.

The probe decides whether a paid-for or free source can replace Yahoo for
names Yahoo dropped, so a verdict that flatters the source is the failure to
guard against: a reused ticker's bars must not read as coverage.
"""

from datetime import date

import pandas as pd

from scripts.probe_tiingo_delisted import (
    Tiingo,
    pick_sample,
    probe,
    return_mismatches,
    window_coverage,
)

FIRST, LAST = date(2019, 1, 2), date(2019, 12, 31)


def sessions(start, end):
    return [stamp.date() for stamp in pd.bdate_range(start, end)]


def test_a_full_window_is_covered():
    inside, expected, verdict = window_coverage(sessions(FIRST, LAST), FIRST, LAST)
    assert verdict == "covered" and inside == expected


def test_market_holidays_do_not_make_a_window_partial():
    bars = sessions(FIRST, LAST)[:-9]  # nine holidays in a year
    assert window_coverage(bars, FIRST, LAST)[2] == "covered"


def test_half_a_window_is_partial():
    bars = sessions(date(2019, 7, 1), LAST)
    inside, expected, verdict = window_coverage(bars, FIRST, LAST)
    assert verdict == "partial" and inside < expected


def test_bars_only_after_the_window_are_not_coverage():
    """The reused-ticker case: plenty of bars, none of them this company's."""
    bars = sessions(date(2021, 1, 4), date(2021, 12, 31))
    assert window_coverage(bars, FIRST, LAST) == (0, len(sessions(FIRST, LAST)), "outside")


def test_no_bars_is_empty():
    assert window_coverage([], FIRST, LAST)[2] == "empty"


def test_return_mismatches_counts_only_days_that_differ():
    index = sessions(date(2019, 1, 2), date(2019, 1, 15))
    ours = pd.Series([100 + i for i in range(len(index))], index=index, dtype=float)
    theirs = ours * 2.0  # a different adjustment LEVEL, identical returns
    assert return_mismatches(ours, theirs) == (len(index) - 1, 0)

    theirs.iloc[5] *= 1.05  # one bad bar breaks the return into it and out of it
    assert return_mismatches(ours, theirs) == (len(index) - 1, 2)


def test_sample_takes_every_case_before_filling_up():
    candidates = pd.DataFrame(
        {
            "symbol": [f"P{i}" for i in range(20)] + ["R1", "N1", "C1"],
            "case": ["plain"] * 20 + ["reused", "renamed", "control"],
        }
    )
    picked = pick_sample(candidates, 8)
    assert len(picked) == 8
    assert {"reused", "renamed", "control"} <= set(picked["case"])


class FakeTiingo:
    def __init__(self, bars, meta):
        self._bars, self._meta = bars, meta

    def meta(self, ticker):
        return self._meta

    def prices(self, ticker, start, end):
        dates = [d for d in self._bars if start <= d <= end]
        return pd.DataFrame({"adjClose": [1.0] * len(dates)}, index=dates)


def candidate(**overrides):
    row = {
        "symbol": "PARA", "case": "reused", "tiingo_ticker": "PARA",
        "first": "2019-01-02", "last": "2019-12-31",
    }
    return pd.DataFrame([{**row, **overrides}])


def test_a_reused_ticker_with_no_bars_in_the_window_reports_whose_series_it_is():
    tiingo = FakeTiingo(
        bars=sessions(date(2021, 2, 12), date(2021, 6, 30)),
        meta={"name": "Someone Else Inc", "startDate": "2021-02-12", "endDate": "2026-10-02"},
    )
    (result,) = probe(candidate(), tiingo)
    assert result["verdict"] == "outside"
    assert result["tiingo_name"] == "Someone Else Inc"
    assert result["tiingo_start"] == "2021-02-12"


def test_a_ticker_with_no_tiingo_candidate_is_reported_not_skipped():
    (result,) = probe(candidate(tiingo_ticker=""), FakeTiingo([], None))
    assert result["verdict"] == "no_candidate"


def test_the_limiter_waits_instead_of_sending_past_the_hourly_limit(monkeypatch):
    slept = []
    monkeypatch.setattr("scripts.probe_tiingo_delisted.time.sleep", slept.append)

    class Response:
        status_code = 200

        def json(self):
            return []

        def raise_for_status(self):
            pass

    tiingo = Tiingo({"Authorization": "Token x"}, per_hour=5)
    monkeypatch.setattr(tiingo.session, "get", lambda *a, **k: Response())

    for _ in range(3):
        tiingo._get("https://example.test")
    assert slept == []
    tiingo._get("https://example.test")
    assert len(slept) == 1 and slept[0] > 3000
