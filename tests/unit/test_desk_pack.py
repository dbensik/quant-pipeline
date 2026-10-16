"""
core/desk_pack.py — the Equity Research Desk's data pack (decision 39).

The options-implied probability was checked against SPY's raw put quotes on
2026-10-15: the strike-slope of puts around 700 for the 2026-12-31 expiry
(699: 4.76, 701: 4.895 mid) gives P(below 700) ~ 0.068; the pack gave 0.067.
"""

import json
import math
from datetime import date, timedelta

import pytest

from core import desk_pack as dp

# --- screen -------------------------------------------------------------------


def test_a_utility_is_refused_before_anything_is_gathered():
    s = dp.screen("DUK", "equity", "Utilities")
    assert s["refuse"] is True
    assert "Step 2" in s["reason"]


def test_another_sector_passes_but_still_needs_the_desks_judgement():
    s = dp.screen("MSFT", "equity", "Information Technology")
    assert s["refuse"] is False
    assert "judgement is still required" in s["reason"]


# --- prices -------------------------------------------------------------------


def _series(values, start=date(2020, 1, 1)):
    return [start + timedelta(days=i) for i in range(len(values))]


def test_returns_and_drawdown_from_total_return_closes():
    closes = [100.0] * 400 + [80.0] * 10 + [120.0]
    days = _series(closes)
    stats = dp.price_stats(days, closes, closes, [1.0] * len(closes), 365)
    assert stats["total_return"]["1y"] == pytest.approx(120 / 100 - 1)
    assert stats["max_drawdown_full"] == pytest.approx(-0.2)
    assert stats["high_52w"] == 120.0 and stats["low_52w"] == 80.0
    assert stats["total_return"]["5y"] is None, "the series does not reach back five years"


def test_quoted_price_and_range_use_split_adjusted_closes():
    total = [100.0, 110.0]
    split = [50.0, 52.0]
    stats = dp.price_stats(_series(total), total, split, [10.0, 10.0], 252)
    assert stats["last_close"] == 52.0
    assert stats["total_return"]["1m"] is None


def test_month_end_closes_keep_each_months_last_bar():
    days = [date(2024, 1, 30), date(2024, 1, 31), date(2024, 2, 1)]
    assert dp.month_end_closes(days, [1.0, 2.0, 3.0]) == [
        {"day": "2024-01-31", "close": 2.0},
        {"day": "2024-02-01", "close": 3.0},
    ]


# --- options-implied probability --------------------------------------------


def _flat_smile(forward, iv):
    return [
        {"strike": k, "is_call": k >= forward, "iv": iv}
        for k in [forward * (0.5 + 0.05 * i) for i in range(21)]
    ]


def test_with_a_flat_smile_it_is_n_of_d2():
    forward, t, iv, strike = 100.0, 0.5, 0.2, 105.0
    sd = iv * math.sqrt(t)
    d2 = (math.log(forward / strike) - 0.5 * sd * sd) / sd
    p = dp.implied_probability_above(_flat_smile(forward, iv), forward, t, strike)
    assert p == pytest.approx(dp._norm_cdf(d2), abs=1e-6)


def test_a_negative_skew_raises_the_probability_of_finishing_above():
    """
    Digital call = N(d2) - vega * dsigma/dK. With vol falling as the strike
    rises (equity skew), the second term is positive: the probability above
    exceeds the flat-vol N(d2) at the strike's own vol.
    """
    forward, t, strike = 100.0, 0.5, 100.0
    smile = [
        {"strike": k, "is_call": k >= forward, "iv": 0.30 - 0.002 * (k - 50)}
        for k in range(50, 151, 5)
    ]
    iv = dp.smile_iv(smile, forward, strike)
    sd = iv * math.sqrt(t)
    flat = dp._norm_cdf((math.log(forward / strike) - 0.5 * sd * sd) / sd)
    assert dp.implied_probability_above(smile, forward, t, strike) > flat


def test_options_view_uses_the_first_expiry_on_or_after_the_date():
    surface = {
        "day": "2026-10-15",
        "expiries": [
            {"expiry": "2026-11-20", "forward": 100.0, "T": 0.1},
            {"expiry": "2026-12-18", "forward": 100.0, "T": 0.18},
        ],
        "smiles": {"2026-11-20": _flat_smile(100, 0.2), "2026-12-18": _flat_smile(100, 0.2)},
        "term_structure": [],
    }
    view = dp.options_view(surface, 105.0, date(2026, 12, 1))
    assert view["trigger"]["expiry_used"] == "2026-12-18"
    below = dp.options_view(surface, 105.0, date(2026, 12, 1), above=False)
    assert below["trigger"]["probability"] == pytest.approx(1 - view["trigger"]["probability"])
    late = dp.options_view(surface, 105.0, date(2027, 6, 1))
    assert late["trigger"]["probability"] is None


def test_no_surface_says_so():
    assert dp.options_view(None) == {
        "available": False,
        "reason": "no archived option chain for this ticker",
    }


# --- provenance ---------------------------------------------------------------


def test_edgar_url_is_the_filing_directory():
    assert (
        dp.edgar_url(789019, "0001193125-10-171791")
        == "https://www.sec.gov/Archives/edgar/data/789019/000119312510171791/"
    )


def test_the_manifest_hashes_every_file_but_itself(tmp_path):
    dp.write_json(tmp_path / "a.json", {"x": 1})
    dp.write_json(tmp_path / "b.json", {"y": date(2026, 1, 2)})
    result = dp.write_manifest(tmp_path, "X", date(2026, 1, 2), "complete", "abc", {})
    assert set(result.files) == {"a.json", "b.json"}
    assert result.files["a.json"] == dp.sha256_of(tmp_path / "a.json")
    manifest = json.loads((tmp_path / "pack.json").read_text())
    assert manifest["files"] == result.files
    assert manifest["seats"]["analysts"] == [
        "screen.json",
        "facts.json",
        "statements.json",
        "prices.json",
    ]
    assert "valuation.json" not in manifest["seats"]["analysts"], (
        "the DCF would anchor the blind seats"
    )
    assert manifest["not_covered"], "every pack says what it does not cover"
