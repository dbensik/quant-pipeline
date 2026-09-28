"""
core/price_adjustment.py — served prices to raw, split-adjusted and total.

Numbers are Yahoo's, captured 2026-09-27: NVDA's 10:1 split on 2024-06-10, MO's
$1.06 dividend on 2026-06-15, HWM's unlisted Arconic spinoff on 2020-04-01.
Against the full history of 11 symbols the engine reproduced Yahoo's
auto_adjust series to within 1e-6 and its Close exactly; these tests pin the
parts that single-fetch check could not reach — bars fetched on DIFFERENT
days, which is the whole reason the stored series drifted.
"""

from datetime import date, datetime, timezone

import pytest

import core.price_adjustment as pa
from core.price_adjustment import Action, ServedBar, adjust, raw_dividends, to_raw

LATER = datetime(2026, 9, 27, 10, tzinfo=timezone.utc)


def at(day: date, hour: int = 10) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc)


def served(day, close, fetched_at, volume=1000.0):
    return ServedBar(day, close, close, close, close, volume, fetched_at)


def closes(bars):
    return [round(b.close, 4) for b in bars]


# ---------------------------------------------------------------------------
# Splits — a stored value never needs restating
# ---------------------------------------------------------------------------

NVDA_SPLIT = Action(date(2024, 6, 10), "split", 10.0, LATER)


def test_a_bar_fetched_before_a_split_and_one_fetched_after_agree():
    """
    2024-06-07 fetched that evening was served $1208.88; fetched today it is
    served $120.888. Both are the same trade, and both must come back as it.
    """
    early = served(date(2024, 6, 7), 1208.88, at(date(2024, 6, 8)))
    late = served(date(2024, 6, 7), 120.888, LATER)
    assert closes(to_raw([early], [NVDA_SPLIT])) == [1208.88]
    assert closes(to_raw([late], [NVDA_SPLIT])) == [1208.88]


def test_split_mode_is_what_yahoo_quotes_and_none_is_what_traded():
    bars = [
        served(date(2024, 6, 7), 1208.88, at(date(2024, 6, 8))),  # fetched pre-split
        served(date(2024, 6, 10), 121.79, at(date(2024, 6, 11))),
    ]
    assert closes(adjust(bars, [NVDA_SPLIT], "none")) == [1208.88, 121.79]
    assert closes(adjust(bars, [NVDA_SPLIT], "split")) == [120.888, 121.79]


def test_volume_moves_opposite_to_price_on_a_split():
    bars = [served(date(2024, 6, 7), 1208.88, at(date(2024, 6, 8)), volume=41_238_600)]
    assert adjust(bars, [NVDA_SPLIT], "none")[0].volume == 41_238_600
    assert adjust(bars, [NVDA_SPLIT], "split")[0].volume == pytest.approx(412_386_000)


def test_a_bar_fetched_on_the_ex_date_morning_follows_the_one_rule():
    """
    The case the plan flagged: a split effective the morning of a 06:00 fetch.
    APPLIED_ON_EX_DATE decides it; this pins the current rule so changing it
    is deliberate.
    """
    bar = served(date(2024, 6, 7), 120.888, at(date(2024, 6, 10), hour=10))
    assert pa.APPLIED_ON_EX_DATE is True
    assert closes(to_raw([bar], [NVDA_SPLIT])) == [1208.88]


def test_the_rule_is_the_only_switch(monkeypatch):
    bar = served(date(2024, 6, 7), 1208.88, at(date(2024, 6, 10), hour=10))
    monkeypatch.setattr(pa, "APPLIED_ON_EX_DATE", False)
    assert closes(to_raw([bar], [NVDA_SPLIT])) == [1208.88]


# ---------------------------------------------------------------------------
# Dividends
# ---------------------------------------------------------------------------

MO_DIV = Action(date(2026, 6, 15), "dividend", 1.06, LATER)


def test_total_mode_takes_the_dividend_out_of_the_ex_date_drop():
    """
    MO served 71.94 then 69.59 across its 2026-06-15 ex-date: -3.27% raw,
    -1.82% total return — Yahoo's auto_adjust figure.
    """
    bars = [
        served(date(2026, 6, 12), 71.94, at(date(2026, 6, 13))),
        served(date(2026, 6, 15), 69.59, at(date(2026, 6, 16))),
    ]
    raw = closes(adjust(bars, [MO_DIV], "none"))
    tot = closes(adjust(bars, [MO_DIV], "total"))
    assert raw[1] / raw[0] - 1 == pytest.approx(-0.0327, abs=1e-4)
    assert tot[1] / tot[0] - 1 == pytest.approx(-0.0182, abs=1e-4)
    assert tot[0] == pytest.approx(71.94 - 1.06, abs=1e-4)


def test_dividends_never_touch_volume():
    bars = [served(date(2026, 6, 12), 71.94, at(date(2026, 6, 13)), volume=8_409_100)]
    assert adjust(bars, [MO_DIV], "total")[0].volume == 8_409_100


def test_a_dividend_served_after_a_split_is_restored_to_its_raw_amount():
    """NVDA paid $0.04 before its split; served today it reads $0.004."""
    old = Action(date(2024, 3, 5), "dividend", 0.004, LATER)
    assert raw_dividends([NVDA_SPLIT, old]) == [(date(2024, 3, 5), pytest.approx(0.04))]
    fetched_then = Action(date(2024, 3, 5), "dividend", 0.04, at(date(2024, 3, 6)))
    assert raw_dividends([NVDA_SPLIT, fetched_then]) == [
        (date(2024, 3, 5), pytest.approx(0.04))
    ]


def test_a_dividend_before_the_series_starts_changes_nothing():
    bars = [served(date(2026, 6, 15), 69.59, at(date(2026, 6, 16)))]
    assert closes(adjust(bars, [MO_DIV], "total")) == [69.59]


# ---------------------------------------------------------------------------
# THE regression — bars fetched on different days no longer disagree
# ---------------------------------------------------------------------------

def test_the_seam_disappears():
    """
    The stored series drifted because each bar carried the adjustments known
    when IT was fetched. Here NVDA's bars either side of its split are fetched
    on opposite sides of it — the early ones served at $1,20x, the later ones
    at $12x — with a dividend in the window too. The result must be identical
    to fetching everything at once. It is the fetched_at bookkeeping that
    makes it so: without it the early bars read 10x too high.

    Dividends cannot cause this by construction: served values carry none,
    whenever they were fetched. The old failure was storing auto_adjust=True
    values, which do.
    """
    div = Action(date(2024, 6, 11), "dividend", 0.01, LATER)
    days = [date(2024, 6, 6), date(2024, 6, 7), date(2024, 6, 10), date(2024, 6, 11)]
    raw_prices = [1209.98, 1208.88, 121.79, 120.91]
    split_prices = [120.998, 120.888, 121.79, 120.91]
    staggered = [
        served(days[0], raw_prices[0], at(date(2024, 6, 7))),
        served(days[1], raw_prices[1], at(date(2024, 6, 8))),
        served(days[2], raw_prices[2], at(date(2024, 6, 11))),
        served(days[3], raw_prices[3], at(date(2024, 6, 12))),
    ]
    at_once = [served(d, p, LATER) for d, p in zip(days, split_prices)]
    for mode in ("none", "split", "total"):
        assert closes(adjust(staggered, [NVDA_SPLIT, div], mode)) == closes(
            adjust(at_once, [NVDA_SPLIT, div], mode)
        )
    assert closes(adjust(staggered, [NVDA_SPLIT, div], "none")) == raw_prices


# ---------------------------------------------------------------------------
# Manual factors — the spinoff Yahoo applies but never lists
# ---------------------------------------------------------------------------

HWM_SPINOFF = Action(date(2020, 4, 1), "manual_factor", 0.76687, LATER)


def test_the_hwm_spinoff_is_undone_to_the_arconic_price_and_reapplied():
    """Served $12.5767 on 2020-03-30; Arconic actually traded near $16.40."""
    bars = [
        served(date(2020, 3, 30), 12.5767, LATER),
        served(date(2020, 4, 1), 13.20, LATER),
    ]
    assert closes(adjust(bars, [HWM_SPINOFF], "none")) == [16.4, 13.2]
    assert closes(adjust(bars, [HWM_SPINOFF], "split")) == [12.5767, 13.2]


def test_a_dividend_served_after_a_spinoff_is_restored_too():
    """HWM's 2020-02-06 dividend is served as 0.015337 — Arconic's $0.02."""
    div = Action(date(2020, 2, 6), "dividend", 0.015337, LATER)
    assert raw_dividends([HWM_SPINOFF, div]) == [(date(2020, 2, 6), pytest.approx(0.02, abs=1e-5))]


# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------

def test_missing_fields_stay_missing():
    bar = ServedBar(date(2024, 6, 7), None, None, None, None, None, LATER)
    out = adjust([bar], [NVDA_SPLIT, MO_DIV], "total")[0]
    assert (out.open, out.high, out.low, out.close, out.volume) == (None,) * 5


def test_an_unknown_mode_is_refused():
    with pytest.raises(ValueError):
        adjust([], [], "adjusted")


def test_output_is_in_date_order_whatever_the_input_order():
    bars = [served(date(2024, 6, 10), 121.79, LATER), served(date(2024, 6, 7), 120.888, LATER)]
    assert [b.day for b in adjust(bars, [NVDA_SPLIT])] == [date(2024, 6, 7), date(2024, 6, 10)]


def test_an_evening_fetch_is_dated_in_new_york_not_utc():
    """
    Bar dates are New York trading dates; fetched_at is stored in UTC. A fetch
    at 21:00 ET on 2024-06-09 is 01:00 UTC on 06-10 — the split's ex-date. It
    happened BEFORE the split, so its served $1,208.88 is already raw. Dating
    it in UTC would multiply it by 10.
    """
    evening = datetime(2024, 6, 10, 1, tzinfo=timezone.utc)
    bar = served(date(2024, 6, 7), 1208.88, evening)
    assert closes(to_raw([bar], [NVDA_SPLIT])) == [1208.88]
