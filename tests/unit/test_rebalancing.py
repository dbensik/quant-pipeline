"""
alpha_models/rebalancing.py — rebalance-date arithmetic.

Every synthetic fixture in the strategy suites is tz-NAIVE, while TimescaleDB
returns tz-aware timestamps. That gap hid a KeyError that only appeared on a
real backtest, so timezone handling is pinned here explicitly.

Phase 1 — asset allocation
"""

import pandas as pd
import pytest

from alpha_models.rebalancing import rebalance_dates


# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tz", [None, "UTC", "America/New_York"])
def test_returned_dates_are_always_present_in_the_input_index(tz):
    """
    THE REGRESSION. Building the result via `.values` silently dropped the
    timezone, so the returned timestamps were naive and every `frame.loc[date]`
    raised KeyError against the tz-aware index the database returns. The unit
    suites never caught it because their fixtures are all tz-naive.
    """
    index = pd.bdate_range("2015-01-02", "2016-06-30", tz=tz)
    dates = rebalance_dates(index, "ME")

    assert len(dates) > 0
    assert dates.tz == index.tz
    missing = [d for d in dates if d not in index]
    assert not missing, f"returned dates absent from the index: {missing}"


# ---------------------------------------------------------------------------
# Completed periods only
# ---------------------------------------------------------------------------

def test_the_final_incomplete_period_is_excluded():
    """
    Nothing in the data says whether the last period finished or the file just
    stopped. Including it made the schedule depend on where the data ended — a
    frame ending 13 January put a 'monthly' rebalance on the 13th.
    """
    index = pd.bdate_range("2022-01-03", "2022-03-15")   # March is partial
    dates = rebalance_dates(index, "ME")

    assert [d.date().isoformat() for d in dates] == ["2022-01-31", "2022-02-28"]


def test_the_schedule_is_stable_under_truncation():
    """
    The property that makes a backtest trustworthy: removing trailing bars must
    not move an earlier rebalance date.
    """
    index = pd.bdate_range("2022-01-03", "2022-12-30")
    full = rebalance_dates(index, "ME")
    trunc = rebalance_dates(index[:-30], "ME")

    assert list(trunc) == list(full[: len(trunc)])


def test_a_single_period_of_data_yields_no_rebalance():
    """Correct, not a bug: no period has completed."""
    index = pd.bdate_range("2022-01-03", "2022-01-28")
    assert len(rebalance_dates(index, "ME")) == 0


# ---------------------------------------------------------------------------
# Period-end dates that are not trading days
# ---------------------------------------------------------------------------

def test_month_ends_falling_on_a_weekend_still_rebalance():
    """
    The bug this module exists to avoid, and which basket_trading still has:
    `index.intersection(resample("ME").last().index)` labels each group with the
    CALENDAR month end, which is a weekend roughly a third of the time, and a
    weekend label is not in a trading-day index. Over 2022 that silently drops
    April, July and December — 3 of 12 rebalances.
    """
    index = pd.bdate_range("2022-01-03", "2022-12-30")
    dates = rebalance_dates(index, "ME")

    # 12 months, minus the final incomplete one.
    assert len(dates) == 11
    months = {d.month for d in dates}
    assert {4, 7} <= months, "a month-end on a weekend must still rebalance"

    naive = index.intersection(pd.Series(1.0, index=index).resample("ME").last().index)
    assert len(naive) < len(dates), "the naive formulation should be strictly worse"


# ---------------------------------------------------------------------------
# Frequencies
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "frequency,expected", [("ME", 11), ("QE", 3), ("W", 51)]
)
def test_supported_frequencies(frequency, expected):
    index = pd.bdate_range("2022-01-03", "2022-12-30")
    assert len(rebalance_dates(index, frequency)) == expected


def test_an_unsupported_frequency_raises():
    """
    Rather than returning an empty schedule: a strategy that never rebalances
    looks exactly like one that is simply flat, and would backtest as such.
    """
    index = pd.bdate_range("2022-01-03", "2022-12-30")
    with pytest.raises(ValueError, match="Unsupported rebalance frequency"):
        rebalance_dates(index, "fortnightly")


def test_an_empty_index_is_empty_not_an_error():
    assert len(rebalance_dates(pd.DatetimeIndex([]), "ME")) == 0
