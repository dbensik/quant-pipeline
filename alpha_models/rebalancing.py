"""
alpha_models/rebalancing.py

Rebalance-date arithmetic shared by the asset-allocation strategies.

WHY THIS IS NOT `index.intersection(index.resample(freq).last().index)`.
That is the obvious formulation — it is what `basket_trading.py` and
`index_rebalancing.py` do — and it silently drops rebalances. `resample("ME")`
labels each group with the CALENDAR month end, which is a Saturday or Sunday
roughly a third of the time, and a weekend label is not in a trading-day index,
so intersecting removes it. Measured over business days in 2022: 12 calendar
month-ends, only 9 survive. April, July and December simply never rebalance.

Grouping by period and taking the last date actually present cannot drop a
period that has any trading day in it at all.

Phase 1 — asset allocation
"""

from __future__ import annotations

import warnings

import pandas as pd

#: Pandas offset aliases we accept. 'M'/'Q' were deprecated for resample in
#: pandas 2.2 and are removed in 3.0; the period aliases below are their
#: replacements and are pure renames.
_PERIOD_ALIAS = {
    "W": "W",
    "ME": "M",
    "QE": "Q",
    "YE": "Y",
    # Tolerated legacy spellings, mapped rather than rejected: the existing
    # registry entries default to "ME"/"QE" but callers may still send "M"/"Q".
    "M": "M",
    "Q": "Q",
    "Y": "Y",
}


def rebalance_dates(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    """
    The last date present in `index` within each COMPLETED `frequency` period.

    The final period is excluded, and that exclusion is the point. Nothing in
    the data says whether the last period is finished or merely where the file
    stops, so including it makes the schedule depend on WHERE THE DATA ENDS: a
    frame ending 13 January put a "monthly" rebalance on the 13th, while the
    same frame with more bars correctly put it on the 31st. That is a signal at
    time t changing because of bars after t, which is the definition of
    look-ahead — `test_no_look_ahead_multi_asset_wide` caught it on all three
    allocation strategies.

    The cost is at most one rebalance at the very end of the sample. Stability
    under truncation is worth more: without it, every backtest's final trade is
    an artifact of the download date.

    Args:
        index:     A sorted DatetimeIndex of trading days.
        frequency: 'W', 'ME' (month end), 'QE' (quarter end) or 'YE'.

    Returns:
        A DatetimeIndex, always a subset of `index`, with one entry per
        completed period that contains at least one trading day. Empty when the
        data spans a single period — correctly, since none has completed.

    Raises:
        ValueError: on an unsupported frequency, rather than silently returning
                    an empty schedule — a strategy that never rebalances looks
                    exactly like one that is simply flat.
    """
    alias = _PERIOD_ALIAS.get(str(frequency).upper())
    if alias is None:
        raise ValueError(
            f"Unsupported rebalance frequency {frequency!r}. "
            f"Valid: {sorted(set(_PERIOD_ALIAS))}"
        )
    if len(index) == 0:
        return pd.DatetimeIndex([])

    # Period conversion warns that it drops tz; that is fine because periods are
    # only used to GROUP. The returned dates come from `index` itself and must
    # keep its tz — going via `.values` silently strips it, and the resulting
    # naive timestamps then raise KeyError against the tz-aware index that
    # TimescaleDB returns. Every synthetic fixture here is tz-naive, so only a
    # real backtest surfaced it.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        periods = index.tz_localize(None) if index.tz is not None else index
        periods = periods.to_period(alias)

    last_per_period = index.to_series().groupby(periods.values).last()
    # Drop the period the final bar falls in — it may be mid-period.
    completed = last_per_period.iloc[:-1]
    return pd.DatetimeIndex(completed, name=index.name)
