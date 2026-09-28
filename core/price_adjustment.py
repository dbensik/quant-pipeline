"""
core/price_adjustment.py
Turn prices as the provider SERVED them into raw, split-adjusted or
total-return prices, at read time.

Phase 2 of research/dividend-drift-plan-2026-09-27.md. Pure of the network and
the database; nothing calls this yet.

WHY AT READ TIME. Every bar stored before this was Yahoo's `auto_adjust=True`
value, adjusted for every split, spinoff and dividend Yahoo knew of on the day
it was FETCHED. Bars fetched on different days were adjusted to different
as-of dates, and the differences are fake one-day moves: the 2025-07-16 seam
(121 payers, CAG -8.8%), MO -1.58pp on its 2026-09-15 ex-date, NFLX's -90% day.
Adjusting once, at read time, from one list of events, makes every bar agree.

WHAT "SERVED" MEANS. Yahoo has no raw price. With `auto_adjust=False` it
returns a close already divided by every split, and multiplied by every
spinoff factor, with an ex-date up to the fetch — but not adjusted for
dividends (checked 2026-09-27: NVDA reads $121 three days before its 10:1
split; HWM reads $12.58 before its spinoff). Its dividend amounts are scaled
the same way (NVDA 0.01; HWM's 2020-02-06 dividend is Arconic's $0.02 x
0.7669). So a served value plus WHEN it was fetched pins down the raw value:

    raw = served x every split ratio / every manual factor
          with an ex-date in (bar date, fetch date]

A bar fetched the morning after it trades is already raw. History fetched
later is not, and needs no restating when a later split arrives — the event
list grows, the stored value does not change.

THE MODES
    none    raw prices and volumes.
    split   raw / later splits x later manual factors. What Yahoo quotes as
            `Close`, and what a chart should show.
    total   split x the dividend factors. What every backtest and screener
            reads, and what Yahoo returns with `auto_adjust=True`.

Volume is adjusted for splits only, so dollar volume is preserved.

ONE UNVERIFIED RULE. When a split's ex-date is the fetch date itself, is the
split already applied? `APPLIED_ON_EX_DATE` says yes. It only matters for a
bar fetched on the morning of an ex-date, and the daily fresh-return check
(phase 6) is what confirms or refutes it on the next real split.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

MODES = ("none", "split", "total")

#: Kinds that rescale PRICES in the served basis, and how. A split of ratio r
#: divides earlier prices by r; a manual factor f multiplies them by f.
SPLIT, DIVIDEND, MANUAL = "split", "dividend", "manual_factor"

_ONE_DAY = timedelta(days=1)


@dataclass(frozen=True)
class ServedBar:
    day: date
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    fetched_at: datetime


@dataclass(frozen=True)
class Action:
    ex_date: date
    kind: str  # SPLIT | DIVIDEND | MANUAL
    value: float  # ratio, dividend per share AS SERVED, or price factor
    fetched_at: datetime


@dataclass(frozen=True)
class Bar:
    day: date
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]


#: Whether a fetch made ON an event's ex-date already reflects it. Assumed yes;
#: unverified — see the module docstring. The one place the rule lives.
APPLIED_ON_EX_DATE = True


#: Bar dates and ex-dates are New York trading dates, so a fetch must be dated
#: there too. fetched_at is stored in UTC, and after 20:00 ET the UTC date has
#: already rolled over: an evening fetch the day before a split would read as
#: a fetch ON the ex-date, and a raw $1,208.88 would come back as $12,088.80.
MARKET_TZ = ZoneInfo("America/New_York")


def _last_applied(fetched_at: datetime) -> date:
    """The latest ex-date a fetch at `fetched_at` already reflects.

    The only place a fetch is turned into a date — prices and dividends both
    come through here."""
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware")
    fetched = fetched_at.astimezone(MARKET_TZ).date()
    return fetched if APPLIED_ON_EX_DATE else fetched - _ONE_DAY


class _Cumulative:
    """
    Product of per-event price multipliers, queryable over an ex-date interval.

    The same events answer two questions — "which events had Yahoo applied
    when this was fetched" and "which events come after this bar" — so both
    are one prefix-product lookup instead of a scan per bar.
    """

    def __init__(self, events: Sequence[Tuple[date, float]]) -> None:
        ordered = sorted(events)
        self.dates = [d for d, _ in ordered]
        self.prefix = [1.0]
        for _, m in ordered:
            self.prefix.append(self.prefix[-1] * m)

    def up_to(self, day: date) -> float:
        """Product over events with ex_date <= day."""
        return self.prefix[bisect_right(self.dates, day)]

    def between(self, after: date, through: date) -> float:
        """Product over events with after < ex_date <= through."""
        if through <= after:
            return 1.0
        return self.up_to(through) / self.up_to(after)

    def after(self, day: date) -> float:
        """Product over every event with ex_date > day."""
        return self.prefix[-1] / self.up_to(day)


def _price_events(actions: Sequence[Action]) -> _Cumulative:
    """
    The multiplier that takes a pre-event raw price to the post-event basis.
    Split r: price / r. Manual factor f: price x f.
    """
    return _Cumulative(
        [(a.ex_date, 1.0 / a.value) for a in actions if a.kind == SPLIT]
        + [(a.ex_date, a.value) for a in actions if a.kind == MANUAL]
    )


def _volume_events(actions: Sequence[Action]) -> _Cumulative:
    """Volume moves opposite to price on a split, and not at all otherwise."""
    return _Cumulative([(a.ex_date, a.value) for a in actions if a.kind == SPLIT])


def _mul(value: Optional[float], factor: float) -> Optional[float]:
    return None if value is None else value * factor


def to_raw(bars: Sequence[ServedBar], actions: Sequence[Action]) -> List[Bar]:
    """
    Undo whatever Yahoo had applied when each bar was fetched.

    The events applied at fetch are those with an ex-date in
    (bar date, fetch date]; `_price_events` maps pre-event to post-event, so
    the served value is divided by that product to get back to raw.
    """
    price, volume = _price_events(actions), _volume_events(actions)
    out = []
    for b in bars:
        through = _last_applied(b.fetched_at)
        p = price.between(b.day, through)
        v = volume.between(b.day, through)
        out.append(
            Bar(
                day=b.day,
                open=_mul(b.open, 1.0 / p),
                high=_mul(b.high, 1.0 / p),
                low=_mul(b.low, 1.0 / p),
                close=_mul(b.close, 1.0 / p),
                volume=_mul(b.volume, 1.0 / v),
            )
        )
    return out


def raw_dividends(actions: Sequence[Action]) -> List[Tuple[date, float]]:
    """
    Dividends per RAW share. Yahoo scales an amount by every split and manual
    factor between its ex-date and the fetch, exactly as it scales a price.
    """
    price = _price_events(actions)
    return sorted(
        (a.ex_date, a.value / price.between(a.ex_date, _last_applied(a.fetched_at)))
        for a in actions
        if a.kind == DIVIDEND
    )


def _dividend_factors(raw: Sequence[Bar], actions: Sequence[Action]) -> _Cumulative:
    """
    One multiplier per dividend: 1 - D / (raw close of the last bar before the
    ex-date). D and that close are both raw, so the ratio is basis-free — the
    same number Yahoo computes in its own basis.

    A dividend with no bar before it (ex-date before the series starts)
    affects no bar and is skipped. So is one whose prior close is missing or
    not above D, rather than inventing a factor.
    """
    days = [b.day for b in raw]
    events = []
    for ex_date, amount in raw_dividends(actions):
        i = bisect_right(days, ex_date - _ONE_DAY) - 1
        if i < 0:
            continue
        prior = raw[i].close
        if prior is None or prior <= amount:
            continue
        events.append((ex_date, 1.0 - amount / prior))
    return _Cumulative(events)


def adjust(
    bars: Sequence[ServedBar], actions: Sequence[Action], mode: str = "total"
) -> List[Bar]:
    """Served bars to `mode` ('none' | 'split' | 'total'), in date order."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
    ordered = sorted(bars, key=lambda b: b.day)
    raw = to_raw(ordered, actions)
    if mode == "none":
        return raw

    price, volume = _price_events(actions), _volume_events(actions)
    dividends = _dividend_factors(raw, actions) if mode == "total" else None
    out = []
    for b in raw:
        p = price.after(b.day)
        if dividends is not None:
            p *= dividends.after(b.day)
        v = volume.after(b.day)
        out.append(
            Bar(
                day=b.day,
                open=_mul(b.open, p),
                high=_mul(b.high, p),
                low=_mul(b.low, p),
                close=_mul(b.close, p),
                volume=_mul(b.volume, v),
            )
        )
    return out
