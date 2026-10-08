"""
core/rates.py: conversions with a known answer from a primary source, the
on-or-before read rule, and the insert-only, never-today write rule.

No database: the repository is a fake satisfying core.rates.RateRepository.
The SQL implementation is covered in tests/api/test_rates_repo_integration.py.
"""

import math
from datetime import date, datetime, timedelta, timezone

import pytest

from config.settings import INGEST_OVERLAP_DAYS, RISK_FREE_RATE_HISTORY_START
from core.rates import (
    RateIngestError,
    RateObservation,
    discount_price,
    discount_to_bond_equivalent,
    discount_to_continuous,
    ingest_rates,
    risk_free_from_observation,
    risk_free_rate,
)

SERIES = "^IRX"
#: 18:00 UTC on 2026-10-08 is 14:00 in New York: a session still trading.
NOW = datetime(2026, 10, 8, 18, 0, tzinfo=timezone.utc)
FETCHED = datetime(2026, 10, 1, tzinfo=timezone.utc)


class FakeRateRepo:
    def __init__(self, rows=()):
        self.rows = {(o.series, o.obs_date): o for o in rows}

    async def insert(self, observations):
        new = 0
        for o in observations:
            if (o.series, o.obs_date) not in self.rows:
                self.rows[(o.series, o.obs_date)] = o
                new += 1
        return new

    async def newest_date(self, series):
        days = [d for (s, d) in self.rows if s == series]
        return max(days) if days else None

    async def latest_on_or_before(self, series, as_of):
        days = [d for (s, d) in self.rows if s == series and d <= as_of]
        return self.rows[(series, max(days))] if days else None


def obs(day, value=4.0):
    return RateObservation(SERIES, day, value, "yfinance", FETCHED)


# --- conversions -------------------------------------------------------------

@pytest.mark.parametrize(
    "discount, treasury_coupon_equivalent",
    # Treasury Daily Treasury Bill Rates, 13-week, 2026-10-01 and 2026-10-07.
    [(4.00, 4.10), (4.05, 4.15)],
)
def test_bond_equivalent_reproduces_treasury(discount, treasury_coupon_equivalent):
    assert round(discount_to_bond_equivalent(discount, 91) * 100, 2) == treasury_coupon_equivalent


def test_continuous_rate_reprices_the_bill():
    r = discount_to_continuous(4.037, 91)
    assert math.exp(-r * 91 / 365) == pytest.approx(discount_price(4.037, 91), abs=1e-15)
    # Ordering that must hold for a positive discount: d < continuous < BEY.
    assert 0.04037 < r < discount_to_bond_equivalent(4.037, 91)


def test_negative_discount_is_a_negative_rate_not_an_error():
    # ^IRX closed at -0.105 on 2020-03-26.
    assert discount_to_continuous(-0.105, 91) < 0
    assert discount_to_bond_equivalent(-0.105, 91) < 0


def test_bond_equivalent_refuses_a_tenor_its_formula_does_not_cover():
    with pytest.raises(ValueError):
        discount_to_bond_equivalent(4.0, 364)


# --- read rule ---------------------------------------------------------------

def test_reading_after_the_observation_reports_its_age():
    rate = risk_free_from_observation(obs(date(2026, 10, 9)), as_of=date(2026, 10, 12))
    assert rate.obs_date == date(2026, 10, 9)
    assert rate.age_days == 3
    assert not rate.stale


def test_stale_beyond_the_max_age_and_not_at_it():
    assert not risk_free_from_observation(obs(date(2026, 10, 1)), date(2026, 10, 8), max_age_days=7).stale
    assert risk_free_from_observation(obs(date(2026, 10, 1)), date(2026, 10, 9), max_age_days=7).stale


def test_an_observation_after_the_requested_date_is_refused():
    with pytest.raises(ValueError, match="look-ahead"):
        risk_free_from_observation(obs(date(2026, 10, 9)), as_of=date(2026, 10, 8))


@pytest.mark.asyncio
async def test_a_bond_holiday_reads_the_previous_print():
    # 2026-10-12 is Columbus Day: stocks trade, the bond market is shut.
    repo = FakeRateRepo([obs(date(2026, 10, 9), 4.02), obs(date(2026, 10, 13), 4.10)])
    rate = await risk_free_rate(repo, date(2026, 10, 12), SERIES)
    assert rate.obs_date == date(2026, 10, 9)
    assert rate.quoted_pct == 4.02


@pytest.mark.asyncio
async def test_nothing_on_or_before_is_none_not_the_first_later_value():
    repo = FakeRateRepo([obs(date(2026, 10, 9))])
    assert await risk_free_rate(repo, date(2026, 10, 8), SERIES) is None


# --- write rule --------------------------------------------------------------

def fetcher_returning(rows, calls=None):
    def fetch(series, start, end_exclusive):
        if calls is not None:
            calls.append((series, start, end_exclusive))
        return list(rows)
    return fetch


@pytest.mark.asyncio
async def test_empty_table_starts_at_the_history_start_and_ends_before_today():
    calls = []
    repo = FakeRateRepo()
    await ingest_rates(repo, SERIES, fetcher_returning([(date(2015, 1, 2), 0.03)], calls), now=NOW)
    assert calls == [(SERIES, date.fromisoformat(RISK_FREE_RATE_HISTORY_START), date(2026, 10, 8))]


@pytest.mark.asyncio
async def test_routine_run_overlaps_the_newest_stored_date():
    calls = []
    repo = FakeRateRepo([obs(date(2026, 10, 1))])
    await ingest_rates(repo, SERIES, fetcher_returning([(date(2026, 10, 2), 4.01)], calls), now=NOW)
    assert calls[0][1] == date(2026, 10, 1) - timedelta(days=INGEST_OVERLAP_DAYS)


@pytest.mark.asyncio
async def test_todays_moving_value_is_never_stored():
    repo = FakeRateRepo()
    rows = [(date(2026, 10, 7), 4.037), (date(2026, 10, 8), 4.043)]
    report = await ingest_rates(repo, SERIES, fetcher_returning(rows), now=NOW)
    assert report.inserted == 1
    assert await repo.newest_date(SERIES) == date(2026, 10, 7)


@pytest.mark.asyncio
async def test_new_york_date_decides_today_not_utc():
    # 02:00 UTC on 10-09 is still 22:00 on 10-08 in New York.
    late = datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc)
    repo = FakeRateRepo()
    rows = [(date(2026, 10, 7), 4.037), (date(2026, 10, 8), 4.043)]
    report = await ingest_rates(repo, SERIES, fetcher_returning(rows), now=late)
    assert report.end_exclusive == date(2026, 10, 8)
    assert await repo.newest_date(SERIES) == date(2026, 10, 7)


@pytest.mark.asyncio
async def test_a_reserved_value_never_replaces_a_stored_one():
    repo = FakeRateRepo([obs(date(2026, 10, 6), 4.037)])
    report = await ingest_rates(
        repo, SERIES, fetcher_returning([(date(2026, 10, 6), 9.99), (date(2026, 10, 7), 4.037)]), now=NOW
    )
    assert report.inserted == 1
    assert (await repo.latest_on_or_before(SERIES, date(2026, 10, 6))).value == 4.037


@pytest.mark.asyncio
async def test_an_empty_fetch_is_an_error_and_writes_nothing():
    repo = FakeRateRepo([obs(date(2026, 10, 1))])
    with pytest.raises(RateIngestError, match="no observations"):
        await ingest_rates(repo, SERIES, fetcher_returning([]), now=NOW)
    assert len(repo.rows) == 1
