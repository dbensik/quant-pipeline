"""
GET /api/v1/ingest/freshness — how old the stored price data is.

The number this exposes was silent for a year: bars ended 2025-07-15 and
every backtest ran on them until 2026-07-31 without anything on screen saying
so. Then the 06:00 job aborted four mornings running (2026-08-28..31) and the
dashboard again looked identical. These tests pin the contract the badge in
AppLayout renders from.

Route tests run against the FakeRepo series (400 daily bars from 2024-01-01),
whose newest bar is fixed in the past, so "everything is stale" is
deterministic regardless of today's date. The threshold arithmetic and the
not-stale branch are covered through the pure summariser with a pinned clock,
because a route test that passes only on certain calendar days is a test that
fails on a Monday.

Phase 5 — React pages for the ported routers
"""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from api.routers.ingest import summarise_freshness
from db.repositories.market_data import AssetLastBar
from tests.api.conftest import N_BARS, START

BASE = "/api/v1/ingest/freshness"
FIXTURE_NEWEST = START + timedelta(days=N_BARS - 1)


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

def test_newest_bar_is_the_max_over_the_fixture(client: TestClient):
    body = client.get(BASE).json()
    assert datetime.fromisoformat(body["newest_bar"]) == FIXTURE_NEWEST
    # Registered assets in the fixture: AAPL, BTC-USD, MSFT, EMPTY-USD.
    assert body["assets"] == 4
    assert body["max_age_days"] == 5


def test_an_asset_with_no_bars_is_stale_and_sorts_first(client: TestClient):
    """
    EMPTY-USD is registered with zero bars. "Never ingested" must read as
    worse than "stopped", so it leads the list and carries null, not a date.
    """
    body = client.get(BASE).json()
    first = body["stale_assets"][0]
    assert first["symbol"] == "EMPTY-USD"
    assert first["last_bar"] is None
    assert first["age_days"] is None


def test_by_class_reports_each_class_once(client: TestClient):
    body = client.get(BASE).json()
    classes = {row["asset_class"]: row for row in body["by_class"]}
    assert set(classes) == {"equity", "crypto"}
    assert classes["equity"]["assets"] == 2           # AAPL, MSFT
    assert classes["crypto"]["assets"] == 2           # BTC-USD, EMPTY-USD
    assert classes["crypto"]["with_bars"] == 1


def test_limit_caps_the_stale_list_but_not_the_count(client: TestClient):
    body = client.get(BASE, params={"limit": 1}).json()
    assert len(body["stale_assets"]) == 1
    assert body["stale"] == 4


def test_threshold_is_validated(client: TestClient):
    assert client.get(BASE, params={"max_age_days": 0}).status_code == 422


# ---------------------------------------------------------------------------
# Summariser — pinned clock
# ---------------------------------------------------------------------------

AS_OF = datetime(2026, 9, 14, 13, 0, tzinfo=timezone.utc)   # a Monday


def bar(days_ago: int) -> datetime:
    return AS_OF - timedelta(days=days_ago)


def test_a_weekend_gap_is_not_stale():
    """Monday: newest equity bar is Friday's, three days old. Green."""
    rows = [AssetLastBar("AAPL", "equity", bar(3))]
    out = summarise_freshness(rows, AS_OF, max_age_days=5, limit=50)
    assert out.age_days == 3
    assert out.stale == 0
    assert out.stale_assets == []


def test_the_sixth_day_is_stale():
    rows = [AssetLastBar("AAPL", "equity", bar(6))]
    out = summarise_freshness(rows, AS_OF, max_age_days=5, limit=50)
    assert out.stale == 1
    assert out.stale_assets[0].age_days == 6


def test_threshold_is_inclusive():
    rows = [AssetLastBar("AAPL", "equity", bar(5))]
    assert summarise_freshness(rows, AS_OF, 5, 50).stale == 0


def test_delisted_assets_are_excluded_from_every_count():
    """
    A delisted name is legitimately frozen. Counting it keeps the badge red
    for a reason nobody can act on — the road to a warning that gets ignored.
    """
    rows = [
        AssetLastBar("AAPL", "equity", bar(1)),
        AssetLastBar("GONE", "equity", bar(400), delisted_at=bar(300)),
    ]
    out = summarise_freshness(rows, AS_OF, 5, 50)
    assert out.assets == 1
    assert out.stale == 0
    assert out.newest_bar == bar(1)
    assert out.by_class[0].assets == 1


def test_stale_assets_are_oldest_first():
    rows = [
        AssetLastBar("A", "equity", bar(7)),
        AssetLastBar("B", "equity", bar(30)),
        AssetLastBar("C", "equity", None),
    ]
    out = summarise_freshness(rows, AS_OF, 5, 50)
    assert [r.symbol for r in out.stale_assets] == ["C", "B", "A"]


def test_empty_registry():
    out = summarise_freshness([], AS_OF, 5, 50)
    assert out.newest_bar is None
    assert out.age_days is None
    assert out.assets == 0
    assert out.by_class == []


def test_naive_timestamps_are_read_as_utc():
    rows = [AssetLastBar("AAPL", "equity", (AS_OF - timedelta(days=2)).replace(tzinfo=None))]
    assert summarise_freshness(rows, AS_OF, 5, 50).age_days == 2
