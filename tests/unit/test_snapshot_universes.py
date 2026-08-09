"""
scripts/snapshot_universes.py — the daily membership snapshot.

This runs unattended, so the behaviour that matters is what it does when a
source misbehaves. An empty constituent list means the scrape failed —
DynamicUniverse returns [] for that and for a genuinely empty index — and
recording it would write a snapshot claiming the index emptied. That is worse
than recording nothing, because the false snapshot then becomes the answer to
every point-in-time query for that day.

Phase 5 — scheduled maintenance
"""

from unittest.mock import AsyncMock, patch

import pytest

from db.repositories.universe import Snapshot


class FakeRepo:
    def __init__(self):
        self.recorded = []

    async def record_snapshot(self, index_name, symbols, taken_at=None):
        self.recorded.append((index_name, list(symbols)))
        return Snapshot(
            index_name=index_name,
            taken_at=taken_at,
            member_count=len(symbols),
            added=[],
            removed=[],
        )


def run_with(tickers_by_index, indexes, dry_run=False):
    """Drive the script with a stubbed fetcher and repo. No network, no DB."""
    import asyncio

    from scripts import snapshot_universes as module

    repo = FakeRepo()

    class FakeUniverse:
        def get_tickers(self, source):
            value = tickers_by_index.get(source)
            if isinstance(value, Exception):
                raise value
            return value or []

    class FakeSession:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *exc):
            return False

    with (
        patch.object(module, "get_session", lambda: FakeSession()),
        patch.object(module, "TimescaleUniverseRepo", lambda _s: repo),
        patch.dict(
            "sys.modules",
            {"data_pipeline.dynamic_universe": type(
                "M", (), {"DynamicUniverse": FakeUniverse}
            )},
        ),
    ):
        code = asyncio.run(module.snapshot(indexes, dry_run))
    return code, repo


def test_a_healthy_index_is_recorded():
    code, repo = run_with({"sp500": ["AAPL", "MSFT"]}, ["sp500"])
    assert code == 0
    assert repo.recorded == [("sp500", ["AAPL", "MSFT"])]


def test_an_empty_list_is_NOT_recorded():
    """
    THE rule. An empty result means the scrape failed; recording it would
    write a snapshot asserting the index emptied, and that false snapshot
    would then answer every point-in-time query for the day.
    """
    code, repo = run_with({"sp500": []}, ["sp500"])
    assert repo.recorded == []
    assert code == 1


def test_one_broken_source_does_not_stop_the_others():
    code, repo = run_with(
        {"sp500": ["AAPL"], "nasdaq100": [], "dow_jones": ["MMM"]},
        ["sp500", "nasdaq100", "dow_jones"],
    )
    assert [name for name, _ in repo.recorded] == ["sp500", "dow_jones"]
    # Partial success is success: one moved page should not look like an outage.
    assert code == 0


def test_a_raising_source_is_caught():
    code, repo = run_with({"sp500": RuntimeError("boom")}, ["sp500"])
    assert repo.recorded == []
    assert code == 1


def test_every_source_failing_is_a_nonzero_exit():
    """That is the case that means something systemic is wrong."""
    code, _ = run_with({"sp500": [], "dow_jones": []}, ["sp500", "dow_jones"])
    assert code == 1


def test_dry_run_records_nothing():
    code, repo = run_with({"sp500": ["AAPL"]}, ["sp500"], dry_run=True)
    assert code == 0
    assert repo.recorded == []


def test_nasdaq100_is_not_in_the_defaults():
    """
    Its Wikipedia page no longer carries a constituents table (2026-08-09), so
    including it would mean a guaranteed daily failure — which trains everyone
    to ignore the log.
    """
    from scripts.snapshot_universes import DEFAULT_INDEXES

    assert "nasdaq100" not in DEFAULT_INDEXES
    assert set(DEFAULT_INDEXES) == {"sp500", "dow_jones", "top_100_crypto"}
