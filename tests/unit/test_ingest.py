"""
core/ingest.py — fetching bars into TimescaleDB.

The fetcher is injected, so none of this reaches the network.

These cover the behaviours that make ingestion correct rather than merely
present: resuming from the newest stored bar, dropping all-NULL bars, keeping
a symbol's existing asset identity, and not letting one bad symbol end a run.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.ingest import (
    DEFAULT_BACKFILL_START,
    IngestJob,
    ingest_symbols,
    is_empty_bar,
    retag,
)
from core.models import OHLCV, Asset, MarketDataRecord, Timestamp

START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def bar(symbol, day, close=100.0, empty=False):
    value = None if empty else close
    return MarketDataRecord(
        asset=Asset(symbol=symbol, asset_class="equity", source="yfinance"),
        ohlcv=OHLCV(
            open=value,
            high=value,
            low=value,
            close=value,
            volume=None if empty else 1000.0,
            timestamp=Timestamp(utc=START + timedelta(days=day)),
        ),
    )


class RecordingRepo:
    """Minimal MarketDataRepository that records what it was asked to write."""

    def __init__(self, assets=None, existing=None):
        self.assets = assets if assets is not None else {
            "AAPL": Asset(symbol="AAPL", asset_class="equity", source="yfinance"),
            "BTC-USD": Asset(symbol="BTC-USD", asset_class="crypto", source="yfinance"),
        }
        self.existing = existing or {}
        self.written = []
        self.replace_calls: list = []

    async def find_asset(self, symbol, asset_class=None):
        return self.assets.get(symbol)

    async def fetch_range(self, symbol, asset_class, start, end, source=None):
        return self.existing.get(symbol, [])

    async def write(self, records, replace: bool = False):
        self.written.extend(records)
        self.replace_calls.append(replace)
        # Mirrors the real repo: rows persisted, not rows submitted.
        return len(records)


def fetcher_for(records, calls=None):
    def fetch(symbols, start_date, end_date):
        if calls is not None:
            calls.append((tuple(symbols), start_date, end_date))
        return list(records)

    return fetch


# ---------------------------------------------------------------------------
# Empty bars
# ---------------------------------------------------------------------------

def test_all_null_bar_is_detected():
    assert is_empty_bar(bar("AAPL", 0, empty=True).ohlcv) is True


def test_a_priced_bar_is_not_empty():
    assert is_empty_bar(bar("AAPL", 0).ohlcv) is False


@pytest.mark.asyncio
async def test_empty_bars_are_dropped_and_counted():
    """
    The Phase 2 cutover chose to skip all-NULL bars rather than store them;
    five crypto tickers in the legacy database were nothing but padding. A NaN
    close also poisons every downstream metric.
    """
    repo = RecordingRepo()
    report = await ingest_symbols(
        repo,
        ["AAPL"],
        fetcher=fetcher_for([bar("AAPL", 0), bar("AAPL", 1, empty=True)]),
    )
    outcome = report.outcomes[0]
    assert outcome.fetched == 2
    assert outcome.written == 1
    assert outcome.skipped_empty == 1
    assert len(repo.written) == 1


# ---------------------------------------------------------------------------
# Asset identity
# ---------------------------------------------------------------------------

def test_retag_preserves_the_symbol_but_replaces_the_class():
    tagged = retag(bar("BTC-USD", 0), "crypto", "yfinance")
    assert tagged.asset.symbol == "BTC-USD"
    assert tagged.asset.asset_class == "crypto"


@pytest.mark.asyncio
async def test_crypto_is_not_written_as_equity():
    """
    THE identity trap. yfinance_adapter hardcodes asset_class="equity", and
    assets are keyed on (symbol, asset_class, source) — so writing BTC-USD as
    equity would create a SECOND asset row and file its bars under an id no
    query uses.
    """
    repo = RecordingRepo()
    await ingest_symbols(
        repo, ["BTC-USD"], fetcher=fetcher_for([bar("BTC-USD", 0)])
    )
    assert {r.asset.asset_class for r in repo.written} == {"crypto"}


# ---------------------------------------------------------------------------
# Resume point
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_symbol_with_no_history_backfills_from_the_default():
    calls = []
    repo = RecordingRepo()
    await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for([], calls))
    assert calls[0][1] == DEFAULT_BACKFILL_START.strftime("%Y-%m-%d")


@pytest.mark.asyncio
async def test_resume_starts_the_day_after_the_newest_stored_bar():
    """
    fetch_range is inclusive at both ends, so requesting the stored date
    itself would re-download a bar the upsert then discards.
    """
    calls = []
    repo = RecordingRepo(existing={"AAPL": [bar("AAPL", 0), bar("AAPL", 5)]})
    await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for([], calls))
    expected = (START + timedelta(days=6)).strftime("%Y-%m-%d")
    assert calls[0][1] == expected


@pytest.mark.asyncio
async def test_full_backfill_ignores_stored_history():
    calls = []
    repo = RecordingRepo(existing={"AAPL": [bar("AAPL", 5)]})
    await ingest_symbols(
        repo, ["AAPL"], full_backfill=True, fetcher=fetcher_for([], calls)
    )
    assert calls[0][1] == DEFAULT_BACKFILL_START.strftime("%Y-%m-%d")


@pytest.mark.asyncio
async def test_an_explicit_start_overrides_the_resume_point():
    calls = []
    repo = RecordingRepo(existing={"AAPL": [bar("AAPL", 5)]})
    await ingest_symbols(
        repo,
        ["AAPL"],
        start=datetime(2020, 3, 1, tzinfo=timezone.utc),
        fetcher=fetcher_for([], calls),
    )
    assert calls[0][1] == "2020-03-01"


@pytest.mark.asyncio
async def test_an_up_to_date_symbol_is_not_fetched():
    calls = []
    future = datetime.now(timezone.utc) + timedelta(days=10)
    repo = RecordingRepo(
        existing={
            "AAPL": [
                MarketDataRecord(
                    asset=repo_asset(),
                    ohlcv=OHLCV(
                        open=1, high=1, low=1, close=1, volume=1,
                        timestamp=Timestamp(utc=future),
                    ),
                )
            ]
        }
    )
    report = await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for([], calls))
    assert calls == []
    assert report.outcomes[0].error is None


def repo_asset():
    return Asset(symbol="AAPL", asset_class="equity", source="yfinance")


# ---------------------------------------------------------------------------
# Failure isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_an_unregistered_symbol_is_reported_not_fetched():
    repo = RecordingRepo()
    report = await ingest_symbols(
        repo, ["NOPE"], fetcher=fetcher_for([bar("NOPE", 0)])
    )
    assert "registry" in report.outcomes[0].error
    assert repo.written == []


@pytest.mark.asyncio
async def test_one_failing_symbol_does_not_end_the_run():
    """
    Symbols are processed one at a time precisely so this holds — a batched
    download would lose every symbol to one bad ticker.
    """
    def flaky(symbols, start_date, end_date):
        if symbols[0] == "BTC-USD":
            raise RuntimeError("provider exploded")
        return [bar("AAPL", 0)]

    repo = RecordingRepo()
    report = await ingest_symbols(repo, ["BTC-USD", "AAPL"], fetcher=flaky)

    assert report.failed == ["BTC-USD"]
    assert report.written == 1


@pytest.mark.asyncio
async def test_progress_is_reported_for_every_symbol_including_failures():
    seen = []
    repo = RecordingRepo()
    await ingest_symbols(
        repo,
        ["AAPL", "NOPE", "BTC-USD"],
        fetcher=fetcher_for([]),
        progress=lambda done, total, symbol: seen.append((done, total, symbol)),
    )
    assert [s[2] for s in seen] == ["AAPL", "NOPE", "BTC-USD"]
    assert seen[-1][0] == 3


@pytest.mark.asyncio
async def test_report_totals_written_across_symbols():
    repo = RecordingRepo()
    report = await ingest_symbols(
        repo,
        ["AAPL"],
        fetcher=fetcher_for([bar("AAPL", 0), bar("AAPL", 1), bar("AAPL", 2)]),
    )
    assert report.written == 3
    assert report.outcomes[0].first_bar == START
    assert report.outcomes[0].last_bar == START + timedelta(days=2)


# ---------------------------------------------------------------------------
# Single-flight guard
# ---------------------------------------------------------------------------

def test_a_second_start_is_refused_while_running():
    guard = IngestJob()
    assert guard.try_start(5) is True
    assert guard.try_start(5) is False


def test_finishing_releases_the_guard():
    guard = IngestJob()
    guard.try_start(1)
    guard.finish(None)
    assert guard.try_start(1) is True


def test_status_reports_progress():
    guard = IngestJob()
    guard.try_start(10)
    guard.note(3, 10, "AAPL")
    status = guard.status()
    assert status["running"] is True
    assert (status["completed"], status["total"]) == (3, 10)
    assert status["current_symbol"] == "AAPL"


def test_status_is_idle_before_any_run():
    assert IngestJob().status()["running"] is False


# ---------------------------------------------------------------------------
# Re-adjustment (corporate actions)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incremental_writes_do_not_overwrite():
    """
    A routine run only adds dates that are missing, so DO NOTHING is right and
    keeps a re-run cheap.
    """
    repo = RecordingRepo()
    await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for([bar("AAPL", 0)]))
    assert repo.replace_calls == [False]


@pytest.mark.asyncio
async def test_full_backfill_asks_the_database_to_overwrite():
    """
    THE corporate-actions regression. yfinance's auto_adjust restates the whole
    series for splits as of the FETCH DATE, so a symbol that splits after its
    bars were stored ends up with two segments adjusted to different as-of
    dates. Measured 2026-08-09: NFLX closed 1260.27 on 2025-07-15 and 125.03 on
    2025-07-16 — a 10:1 split in November 2025 applied to the newer segment
    only, which every strategy reads as a -90% day.

    A full backfill is the fix, but it could not work while write() used ON
    CONFLICT DO NOTHING: every corrected bar collided with an existing row and
    was silently discarded. The re-fetch reported 39,707 bars written and
    changed nothing.
    """
    repo = RecordingRepo(existing={"AAPL": [bar("AAPL", 0)]})
    await ingest_symbols(
        repo, ["AAPL"], full_backfill=True, fetcher=fetcher_for([bar("AAPL", 0)])
    )
    assert repo.replace_calls == [True]


@pytest.mark.asyncio
async def test_written_counts_rows_the_database_accepted():
    """
    Not rows submitted. The two differ whenever bars already exist, and
    reporting the latter is what made a no-op refresh look successful.
    """

    class HalfRejecting(RecordingRepo):
        async def write(self, records, replace: bool = False):
            self.written.extend(records)
            self.replace_calls.append(replace)
            return len(records) // 2  # as if half collided

    repo = HalfRejecting()
    report = await ingest_symbols(
        repo, ["AAPL"], fetcher=fetcher_for([bar("AAPL", i) for i in range(4)])
    )
    assert report.outcomes[0].fetched == 4
    assert report.outcomes[0].written == 2


# ---------------------------------------------------------------------------
# Skipping symbols already flagged unresolved
# ---------------------------------------------------------------------------

FLAGGED = datetime(2026, 8, 9, tzinfo=timezone.utc)


def flagged_repo(delisted_at=FLAGGED):
    """A registry whose one asset is already flagged unresolved."""
    return RecordingRepo(
        assets={
            "WBA": Asset(
                symbol="WBA",
                asset_class="equity",
                source="yfinance",
                delisted_at=delisted_at,
            )
        }
    )


@pytest.mark.asyncio
async def test_a_flagged_symbol_is_not_re_fetched():
    """
    The bug this covers: nothing consulted `delisted_at`, so eleven known-dead
    symbols each cost a 13-month provider round trip and an ERROR line every
    morning — while the run still exited 0, so nothing reported it.

    Asserting on the FETCHER, not on the report: the point is that the network
    call does not happen, and a report field could be set either way.
    """
    repo = flagged_repo()
    calls = []
    report = await ingest_symbols(
        repo, ["WBA"], fetcher=fetcher_for([bar("WBA", 0)], calls)
    )
    assert calls == []                      # the provider was never asked
    assert repo.written == []
    assert report.skipped_delisted == ["WBA"]
    assert report.failed == []              # a skip is not a failure


@pytest.mark.asyncio
async def test_full_backfill_still_fetches_a_flagged_symbol():
    """
    The repair path must stay open. `mark_full_refresh` clears the flag, so if
    a backfill skipped flagged assets a mislabelled symbol could never be
    fixed through the CLI — which is precisely the state BK, FI and MMC were
    in after being wrongly flagged as delisted rather than renamed.
    """
    repo = flagged_repo()
    calls = []
    report = await ingest_symbols(
        repo,
        ["WBA"],
        full_backfill=True,
        fetcher=fetcher_for([bar("WBA", 0)], calls),
    )
    assert len(calls) == 1
    assert report.skipped_delisted == []
    assert len(repo.written) == 1


@pytest.mark.asyncio
async def test_naming_a_symbol_explicitly_overrides_the_skip():
    """`--symbols BNY` must not be silently ignored because BNY is flagged."""
    repo = flagged_repo()
    calls = []
    report = await ingest_symbols(
        repo, ["WBA"], fetcher=fetcher_for([bar("WBA", 0)], calls),
        skip_delisted=False,
    )
    assert len(calls) == 1
    assert report.skipped_delisted == []


@pytest.mark.asyncio
async def test_an_unflagged_symbol_is_still_fetched():
    """Guards against the skip swallowing healthy symbols."""
    repo = flagged_repo(delisted_at=None)
    calls = []
    report = await ingest_symbols(
        repo, ["WBA"], fetcher=fetcher_for([bar("WBA", 0)], calls)
    )
    assert len(calls) == 1
    assert report.skipped_delisted == []
