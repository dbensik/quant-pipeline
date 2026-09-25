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
    history_ceiling,
    history_floor,
    implausible_jump,
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


# ---------------------------------------------------------------------------
# Implausible jumps — FLAGGED, never dropped
#
# The measured case: Yahoo's own TIA-USD closes 0.0105 on 2024-03-25 and
# 7149.41 on 2024-03-26 — 680,637x — then 298.86, then 0.0131. Verified
# present at the provider, so this is not an ingest fault. 35 of 99 crypto
# series carry moves like it.
# ---------------------------------------------------------------------------

def test_the_tia_spike_is_implausible():
    assert implausible_jump(0.0105, 7149.415) is True


def test_a_real_crypto_rally_is_not_flagged():
    """
    Crypto genuinely doubles in a day; BONK, WIF and FARTCOIN are all in this
    universe. The threshold has to sit above ordinary violence or it fires on
    real data and gets switched off.
    """
    assert implausible_jump(1.00, 2.50) is False
    assert implausible_jump(1.00, 9.00) is False


def test_the_threshold_is_symmetric():
    """A 20x crash is exactly as impossible as a 20x rally."""
    assert implausible_jump(100.0, 5.0) is True
    assert implausible_jump(5.0, 100.0) is True


def test_missing_or_nonsense_prices_cannot_be_implausible():
    assert implausible_jump(None, 5.0) is False
    assert implausible_jump(5.0, None) is False
    assert implausible_jump(float("nan"), 5.0) is False
    assert implausible_jump(0.0, 5.0) is False
    assert implausible_jump(-1.0, 5.0) is False


@pytest.mark.asyncio
async def test_implausible_bars_are_counted_AND_STILL_WRITTEN():
    """
    The distinction from `is_empty_bar`, and the whole point.

    An all-NULL bar carries no information, so it is dropped. A 680,637x move
    carries plenty — either the provider is wrong or something real happened —
    so it is counted and kept. Dropping it would also be self-defeating:
    remove the spike from 0.0105 -> 7149.41 -> 298.86 and the remaining
    0.0105 -> 298.86 is still impossible, trading one bad bar for another,
    while the hole left behind is indistinguishable from a provider outage.
    """
    repo = RecordingRepo()
    report = await ingest_symbols(
        repo,
        ["BTC-USD"],
        fetcher=fetcher_for([
            bar("BTC-USD", 0, close=0.0105),
            bar("BTC-USD", 1, close=7149.415),
            bar("BTC-USD", 2, close=298.86),
        ]),
    )
    outcome = report.outcomes[0]
    assert outcome.implausible_jumps == 2      # into the spike and out of it
    assert outcome.skipped_empty == 0          # not confused with empty bars
    assert len(repo.written) == 3              # NOTHING was dropped
    assert outcome.written == 3


@pytest.mark.asyncio
async def test_a_clean_series_reports_no_jumps():
    repo = RecordingRepo()
    report = await ingest_symbols(
        repo,
        ["AAPL"],
        fetcher=fetcher_for([bar("AAPL", i, close=100.0 + i) for i in range(4)]),
    )
    assert report.outcomes[0].implausible_jumps == 0


@pytest.mark.asyncio
async def test_jumps_are_judged_in_date_order_not_fetch_order():
    """
    A provider that returns bars out of order would otherwise manufacture
    jumps that the series does not contain.
    """
    repo = RecordingRepo()
    shuffled = [bar("AAPL", 2, close=102.0), bar("AAPL", 0, close=100.0),
                bar("AAPL", 1, close=101.0)]
    report = await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for(shuffled))
    assert report.outcomes[0].implausible_jumps == 0


# ---------------------------------------------------------------------------
# Identity gate — a ticker is not an identifier
#
# Audited 2026-09-17: 22 of 99 crypto assets held a DIFFERENT coin's entire
# history, 27,076 bars. Yahoo's MNT-USD is a micro-cap called MINTY; Uniswap
# was stored as UNICORN Token. Every daily run appended more of it.
# ---------------------------------------------------------------------------

def identity_repo(status):
    """A repo whose one crypto asset carries `status` as its recorded verdict."""
    meta = {"identity_status": status} if status else {}
    return RecordingRepo(assets={
        "UNI-USD": Asset(symbol="UNI-USD", asset_class="crypto",
                         source="yfinance", metadata=meta),
    })


@pytest.mark.asyncio
async def test_a_wrong_asset_is_not_fetched():
    repo = identity_repo("wrong_asset")
    calls = []
    report = await ingest_symbols(
        repo, ["UNI-USD"], fetcher=fetcher_for([bar("UNI-USD", 0)], calls)
    )
    assert report.outcomes[0].skipped_identity is True
    assert report.skipped_identity == ["UNI-USD"]
    assert calls == []            # the provider was never asked
    assert repo.written == []     # and nothing was stored


@pytest.mark.asyncio
async def test_a_suspect_asset_is_not_fetched():
    """
    SUSPECT is a live question, not a clearance — the stablecoins where price
    cannot distinguish BlackRock's BUIDL from DFOhub's.
    """
    repo = identity_repo("suspect")
    await ingest_symbols(repo, ["UNI-USD"], fetcher=fetcher_for([bar("UNI-USD", 0)]))
    assert repo.written == []


@pytest.mark.asyncio
async def test_a_verified_asset_is_fetched_normally():
    repo = identity_repo("match")
    await ingest_symbols(repo, ["UNI-USD"], fetcher=fetcher_for([bar("UNI-USD", 0)]))
    assert len(repo.written) == 1


@pytest.mark.asyncio
async def test_unverifiable_is_allowed_through():
    """
    Absence of evidence, not evidence of substitution: 23 coins sit below the
    reference set. Blocking them would drop real data for no finding.
    """
    repo = identity_repo("unverifiable")
    await ingest_symbols(repo, ["UNI-USD"], fetcher=fetcher_for([bar("UNI-USD", 0)]))
    assert len(repo.written) == 1


@pytest.mark.asyncio
async def test_an_unchecked_asset_is_never_blocked():
    """
    THE REGRESSION THAT WOULD STOP THE DAILY RUN. 516 equities and 11 ETFs
    carry no identity metadata at all. Treating "unchecked" as "unsafe" would
    halt the entire pipeline the day this shipped.
    """
    repo = RecordingRepo()  # AAPL, no metadata
    await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for([bar("AAPL", 0)]))
    assert len(repo.written) == 1


@pytest.mark.asyncio
async def test_a_backfill_does_NOT_bypass_the_identity_gate():
    """
    The asymmetry with `skip_delisted`, and the whole reason the gate exists.

    A full backfill REPAIRS a stale adjustment or a wrongly-flagged symbol, so
    it deliberately ignores the delisted gate. It cannot repair a wrong asset:
    refetching UNI-USD imports more UNICORN Token. Letting a backfill through
    here would reintroduce exactly the 27,076 bars this is meant to stop.
    """
    repo = identity_repo("wrong_asset")
    await ingest_symbols(
        repo, ["UNI-USD"], full_backfill=True,
        fetcher=fetcher_for([bar("UNI-USD", 0)]),
    )
    assert repo.written == []


@pytest.mark.asyncio
async def test_the_gate_can_be_overridden_explicitly():
    """An operator who has fixed the mapping by hand needs a way through."""
    repo = identity_repo("wrong_asset")
    await ingest_symbols(
        repo, ["UNI-USD"], skip_unsafe_identity=False,
        fetcher=fetcher_for([bar("UNI-USD", 0)]),
    )
    assert len(repo.written) == 1


@pytest.mark.asyncio
async def test_an_unrecognised_verdict_is_not_a_verdict():
    """Someone else's metadata must not silently block ingestion."""
    repo = identity_repo("banana")
    await ingest_symbols(repo, ["UNI-USD"], fetcher=fetcher_for([bar("UNI-USD", 0)]))
    assert len(repo.written) == 1


# ---------------------------------------------------------------------------
# History floor — trimming a contaminated prefix has to STAY trimmed
#
# TIA-USD's first 1105 bars were a micro-cap: every close below Celestia's
# all-time low of $0.279235, then a 432x jump on 2025-03-09. They were deleted.
# But the provider still serves them, --full-backfill starts at 2015, and the
# identity gate does NOT block the symbol because its identity is a correct
# MATCH. Without a floor, one backfill silently undoes the repair.
# ---------------------------------------------------------------------------

FLOOR_META = {"history_valid_from": "2024-01-03"}


def floor_repo():
    return RecordingRepo(assets={
        "TIA-USD": Asset(symbol="TIA-USD", asset_class="crypto",
                         source="yfinance", metadata=dict(FLOOR_META)),
    })


def test_history_floor_is_read_from_metadata():
    assert history_floor({}) is None
    assert history_floor(None) is None
    assert history_floor(FLOOR_META) == datetime(2024, 1, 3, tzinfo=timezone.utc)


def test_an_unparseable_floor_is_ignored_not_fatal():
    """Someone else's metadata must not stop ingestion."""
    assert history_floor({"history_valid_from": "whenever"}) is None


@pytest.mark.asyncio
async def test_bars_before_the_floor_are_refused_even_on_a_backfill():
    """
    THE regression. A backfill asks from 2015; the provider returns the
    contaminated prefix; without this the deleted bars come straight back.
    """
    repo = floor_repo()
    report = await ingest_symbols(
        repo, ["TIA-USD"], full_backfill=True,
        fetcher=fetcher_for([bar("TIA-USD", i, close=1.0 + i) for i in range(6)]),
    )
    # START is 2024-01-01, so days 0 and 1 precede the 2024-01-03 floor.
    assert report.outcomes[0].skipped_before_floor == 2
    assert len(repo.written) == 4
    assert all(r.ohlcv.timestamp.utc >= datetime(2024, 1, 3, tzinfo=timezone.utc)
               for r in repo.written)


@pytest.mark.asyncio
async def test_the_fetch_window_itself_is_clamped_to_the_floor():
    """Cheaper than filtering afterwards, and it is what the live run showed:
    a full backfill of TIA-USD asked for 561 records, not eleven years."""
    repo = floor_repo()
    calls = []
    await ingest_symbols(
        repo, ["TIA-USD"], full_backfill=True, fetcher=fetcher_for([], calls)
    )
    assert calls[0][1] == "2024-01-03"


@pytest.mark.asyncio
async def test_an_asset_with_no_floor_is_unaffected():
    """516 equities carry no floor; they must fetch their whole history."""
    repo = RecordingRepo()
    calls = []
    await ingest_symbols(
        repo, ["AAPL"], full_backfill=True, fetcher=fetcher_for([], calls)
    )
    assert calls[0][1] == DEFAULT_BACKFILL_START.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# History ceiling — a delisted ticker that was later REASSIGNED
#
# PARA's shape. Paramount Global filed a Form 25-NSE on 2025-08-07; from
# 2026-08-07 the key serves an unrelated penny stock at $1.76. The good
# history is a PREFIX and the contamination a SUFFIX, which is exactly what a
# floor cannot express.
# ---------------------------------------------------------------------------

CEILING_META = {"history_valid_until": "2024-01-04"}


def ceiling_repo():
    return RecordingRepo(assets={
        "PARA": Asset(symbol="PARA", asset_class="equity",
                      source="yfinance", metadata=dict(CEILING_META)),
    })


def test_history_ceiling_is_read_from_metadata():
    assert history_ceiling({}) is None
    assert history_ceiling(CEILING_META) == datetime(2024, 1, 4, tzinfo=timezone.utc)


def test_an_unparseable_ceiling_is_ignored_not_fatal():
    assert history_ceiling({"history_valid_until": "soon"}) is None


@pytest.mark.asyncio
async def test_bars_after_the_ceiling_are_refused_even_on_a_backfill():
    """
    THE regression, and why `delisted_at` alone is not enough: the routine
    skip gate honours it, but --full-backfill deliberately IGNORES that gate
    because a backfill is the repair path for a wrongly-flagged symbol. So one
    backfill re-imports the successor's prices, and the identity gate does not
    stop an equity either — there is no reference to check it against.
    """
    repo = ceiling_repo()
    report = await ingest_symbols(
        repo, ["PARA"], full_backfill=True,
        fetcher=fetcher_for([bar("PARA", i, close=10.0 + i) for i in range(6)]),
    )
    # START is 2024-01-01, so days 3..5 fall on or after the 2024-01-04 ceiling.
    assert report.outcomes[0].skipped_after_ceiling == 3
    assert len(repo.written) == 3
    assert all(r.ohlcv.timestamp.utc < datetime(2024, 1, 4, tzinfo=timezone.utc)
               for r in repo.written)


@pytest.mark.asyncio
async def test_the_fetch_window_end_is_clamped_to_the_ceiling():
    repo = ceiling_repo()
    calls = []
    await ingest_symbols(
        repo, ["PARA"], full_backfill=True, fetcher=fetcher_for([], calls)
    )
    assert calls[0][2] == "2024-01-04"


@pytest.mark.asyncio
async def test_a_ceiling_already_passed_means_no_fetch_at_all():
    """Once the window starts after the ceiling there is nothing legitimate
    left to ask for, and the provider must not be called."""
    repo = ceiling_repo()
    calls = []
    await ingest_symbols(
        repo, ["PARA"], start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        fetcher=fetcher_for([bar("PARA", 0)], calls),
    )
    assert calls == []
    assert repo.written == []


@pytest.mark.asyncio
async def test_an_asset_with_no_ceiling_is_unaffected():
    repo = RecordingRepo()
    calls = []
    await ingest_symbols(repo, ["AAPL"], fetcher=fetcher_for([], calls))
    assert calls and calls[0][2] != ""
