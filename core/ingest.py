"""
core/ingest.py
Fetching new price bars and writing them to TimescaleDB.

WHY THIS EXISTS AT ALL. The Streamlit "Run Data Ingestion Pipeline" button
shelled out to `cli/run_pipeline.py`, which builds a PipelineOrchestrator over
a `sqlite3.Connection` and writes to `quant_pipeline.db`. NOTHING in that path
touches TimescaleDB — the migration was a one-time script — so every ingest
run since the cutover has been filling a database the API does not read. The
newest bar in TimescaleDB was 2025-07-15 while the button reported success.

So this is not a port of that button. It is the missing write path: fetch
through the existing adapters, which already return MarketDataRecord, and
persist through the repository, whose write() is an idempotent upsert
(ON CONFLICT DO NOTHING on the (time, asset_id) primary key) and therefore
safe to re-run.

Pure of FastAPI so it can be tested directly and driven from a CLI later.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Protocol

from core.models import Asset, MarketDataRecord, OHLCV, Timestamp

logger = logging.getLogger(__name__)

#: How far back to reach for a symbol with no stored bars at all.
DEFAULT_BACKFILL_START = datetime(2015, 1, 1, tzinfo=timezone.utc)

#: Rows per write. The repository clamps bind parameters, but batching also
#: means a failure part-way through leaves earlier symbols persisted.
WRITE_BATCH = 2_000


class Fetcher(Protocol):
    def __call__(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> List[MarketDataRecord]:
        ...


def default_fetcher(
    symbols: List[str], start_date: str, end_date: str
) -> List[MarketDataRecord]:
    from core.adapters import yfinance_adapter

    return yfinance_adapter.fetch(symbols, start_date, end_date)


@dataclass
class SymbolOutcome:
    symbol: str
    fetched: int = 0
    written: int = 0
    skipped_empty: int = 0
    error: Optional[str] = None
    first_bar: Optional[datetime] = None
    last_bar: Optional[datetime] = None


@dataclass
class IngestReport:
    symbols: List[str] = field(default_factory=list)
    outcomes: List[SymbolOutcome] = field(default_factory=list)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @property
    def written(self) -> int:
        return sum(o.written for o in self.outcomes)

    @property
    def failed(self) -> List[str]:
        return [o.symbol for o in self.outcomes if o.error]


def is_empty_bar(ohlcv: OHLCV) -> bool:
    """
    True when every price field is missing.

    The Phase 2 cutover chose to skip these rather than store all-NULL rows —
    five crypto tickers in the legacy database were nothing but padding. A
    NaN close also poisons every downstream metric, so they are counted and
    dropped rather than written.
    """
    values = (ohlcv.open, ohlcv.high, ohlcv.low, ohlcv.close)
    return all(v is None or v != v for v in values)


def retag(record: MarketDataRecord, asset_class: str, source: str) -> MarketDataRecord:
    """
    Force a record onto a known asset identity.

    yfinance_adapter hardcodes asset_class="equity". Writing BTC-USD that way
    would not merely mislabel it: assets are keyed on
    (symbol, asset_class, source), so it would CREATE A SECOND asset row for a
    symbol that already exists as crypto, and its bars would land under an id
    no query uses.
    """
    return MarketDataRecord(
        asset=Asset(
            symbol=record.asset.symbol,
            asset_class=asset_class,
            source=source,
            metadata=record.asset.metadata,
        ),
        ohlcv=record.ohlcv,
    )


async def ingest_symbols(
    repo: Any,
    symbols: List[str],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    full_backfill: bool = False,
    fetcher: Fetcher = default_fetcher,
    progress: Optional[Callable[[int, int, str], None]] = None,
    run_in_thread: Optional[Callable] = None,
) -> IngestReport:
    """
    Fetch and persist bars for each symbol.

    Args:
        repo:          A MarketDataRepository.
        start/end:     Explicit window. When `start` is omitted the window
                       begins the day after the symbol's newest stored bar,
                       so a routine run fetches only what is missing.
        full_backfill: Ignore stored history and start from
                       DEFAULT_BACKFILL_START.
        fetcher:       Injected so tests never reach the network.
        run_in_thread: Optional awaitable-returning wrapper for the blocking
                       fetch (FastAPI passes run_in_threadpool).

    Symbols are processed ONE AT A TIME rather than in one batched download.
    It is slower, but a symbol that fails cannot take the others with it, and
    per-symbol progress is what a long run needs to report.
    """
    report = IngestReport(symbols=list(symbols), started_at=datetime.now(timezone.utc))
    end = end or datetime.now(timezone.utc)
    total = len(symbols)

    for index, symbol in enumerate(symbols, start=1):
        outcome = SymbolOutcome(symbol=symbol)
        report.outcomes.append(outcome)

        try:
            asset = await repo.find_asset(symbol)
            if asset is None:
                outcome.error = "Not in the asset registry — add it first."
                if progress:
                    progress(index, total, symbol)
                continue

            window_start = start
            if window_start is None:
                window_start = (
                    DEFAULT_BACKFILL_START
                    if full_backfill
                    else await _resume_point(repo, symbol)
                )

            if window_start >= end:
                # Already current. Not an error, and not a fetch.
                if progress:
                    progress(index, total, symbol)
                continue

            call = lambda: fetcher(  # noqa: E731 — bound per iteration
                [symbol],
                window_start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
            )
            raw = await run_in_thread(call) if run_in_thread else call()

            outcome.fetched = len(raw)
            usable: List[MarketDataRecord] = []
            for record in raw:
                if is_empty_bar(record.ohlcv):
                    outcome.skipped_empty += 1
                    continue
                usable.append(retag(record, asset.asset_class, asset.source))

            for start_index in range(0, len(usable), WRITE_BATCH):
                await repo.write(usable[start_index : start_index + WRITE_BATCH])

            outcome.written = len(usable)
            if usable:
                stamps = [r.ohlcv.timestamp.utc for r in usable]
                outcome.first_bar, outcome.last_bar = min(stamps), max(stamps)

        except Exception as exc:  # noqa: BLE001 — one symbol must not end the run
            logger.exception("Ingest failed for %s", symbol)
            outcome.error = str(exc)

        if progress:
            progress(index, total, symbol)

    report.finished_at = datetime.now(timezone.utc)
    return report


async def _resume_point(repo: Any, symbol: str) -> datetime:
    """
    The day after the newest stored bar, or DEFAULT_BACKFILL_START if none.

    +1 day rather than the stored date itself: fetch_range is inclusive at
    both ends, so re-requesting the last stored day would re-download a bar
    the upsert then discards.
    """
    records = await repo.fetch_range(
        symbol=symbol,
        asset_class=None,
        start=DEFAULT_BACKFILL_START,
        end=datetime.now(timezone.utc),
    )
    if not records:
        return DEFAULT_BACKFILL_START
    newest = max(r.ohlcv.timestamp.utc for r in records)
    return newest + timedelta(days=1)


# ---------------------------------------------------------------------------
# Single-flight guard
# ---------------------------------------------------------------------------

class IngestJob:
    """
    Tracks the one ingest allowed to run at a time.

    Two overlapping runs would both fetch the same window and write the same
    rows. The upsert makes that harmless to the data but not to the provider —
    it doubles the requests to Yahoo for no benefit — and the progress stream
    of two interleaved runs is meaningless.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running = False
        self.started_at: Optional[datetime] = None
        self.completed: int = 0
        self.total: int = 0
        self.current: Optional[str] = None
        self.last_report: Optional[IngestReport] = None

    def try_start(self, total: int) -> bool:
        with self._lock:
            if self._running:
                return False
            self._running = True
            self.started_at = datetime.now(timezone.utc)
            self.completed = 0
            self.total = total
            self.current = None
            return True

    def note(self, completed: int, total: int, symbol: str) -> None:
        self.completed, self.total, self.current = completed, total, symbol

    def finish(self, report: Optional[IngestReport]) -> None:
        with self._lock:
            self._running = False
            self.current = None
            if report is not None:
                self.last_report = report

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    def status(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "started_at": self.started_at,
            "completed": self.completed,
            "total": self.total,
            "current_symbol": self.current,
        }


#: Process-wide. A multi-worker deployment would need this in the database
#: instead; with one worker it is the correct scope.
job = IngestJob()
