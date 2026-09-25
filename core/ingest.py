"""
core/ingest.py
Fetching new price bars and writing them to TimescaleDB.

WHY THIS EXISTS AT ALL. The Streamlit "Run Data Ingestion Pipeline" button
shelled out to `cli/run_pipeline.py`, which built a PipelineOrchestrator over
a `sqlite3.Connection` and wrote to `quant_pipeline.db`. NOTHING in that path
touched TimescaleDB — the migration was a one-time script — so every ingest
run since the cutover filled a database the API does not read. The newest bar
in TimescaleDB was 2025-07-15 while the button reported success.

Both callers now come here: the API's POST /api/v1/ingest and, since
2026-08-09, `cli/run_pipeline.py`. The orchestrator was deleted.

So this is not a port of that button. It is the missing write path: fetch
through the existing adapters, which already return MarketDataRecord, and
persist through the repository.

An incremental run upserts with ON CONFLICT DO NOTHING, so re-running it is
idempotent and cheap. A `full_backfill` run passes replace=True instead,
because it exists to RESTATE history: yfinance's auto_adjust re-adjusts the
whole series for splits as of the fetch date, so a symbol that split after
its bars were stored ends up with two segments adjusted to different as-of
dates. DO NOTHING made that refresh a silent no-op.

Pure of FastAPI so it can be tested directly and driven from a CLI later.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Protocol

from config.settings import INGEST_OVERLAP_DAYS, MAX_DAILY_MOVE_MULTIPLE
from core.corporate_actions import looks_unresolved
from core.crypto_identity import ingest_block_reason, metadata_allows_ingest
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
    #: Bars inserted BEHIND the newest stored bar — holes the overlap window
    #: found and filled. Non-zero means a previous run lost a day.
    filled: int = 0
    error: Optional[str] = None
    first_bar: Optional[datetime] = None
    last_bar: Optional[datetime] = None
    #: Set when an empty fetch plus a stale newest bar says the provider no
    #: longer serves this symbol, rather than "already current" — the two were
    #: indistinguishable before. NOT proof of delisting: it may be a rename.
    delisted: bool = False
    #: Set when the symbol was already flagged and this run did not re-fetch it.
    skipped_delisted: bool = False
    #: Set when a RECORDED identity verdict says this ticker is not the asset
    #: we meant, so it was not fetched. See core/crypto_identity.py.
    skipped_identity: bool = False
    #: Bars refused because they predate the asset's recorded history floor —
    #: a contaminated prefix that was deliberately removed.
    skipped_before_floor: int = 0
    #: Bars refused because they postdate the asset's history ceiling — the
    #: ticker was delisted and something else now trades under it.
    skipped_after_ceiling: int = 0
    #: Bars whose move from the previous bar is beyond MAX_DAILY_MOVE_MULTIPLE.
    #: Counted and reported, never dropped — see `implausible_jump`. Yahoo's
    #: own TIA-USD closes 0.0105 then 7149.41 on 2024-03-26.
    implausible_jumps: int = 0


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

    @property
    def delisted(self) -> List[str]:
        return [o.symbol for o in self.outcomes if o.delisted]

    @property
    def skipped_delisted(self) -> List[str]:
        return [o.symbol for o in self.outcomes if o.skipped_delisted]

    @property
    def skipped_identity(self) -> List[str]:
        return [o.symbol for o in self.outcomes if o.skipped_identity]

    @property
    def implausible(self) -> List[str]:
        """Symbols carrying at least one bar beyond MAX_DAILY_MOVE_MULTIPLE.

        Reported, not acted on: the bars were stored. A quiet counter on an
        outcome nobody prints is how 35 corrupt crypto series went two years
        without anyone noticing."""
        return [o.symbol for o in self.outcomes if o.implausible_jumps]

    @property
    def filled(self) -> Dict[str, int]:
        """Symbols whose missing bars the overlap window refilled, and how many.

        Each one is a day an earlier run lost. Before the overlap, 463 equities
        lost 2026-08-28 and nothing said so for a month."""
        return {o.symbol: o.filled for o in self.outcomes if o.filled}


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


def implausible_jump(
    previous_close: Optional[float],
    close: Optional[float],
    limit: float = MAX_DAILY_MOVE_MULTIPLE,
) -> bool:
    """
    True when one bar to the next moves by more than `limit` times.

    FLAGGED, NEVER DROPPED — the opposite of `is_empty_bar` above, and
    deliberately so. An all-NULL bar carries no information, so discarding it
    loses nothing. A 680,637x move carries a great deal: either the provider is
    wrong or something real happened, and crypto genuinely does 10x in a day
    (BONK, WIF and FARTCOIN are all in this universe).

    Dropping it would also be self-defeating. Yahoo's TIA-USD runs
    0.0105 -> 7149.41 -> 298.86; remove the spike and 0.0105 -> 298.86 is still
    impossible, so one bad bar has been traded for another. And the hole left
    behind is indistinguishable from a provider outage — a shape this project
    has already spent a day diagnosing once (PARA's 388-day gap).

    So this counts and reports. The decision stays with a human.
    """
    if previous_close is None or close is None:
        return False
    if previous_close != previous_close or close != close:  # NaN
        return False
    if previous_close <= 0 or close <= 0:
        return False
    hi, lo = max(previous_close, close), min(previous_close, close)
    return (hi / lo) > limit


#: Asset metadata key: the earliest date whose bars belong to THIS instrument.
META_HISTORY_VALID_FROM = "history_valid_from"

#: And the first date whose bars no longer do. The mirror image, and needed for
#: a different event: a ticker that is DELISTED and later REASSIGNED. Measured
#: on PARA — Paramount Global filed a Form 25-NSE on 2025-08-07, and from
#: 2026-08-07 the key serves an unrelated penny stock. The good history is a
#: prefix and the contamination a suffix, which is exactly what a floor cannot
#: express.
META_HISTORY_VALID_UNTIL = "history_valid_until"


def history_floor(metadata: Optional[Dict[str, Any]]) -> Optional[datetime]:
    """
    The earliest date whose bars belong to this instrument, if recorded.

    WHY THIS IS NEEDED. Trimming a contaminated prefix is not durable on its
    own: the provider still serves those bars. TIA-USD's first 1105 bars were a
    micro-cap — every close below Celestia's all-time low, then a 432x jump on
    2025-03-09 — and they were deleted. But `--full-backfill` starts at
    DEFAULT_BACKFILL_START (2015), Yahoo happily returns the micro-cap again,
    and the identity gate does NOT block it, because the identity is a correct
    MATCH. One backfill would silently undo the repair.

    So a cleaned asset records where its real history begins, and ingestion
    refuses to write before it.
    """
    raw = (metadata or {}).get(META_HISTORY_VALID_FROM)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw))
    except ValueError:
        logger.warning("Unparseable %s: %r", META_HISTORY_VALID_FROM, raw)
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def history_ceiling(metadata: Optional[Dict[str, Any]]) -> Optional[datetime]:
    """
    The first date whose bars no longer belong to this instrument.

    `delisted_at` alone is not enough to make a delisting stick. The routine
    skip gate honours it, but `--full-backfill` deliberately IGNORES that gate,
    because a backfill is the repair path for a symbol wrongly flagged. So a
    single backfill of a delisted-and-reassigned ticker re-imports the
    successor's prices, and the identity gate does not stop it either — for an
    equity there is no reference to check against.

    A ceiling is checked by ingestion itself, so it holds whatever the caller
    asks for.
    """
    raw = (metadata or {}).get(META_HISTORY_VALID_UNTIL)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw))
    except ValueError:
        logger.warning("Unparseable %s: %r", META_HISTORY_VALID_UNTIL, raw)
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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
    skip_delisted: bool = True,
    skip_unsafe_identity: bool = True,
) -> IngestReport:
    """
    Fetch and persist bars for each symbol.

    Args:
        repo:          A MarketDataRepository.
        start/end:     Explicit window. When `start` is omitted the window
                       begins INGEST_OVERLAP_DAYS before the symbol's newest
                       stored bar, so a routine run also refills any day an
                       earlier run lost. Existing bars are never rewritten.
        full_backfill: Ignore stored history and start from
                       DEFAULT_BACKFILL_START.
        fetcher:       Injected so tests never reach the network.
        run_in_thread: Optional awaitable-returning wrapper for the blocking
                       fetch (FastAPI passes run_in_threadpool).
        skip_delisted: Skip assets already flagged unresolved. Pass False when
                       the caller named the symbols explicitly — skipping a
                       symbol somebody asked for by name is the wrong default.
                       Ignored when `full_backfill` is set, which repairs.
        skip_unsafe_identity:
                       Skip assets whose RECORDED identity verdict says the
                       ticker is not the asset we meant. Unlike skip_delisted
                       this is NOT ignored by full_backfill — see the gate.

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

            # Already known unresolvable: don't re-ask the provider every run.
            # Before this gate, eleven dead symbols each cost a 13-month fetch
            # and an ERROR line every morning while the run still exited 0.
            #
            # Deliberately NOT applied when full_backfill is set: that is the
            # repair path, and `mark_full_refresh` clears the flag. Skipping
            # here would make a flagged symbol unrepairable through the CLI —
            # which is exactly how BK/FI/MMC would have stayed broken.
            if skip_delisted and not full_backfill and asset.delisted_at:
                outcome.skipped_delisted = True
                # Deliberately NOT "re-check with --full-backfill": a backfill
                # re-asks the provider about the OLD ticker, which stays dead
                # whether the cause was a delisting or a rename, so it can
                # never surface a successor. Clearing this flag means finding
                # the new ticker and renaming the asset row — how BK/FI/MMC
                # were repaired on 2026-08-09.
                logger.info(
                    "%s: skipped, flagged unresolved at %s. If this was a "
                    "RENAME, find the successor ticker and rename the asset "
                    "row (bars follow asset_id); a backfill of %s alone will "
                    "not find it.",
                    symbol,
                    asset.delisted_at.date().isoformat(),
                    symbol,
                )
                if progress:
                    progress(index, total, symbol)
                continue

            # A ticker is not an identifier. Yahoo's MNT-USD is a micro-cap
            # called MINTY; Mantle is elsewhere. 22 of 99 crypto assets held a
            # different coin's entire history (27,076 bars) before this gate,
            # and every daily run appended more of it.
            #
            # DELIBERATELY NOT BYPASSED BY full_backfill — the opposite of the
            # delisted gate above, and the asymmetry is the point. A backfill
            # REPAIRS a stale-adjustment or a wrongly-flagged symbol, so it must
            # reach those. It cannot repair a wrong asset: refetching UNI-USD
            # just imports more UNICORN Token. The only way past this gate is to
            # fix the mapping and re-verify, which flips the recorded verdict.
            if skip_unsafe_identity and not metadata_allows_ingest(asset.metadata):
                outcome.skipped_identity = True
                logger.warning(
                    "%s: skipped — %s. Re-verify with "
                    "scripts/audit_crypto_identity.py once the cause is fixed; "
                    "a backfill will NOT clear this.",
                    symbol,
                    ingest_block_reason(asset.metadata) or "recorded as unsafe",
                )
                if progress:
                    progress(index, total, symbol)
                continue

            newest_stored = await _newest_bar(repo, symbol)

            window_start = start
            if window_start is None:
                window_start = (
                    DEFAULT_BACKFILL_START
                    if full_backfill
                    else (
                        newest_stored + timedelta(days=1)
                        if newest_stored
                        else DEFAULT_BACKFILL_START
                    )
                )

            # A cleaned asset knows where its real history starts. Clamp the
            # window rather than trusting the provider not to serve the
            # contaminated prefix again.
            floor = history_floor(asset.metadata)
            if floor is not None and window_start < floor:
                window_start = floor

            # And where its real history STOPS. A delisted ticker that was
            # later reassigned keeps serving prices — they just belong to
            # somebody else now.
            ceiling = history_ceiling(asset.metadata)
            window_end = end
            if ceiling is not None and window_end > ceiling:
                window_end = ceiling

            if window_start >= window_end:
                # Already current. Not an error, and not a fetch.
                if progress:
                    progress(index, total, symbol)
                continue

            # Reach back behind the newest bar. Resuming at newest + 1 cannot
            # heal a hole: once any later bar is stored, a lost day is never
            # requested again. Existing bars are untouched (DO NOTHING), so the
            # overlap only ever INSERTS a missing day. Only for a routine run:
            # an explicit `start` and a full backfill already say what to fetch.
            overlapping = (
                start is None and not full_backfill and newest_stored is not None
            )
            if overlapping:
                window_start = newest_stored - timedelta(days=INGEST_OVERLAP_DAYS)
                if floor is not None and window_start < floor:
                    window_start = floor

            call = lambda: fetcher(  # noqa: E731 — bound per iteration
                [symbol],
                window_start.strftime("%Y-%m-%d"),
                window_end.strftime("%Y-%m-%d"),
            )
            raw = await run_in_thread(call) if run_in_thread else call()

            outcome.fetched = len(raw)
            usable: List[MarketDataRecord] = []
            empty_days: List[str] = []
            for record in raw:
                if is_empty_bar(record.ohlcv):
                    outcome.skipped_empty += 1
                    empty_days.append(record.ohlcv.timestamp.utc.date().isoformat())
                    continue
                # Belt and braces: a provider may return bars earlier than the
                # window it was asked for, and those are exactly the ones a
                # cleaned asset must never see again.
                if floor is not None and record.ohlcv.timestamp.utc < floor:
                    outcome.skipped_before_floor += 1
                    continue
                if ceiling is not None and record.ohlcv.timestamp.utc >= ceiling:
                    outcome.skipped_after_ceiling += 1
                    continue
                usable.append(retag(record, asset.asset_class, asset.source))

            # Plausibility is judged on the fetched run of bars, in date
            # order — the cheap check that would have surfaced 35 corrupt
            # crypto series on the day they were ingested instead of two
            # years later.
            in_order = sorted(usable, key=lambda r: r.ohlcv.timestamp.utc)
            for earlier, later in zip(in_order, in_order[1:]):
                if implausible_jump(earlier.ohlcv.close, later.ohlcv.close):
                    outcome.implausible_jumps += 1
            if outcome.implausible_jumps:
                logger.warning(
                    "%s: %d bar(s) move more than %gx from the previous bar. "
                    "Stored anyway — this is usually the PROVIDER serving a "
                    "wrong or mixed series, not a fetch fault. Check identity "
                    "before trusting this symbol (core/crypto_identity.py).",
                    symbol,
                    outcome.implausible_jumps,
                    MAX_DAILY_MOVE_MULTIPLE,
                )

            # replace=full_backfill. An incremental run writes only new dates,
            # so DO NOTHING is right and cheap. A full backfill exists to
            # RESTATE history — yfinance re-adjusts the whole series for splits
            # as of the fetch date — and DO NOTHING made that a silent no-op.
            if empty_days:
                # Logged by date so a lost day can be traced. On 2026-09-22
                # DXCM's bar never landed across three runs and nothing said
                # whether Yahoo had served it empty or not at all.
                logger.info(
                    "%s: dropped %d empty bar(s): %s",
                    symbol, len(empty_days), ", ".join(sorted(empty_days)),
                )

            # Bars behind the newest stored one can only be holes being filled
            # — the overlap re-requests days already held, and DO NOTHING
            # discards those. Written as their own batch so the fill is counted.
            persisted = 0
            if overlapping:
                behind = [r for r in usable if r.ohlcv.timestamp.utc < newest_stored]
                usable_new = [
                    r for r in usable if r.ohlcv.timestamp.utc >= newest_stored
                ]
                for start_index in range(0, len(behind), WRITE_BATCH):
                    outcome.filled += await repo.write(
                        behind[start_index : start_index + WRITE_BATCH],
                        replace=False,
                    ) or 0
                persisted += outcome.filled
                if outcome.filled:
                    logger.warning(
                        "%s: filled %d missing bar(s) behind the newest stored "
                        "bar — a previous run lost them.",
                        symbol, outcome.filled,
                    )
            else:
                usable_new = usable
            for start_index in range(0, len(usable_new), WRITE_BATCH):
                persisted += await repo.write(
                    usable_new[start_index : start_index + WRITE_BATCH],
                    replace=full_backfill,
                ) or 0

            # Rows the DATABASE accepted, not rows submitted. The two differ
            # whenever bars already exist, and reporting the latter made a
            # no-op refresh look like 39,707 bars written.
            outcome.written = persisted

            # A full backfill has just restated the whole series, so record
            # when — a split newer than this means the bars have drifted.
            if full_backfill and usable and hasattr(repo, "mark_full_refresh"):
                await repo.mark_full_refresh(symbol, datetime.now(timezone.utc))

            # An empty fetch is ambiguous on its own: already current, or gone
            # from the provider. Judged together with how old the newest bar is.
            # "Gone from the provider" is NOT the same as delisted — it is also
            # what a rename looks like, which is how three live S&P 500 names
            # got marked dead. Hence the warning: this stamp needs a human.
            #
            # "Empty" means nothing NEWER than what is stored. The overlap
            # re-serves bars already held, and counting those would stop a dead
            # symbol from ever being flagged.
            newer = [
                r for r in raw
                if newest_stored is None or r.ohlcv.timestamp.utc > newest_stored
            ]
            if not newer and looks_unresolved(newest_stored, len(newer)):
                outcome.delisted = True
                logger.warning(
                    "%s: unresolved at the provider — MAY BE A RENAME, not a "
                    "delisting. Check for a successor ticker before trusting "
                    "this flag (see core/corporate_actions.py).",
                    symbol,
                )
                if hasattr(repo, "mark_delisted"):
                    await repo.mark_delisted(symbol, datetime.now(timezone.utc))
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


async def _newest_bar(repo: Any, symbol: str) -> Optional[datetime]:
    """
    Timestamp of the newest stored bar, or None if there are none.

    This + 1 day decides whether a symbol is already current (no fetch at all),
    and bars newer than it decide whether a fetch means "current" or
    "delisted". The fetch itself reaches INGEST_OVERLAP_DAYS further back.
    """
    records = await repo.fetch_range(
        symbol=symbol,
        asset_class=None,
        start=DEFAULT_BACKFILL_START,
        end=datetime.now(timezone.utc),
    )
    if not records:
        return None
    return max(r.ohlcv.timestamp.utc for r in records)


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
