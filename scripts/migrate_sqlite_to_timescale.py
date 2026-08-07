"""
scripts/migrate_sqlite_to_timescale.py

One-time migration: reads OHLCV rows from the legacy SQLite database and writes
them into TimescaleDB via the Phase 2 schema.

Run ONCE after `alembic upgrade head` has created the schema:

    # count only, writes nothing
    python scripts/migrate_sqlite_to_timescale.py --dry-run

    # end-to-end smoke test on two tickers (one equity, one crypto)
    python scripts/migrate_sqlite_to_timescale.py --tickers AAPL BTC-USD

    # the real run
    python scripts/migrate_sqlite_to_timescale.py

Source schema (legacy SQLite, table `price_data_daily`)
-------------------------------------------------------
    Timestamp DATETIME, Ticker TEXT, Open/High/Low/Close REAL, Volume INTEGER,
    volatility_90d, beta, sharpe_ratio_90d, rsi_14d REAL

Note there is no asset_class and no source column — both are derived here (see
classify_asset_class). The four trailing indicator columns are DERIVED data and
are intentionally NOT migrated: market_data is the OHLCV source of truth, and
data_pipeline's DataEnricher recomputes these from OHLCV on demand. They remain
available in the archived SQLite file.

Safety guarantees
-----------------
* Idempotent — every insert uses ON CONFLICT DO NOTHING against the composite
  PK (time, asset_id), so re-running is safe and resumable after an interrupt.
* Streaming — rows are iterated off the SQLite cursor, never fetchall()'d, so
  memory stays flat regardless of table size.
* Batched — one multi-row INSERT per batch instead of a statement per row.
* Dry-run — --dry-run counts and classifies without opening a write path.

Phase 2 — TimescaleDB Schema & Repository Layer
"""

import argparse
import asyncio
import logging
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

# Ensure the project root is on sys.path when running as a script
_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import json

from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.models import Asset
from db.models import MarketDataORM
from db.repositories.market_data import TimescaleMarketDataRepo
from db.session import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_SQLITE_PATH = str(_project_root / "quant_pipeline.db")
DEFAULT_TABLE = "price_data_daily"

# Everything in the legacy database was fetched through yfinance — including the
# crypto pairs, which use yfinance's SYMBOL-USD convention rather than CoinGecko ids.
SOURCE = "yfinance"

OHLCV_COLUMNS = ("Open", "High", "Low", "Close", "Volume")

# PostgreSQL's wire protocol caps a single statement at 32767 bind parameters.
# A multi-row INSERT binds one parameter per column per row, so the maximum
# rows per statement is 32767 // (columns per row). market_data has 8 columns
# → 4095 rows. Exceeding this raises asyncpg InterfaceError mid-run.
MAX_BIND_PARAMS = 32_767
COLUMNS_PER_ROW = 8
MAX_ROWS_PER_INSERT = MAX_BIND_PARAMS // COLUMNS_PER_ROW  # 4095


# ---------------------------------------------------------------------------
# Asset classification
# ---------------------------------------------------------------------------

def classify_asset_class(ticker: str) -> str:
    """
    Derive asset_class for a legacy ticker.

    The legacy price tables carry no asset_class column, and the `asset_universe`
    table cannot supply one: its crypto rows are lowercase bare symbols ('btc',
    'eth') while the price tables use yfinance pair notation ('BTC-USD'), so the
    two never join. The '-USD' suffix is the reliable discriminator — it covers
    all 115 tickers absent from asset_universe and agrees with asset_universe on
    every one of the 501 that do match.
    """
    return "crypto" if ticker.upper().endswith("-USD") else "equity"


def load_ticker_metadata(conn: sqlite3.Connection) -> Dict[str, dict]:
    """
    Best-effort sector/index metadata from the legacy `universe` table, keyed by
    ticker. Missing tickers simply get {} — metadata is decorative, not required.
    """
    meta: Dict[str, dict] = {}
    try:
        rows = conn.execute("SELECT ticker, source, metadata FROM universe").fetchall()
    except sqlite3.OperationalError:
        log.warning("No `universe` table found — assets will be created without metadata.")
        return meta

    for row in rows:
        entry: dict = {}
        raw = row["metadata"]
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    entry.update(parsed)
            except (json.JSONDecodeError, TypeError):
                pass
        if row["source"]:
            # e.g. 'S&P 500' — index membership, distinct from the data source
            entry["index"] = row["source"]
        if entry:
            meta[row["ticker"]] = entry
    return meta


# ---------------------------------------------------------------------------
# SQLite reading
# ---------------------------------------------------------------------------

def parse_timestamp(raw) -> datetime:
    """
    Parse a legacy timestamp into an aware UTC datetime.

    Legacy values are naive 'YYYY-MM-DD HH:MM:SS' strings representing daily
    bars. Naive values are interpreted as UTC — for daily bars the date is the
    only meaningful part, so this is a labelling choice, not a time shift.
    """
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(raw))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def is_empty_bar(row: sqlite3.Row) -> bool:
    """
    True when Open/High/Low/Close are all NULL.

    The legacy table pads every ticker across the full date range, so assets that
    did not exist yet carry a row per date with no price data at all (USDT0-USD
    has ~2,023 such rows — its entire span). These carry no information and are
    skipped rather than loaded as NULL rows into the hypertable.
    """
    return all(row[c] is None for c in ("Open", "High", "Low", "Close"))


def iter_rows(
    conn: sqlite3.Connection,
    table: str,
    time_column: str,
    tickers: Optional[Sequence[str]] = None,
) -> Iterator[sqlite3.Row]:
    """Stream rows off the cursor — never materialise the whole table."""
    sql = f'SELECT * FROM "{table}"'
    params: tuple = ()
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        sql += f" WHERE Ticker IN ({placeholders})"
        params = tuple(tickers)
    sql += f' ORDER BY "{time_column}"'

    cursor = conn.execute(sql, params)
    while True:
        chunk = cursor.fetchmany(10_000)
        if not chunk:
            return
        yield from chunk


def detect_time_column(conn: sqlite3.Connection, table: str) -> str:
    """Legacy tables disagree: price_data_daily uses `Timestamp`, price_data uses `Date`."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if not cols:
        raise SystemExit(f"Table '{table}' does not exist in the SQLite database.")
    for candidate in ("Timestamp", "Date", "time", "timestamp", "date"):
        if candidate in cols:
            return candidate
    raise SystemExit(f"No recognisable time column in '{table}'. Columns: {cols}")


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

async def migrate(
    sqlite_path: str,
    table: str = DEFAULT_TABLE,
    batch_size: int = 5_000,
    dry_run: bool = False,
    tickers: Optional[Sequence[str]] = None,
) -> Tuple[int, int, int]:
    """
    Returns (rows_read, rows_skipped, rows_written).

    rows_read == rows_skipped + rows_written is the reconciliation invariant.
    """
    if batch_size > MAX_ROWS_PER_INSERT:
        log.warning(
            "batch-size %d exceeds the %d-row limit imposed by PostgreSQL's "
            "32767 bind-parameter cap — clamping to %d.",
            batch_size, MAX_ROWS_PER_INSERT, MAX_ROWS_PER_INSERT,
        )
        batch_size = MAX_ROWS_PER_INSERT

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    time_column = detect_time_column(conn, table)
    log.info("Source: %s.%s (time column: %s)", sqlite_path, table, time_column)

    # --- resolve the ticker universe up front ------------------------------
    ticker_sql = f'SELECT DISTINCT Ticker FROM "{table}"'
    params: tuple = ()
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        ticker_sql += f" WHERE Ticker IN ({placeholders})"
        params = tuple(tickers)
    all_tickers = sorted(r[0] for r in conn.execute(ticker_sql, params) if r[0])

    class_counts = Counter(classify_asset_class(t) for t in all_tickers)
    log.info(
        "Found %d distinct tickers (%s)",
        len(all_tickers),
        ", ".join(f"{n} {k}" for k, n in sorted(class_counts.items())),
    )

    ticker_meta = load_ticker_metadata(conn)

    if dry_run:
        total = skipped = 0
        for row in iter_rows(conn, table, time_column, tickers):
            total += 1
            if is_empty_bar(row):
                skipped += 1
        conn.close()
        log.info(
            "DRY RUN — would write %d rows, skip %d empty bars, of %d read. "
            "Nothing was written to TimescaleDB.",
            total - skipped, skipped, total,
        )
        return total, skipped, 0

    rows_read = rows_skipped = rows_written = 0
    malformed = 0

    async with get_session() as session:
        repo = TimescaleMarketDataRepo(session)

        # --- Phase A: upsert the ~616 assets once, cache their ids ---------
        # Reuses the repository's tested upsert (SELECT-then-INSERT with an
        # ON CONFLICT guard). Doing this once per ticker rather than once per
        # row is the difference between ~616 and ~920,000 round-trips.
        asset_ids: Dict[str, int] = {}
        for ticker in all_tickers:
            asset = Asset(
                symbol=ticker,
                asset_class=classify_asset_class(ticker),
                source=SOURCE,
                metadata=ticker_meta.get(ticker, {}),
            )
            asset_ids[ticker] = await repo._get_or_create_asset(asset)
        await session.commit()
        log.info("Asset registry ready: %d assets resolved.", len(asset_ids))

        # --- Phase B: stream OHLCV rows in batched multi-row INSERTs -------
        batch: List[dict] = []

        async def flush() -> int:
            if not batch:
                return 0
            stmt = (
                pg_insert(MarketDataORM)
                .values(batch)
                .on_conflict_do_nothing(index_elements=["time", "asset_id"])
            )
            await session.execute(stmt)
            await session.commit()
            written = len(batch)
            batch.clear()
            return written

        for row in iter_rows(conn, table, time_column, tickers):
            rows_read += 1

            if is_empty_bar(row):
                rows_skipped += 1
                continue

            try:
                batch.append(
                    {
                        "time": parse_timestamp(row[time_column]),
                        "asset_id": asset_ids[row["Ticker"]],
                        "open": row["Open"],
                        "high": row["High"],
                        "low": row["Low"],
                        "close": row["Close"],
                        "volume": row["Volume"],
                        "source": SOURCE,
                    }
                )
            except (KeyError, ValueError, TypeError) as exc:
                malformed += 1
                rows_skipped += 1
                if malformed <= 10:
                    log.warning("Skipping malformed row %s: %s", dict(row), exc)
                continue

            if len(batch) >= batch_size:
                rows_written += await flush()
                log.info(
                    "Progress: %d written / %d read (%d skipped)",
                    rows_written, rows_read, rows_skipped,
                )

        rows_written += await flush()

    conn.close()

    if malformed:
        log.warning("%d malformed rows skipped in total.", malformed)

    log.info(
        "Migration complete — read %d, skipped %d (empty bars + malformed), written %d.",
        rows_read, rows_skipped, rows_written,
    )
    if rows_read != rows_skipped + rows_written:
        log.error(
            "RECONCILIATION MISMATCH: read(%d) != skipped(%d) + written(%d)",
            rows_read, rows_skipped, rows_written,
        )
    return rows_read, rows_skipped, rows_written


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate legacy SQLite OHLCV rows into TimescaleDB."
    )
    parser.add_argument(
        "--sqlite-path",
        default=DEFAULT_SQLITE_PATH,
        help=f"Path to the legacy SQLite database (default: {DEFAULT_SQLITE_PATH})",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=(
            f"Source table (default: {DEFAULT_TABLE}). The legacy `price_data` "
            "table is a superseded subset and is intentionally not migrated."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MAX_ROWS_PER_INSERT,
        help=(
            f"Rows per multi-row INSERT (default and maximum: {MAX_ROWS_PER_INSERT}). "
            "Values above the maximum are clamped — see MAX_BIND_PARAMS."
        ),
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Restrict the migration to these tickers — use for smoke tests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count and classify without writing to TimescaleDB",
    )
    args = parser.parse_args()

    if not Path(args.sqlite_path).exists():
        log.error("SQLite file not found: %s", args.sqlite_path)
        sys.exit(1)

    asyncio.run(
        migrate(
            args.sqlite_path,
            table=args.table,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            tickers=args.tickers,
        )
    )


if __name__ == "__main__":
    main()
