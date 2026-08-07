"""
scripts/migrate_sqlite_to_timescale.py

One-time migration: reads all rows from an existing SQLite market_data table
and writes them into TimescaleDB via TimescaleMarketDataRepo.

Run ONCE after `alembic upgrade head` has created the schema:

    python scripts/migrate_sqlite_to_timescale.py --sqlite-path your_existing.db

Safety guarantees
-----------------
* Idempotent — repo.write() uses ON CONFLICT DO NOTHING, so re-running is safe.
* Batch mode — rows are written in configurable chunks to bound memory usage.
* Dry-run flag — pass --dry-run to count rows without writing anything.

Phase 2 — TimescaleDB Schema & Repository Layer
"""

import argparse
import asyncio
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List

# Ensure the project root is on sys.path when running as a script
_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from core.models import Asset, OHLCV, MarketDataRecord, Timestamp
from db.repositories.market_data import TimescaleMarketDataRepo
from db.session import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLite row → domain object
# ---------------------------------------------------------------------------

def sqlite_row_to_record(row: sqlite3.Row) -> MarketDataRecord:
    """
    Convert a sqlite3.Row from the legacy market_data table into a canonical
    MarketDataRecord.

    Expected SQLite columns (adjust column names below if your schema differs):
        time / timestamp, symbol, asset_class, source, open, high, low, close, volume
    """
    # Tolerate both 'time' and 'timestamp' column names
    raw_time = row["time"] if "time" in row.keys() else row["timestamp"]

    # Parse timestamp — accept ISO strings or Unix epoch integers/floats
    if isinstance(raw_time, (int, float)):
        ts = datetime.fromtimestamp(raw_time, tz=timezone.utc)
    elif isinstance(raw_time, str):
        ts = datetime.fromisoformat(raw_time).replace(tzinfo=timezone.utc)
    else:
        ts = raw_time  # already a datetime

    asset_class = row["asset_class"] if "asset_class" in row.keys() else "equity"
    source = row["source"] if "source" in row.keys() else "yfinance"

    return MarketDataRecord(
        asset=Asset(
            symbol=row["symbol"],
            asset_class=asset_class,
            source=source,
        ),
        ohlcv=OHLCV(
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
            timestamp=Timestamp(utc=ts),
        ),
    )


# ---------------------------------------------------------------------------
# Migration logic
# ---------------------------------------------------------------------------

async def migrate(sqlite_path: str, batch_size: int = 500, dry_run: bool = False) -> None:
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM market_data").fetchall()
    conn.close()

    total = len(rows)
    log.info("Found %d rows in SQLite %s", total, sqlite_path)

    if dry_run:
        log.info("Dry run — no data written to TimescaleDB.")
        return

    written = 0
    async with get_session() as session:
        repo = TimescaleMarketDataRepo(session)

        for batch_start in range(0, total, batch_size):
            batch_rows = rows[batch_start : batch_start + batch_size]
            records: List[MarketDataRecord] = []

            for row in batch_rows:
                try:
                    records.append(sqlite_row_to_record(row))
                except Exception as exc:
                    log.warning("Skipping malformed row %s: %s", dict(row), exc)

            if records:
                await repo.write(records)
                written += len(records)
                log.info(
                    "Progress: %d / %d rows written (batch %d–%d)",
                    written,
                    total,
                    batch_start + 1,
                    batch_start + len(batch_rows),
                )

    log.info("Migration complete. %d rows written to TimescaleDB.", written)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate SQLite market_data rows into TimescaleDB."
    )
    parser.add_argument(
        "--sqlite-path",
        required=True,
        help="Path to the existing SQLite database file (e.g. your_existing.db)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows per write batch (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows without writing to TimescaleDB",
    )
    args = parser.parse_args()

    if not Path(args.sqlite_path).exists():
        log.error("SQLite file not found: %s", args.sqlite_path)
        sys.exit(1)

    asyncio.run(migrate(args.sqlite_path, args.batch_size, args.dry_run))


if __name__ == "__main__":
    main()
