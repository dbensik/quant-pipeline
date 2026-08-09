"""
cli/run_pipeline.py

Fetch price bars into TimescaleDB from the command line.

REWRITTEN 2026-08-09. This used to build a `PipelineOrchestrator` over a
`sqlite3.Connection` and write `quant_pipeline.db` — a database nothing has
read since the Phase 2 migration. It reported success either way, which is how
the Streamlit ingest button left TimescaleDB thirteen months stale while
looking healthy. Same defect, same shape: a working code path feeding a dead
store.

It now drives `core.ingest`, the same code the API's `POST /api/v1/ingest`
uses, so the CLI and the API cannot diverge.

    python -m cli.run_pipeline                     # resume every registered symbol
    python -m cli.run_pipeline --symbols AAPL MSFT # just these
    python -m cli.run_pipeline --full-backfill     # restate history (see below)
    python -m cli.run_pipeline --dry-run           # report the plan, write nothing

WHEN TO USE --full-backfill
    yfinance's auto_adjust restates a whole series for splits as of the fetch
    date, so a symbol that splits after its bars were stored ends up with two
    segments adjusted to different as-of dates — a discontinuity every
    strategy reads as a real move. A full backfill overwrites the stored bars
    and removes it. `GET /api/v1/ingest/health` says which symbols need one.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.ingest import ingest_symbols  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def _registered_symbols(session) -> list[str]:
    from sqlalchemy import select

    from db.models import AssetORM

    result = await session.execute(select(AssetORM.symbol).order_by(AssetORM.symbol))
    return list(result.scalars().all())


async def run(args: argparse.Namespace) -> int:
    async with get_session() as session:
        symbols = args.symbols or await _registered_symbols(session)
        symbols = [s.upper().strip() for s in symbols if s and s.strip()]

        if not symbols:
            logger.error("No symbols to ingest — the asset registry is empty.")
            return 1

        mode = "FULL BACKFILL (restating history)" if args.full_backfill else "resume"
        logger.info("%d symbol(s), mode: %s", len(symbols), mode)

        if args.dry_run:
            logger.info("Dry run — writing nothing. Would ingest: %s",
                        ", ".join(symbols[:20]) + ("…" if len(symbols) > 20 else ""))
            return 0

        repo = TimescaleMarketDataRepo(session)

        def progress(done: int, total: int, symbol: str) -> None:
            # One line per symbol rather than a bar: this output is usually
            # redirected to a log.
            logger.info("[%d/%d] %s", done, total, symbol)

        report = await ingest_symbols(
            repo=repo,
            symbols=symbols,
            full_backfill=args.full_backfill,
            progress=progress,
        )

    logger.info(
        "Done: %d row(s) persisted across %d symbol(s).",
        report.written,
        len(report.symbols),
    )
    if report.delisted:
        logger.warning(
            "Marked as no longer trading: %s", ", ".join(report.delisted)
        )
    if report.failed:
        logger.error("Failed: %s", ", ".join(report.failed))
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch price bars into TimescaleDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--symbols", nargs="*", help="Tickers to fetch. Omit for the whole registry."
    )
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Refetch from 2015 and OVERWRITE stored bars (fixes split drift).",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report the plan; write nothing."
    )
    args = parser.parse_args()

    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return 130
    except Exception:
        logger.exception("Ingest failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
