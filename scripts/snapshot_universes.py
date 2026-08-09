"""
scripts/snapshot_universes.py

Record what each index contains today, for point-in-time membership.

    python scripts/snapshot_universes.py                    # every working index
    python scripts/snapshot_universes.py --indexes sp500    # just these
    python scripts/snapshot_universes.py --dry-run          # fetch, write nothing

WHY THIS RUNS DAILY
    `universe_membership` can only record what it OBSERVES; nothing can
    reconstruct index membership retroactively. A screen over a past window is
    refused unless a snapshot predates it, so the feature's usefulness is
    entirely a function of how long this has been running. Missing a day
    leaves a gap that cannot be filled later.

WHY IT DOES NOT GO THROUGH THE API
    A scheduled job that needs uvicorn running is a job that silently stops
    when uvicorn is not. This talks to the database directly, so it works
    whether or not anything else is up.

Failures are per index: one broken source does not stop the others. The exit
code is non-zero only when EVERY requested index failed, which is the case
that means something systemic is wrong rather than one page having moved.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.repositories.universe import TimescaleUniverseRepo  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("snapshot-universes")

#: nasdaq100 is deliberately absent. Its Wikipedia page no longer carries a
#: constituents table at all (checked 2026-08-09), so including it would mean a
#: guaranteed daily failure — which trains everyone to ignore the log.
DEFAULT_INDEXES = ("sp500", "dow_jones", "top_100_crypto")


async def snapshot(indexes: list[str], dry_run: bool) -> int:
    from data_pipeline.dynamic_universe import DynamicUniverse

    fetcher = DynamicUniverse()
    succeeded, failed = 0, []

    async with get_session() as session:
        repo = TimescaleUniverseRepo(session)

        for index_name in indexes:
            try:
                symbols = await asyncio.to_thread(fetcher.get_tickers, index_name)
            except Exception as exc:  # noqa: BLE001
                logger.error("%s: fetch raised: %s", index_name, exc)
                failed.append(index_name)
                continue

            if not symbols:
                # DynamicUniverse returns [] for a failed scrape as well as for
                # an empty index. Recording it would write a snapshot claiming
                # the index emptied, which would be worse than recording
                # nothing.
                logger.error(
                    "%s: constituent list came back empty — refusing to record "
                    "a snapshot that would claim the index is empty.",
                    index_name,
                )
                failed.append(index_name)
                continue

            if dry_run:
                logger.info("%s: %d members (dry run)", index_name, len(symbols))
                succeeded += 1
                continue

            result = await repo.record_snapshot(index_name, symbols)
            succeeded += 1
            logger.info(
                "%s: %d members (+%d added, -%d removed)",
                index_name,
                result.member_count,
                len(result.added),
                len(result.removed),
            )
            # Changes are the interesting part of a daily run, and they are
            # exactly what cannot be recovered if this stops running.
            if result.added:
                logger.info("%s: joined -> %s", index_name, ", ".join(result.added))
            if result.removed:
                logger.info("%s: left   -> %s", index_name, ", ".join(result.removed))

    if failed:
        logger.warning("Failed: %s", ", ".join(failed))
    logger.info("Snapshotted %d of %d index(es).", succeeded, len(indexes))

    # Non-zero only when nothing worked; one moved page should not page anyone.
    return 0 if succeeded else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--indexes", nargs="*", default=list(DEFAULT_INDEXES),
        help=f"Default: {' '.join(DEFAULT_INDEXES)}",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        return asyncio.run(snapshot(args.indexes, args.dry_run))
    except KeyboardInterrupt:
        return 130
    except Exception:
        logger.exception("Snapshot run failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
