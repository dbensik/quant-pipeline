"""
scripts/precompute_surfaces.py

Price every complete capture in the option-chain archive into the surface
cache, so GET /api/v1/options/surface answers from disk. Idempotent: a
ticker-day whose cached key still matches is skipped in milliseconds.

    python scripts/precompute_surfaces.py                 # whole archive
    python scripts/precompute_surfaces.py --tickers SPY   # one ticker
    python scripts/precompute_surfaces.py --since 2026-10-01

A surface costs 10-35 s; the archive (108 captures on 2026-10-08) about 32
minutes cold. Not part of the 06:00 job: the API computes a missing surface
on request. Exit 1 if any capture failed to price.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings  # noqa: E402
from core.options_surface import ChainArchive, NoRateError, SurfaceCache, compute_for, pricing_fingerprint, surface_inputs  # noqa: E402
from db.repositories.dividends import TimescaleDividendRepo  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("precompute_surfaces")


async def run(args: argparse.Namespace) -> int:
    archive = ChainArchive(Path(settings.OPTION_CHAIN_ARCHIVE_DIR))
    cache = SurfaceCache(Path(settings.OPTIONS_SURFACE_CACHE_DIR))
    code = pricing_fingerprint()
    captures = [
        c for c in archive.captures()
        if not c.partial
        and (not args.tickers or c.ticker in args.tickers)
        and (args.since is None or c.day >= args.since)
    ]
    failed = computed = hits = 0
    async with AsyncSessionLocal() as session:
        rates, divs = TimescaleRateRepo(session), TimescaleDividendRepo(session)
        for i, cap in enumerate(captures, 1):
            started = time.perf_counter()
            try:
                inputs = await surface_inputs(cap, rates, divs, code)
                _, hit = await cache.get_or_compute(inputs, compute_for(inputs), asyncio.to_thread)
            except NoRateError as exc:
                failed += 1
                logger.error("[%d/%d] %s %s: %s", i, len(captures), cap.ticker, cap.day, exc)
                continue
            except Exception:
                failed += 1
                logger.exception("[%d/%d] %s %s failed", i, len(captures), cap.ticker, cap.day)
                continue
            hits += hit
            computed += not hit
            logger.info("[%d/%d] %s %s %s %.1fs", i, len(captures), cap.ticker, cap.day,
                        "cached" if hit else "computed", time.perf_counter() - started)
    logger.info("%d captures: %d computed, %d already cached, %d failed", len(captures), computed, hits, failed)
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tickers", nargs="+", type=str.upper)
    parser.add_argument("--since", type=date.fromisoformat)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
