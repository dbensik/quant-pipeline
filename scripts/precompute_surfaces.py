"""
scripts/precompute_surfaces.py

Price every complete capture in the option-chain archive into the surface
cache, so GET /api/v1/options/surface answers from disk. Idempotent: a
ticker-day whose cached key still matches is skipped in milliseconds.

    python scripts/precompute_surfaces.py                 # whole archive
    python scripts/precompute_surfaces.py --tickers SPY   # one ticker
    python scripts/precompute_surfaces.py --since 2026-10-01
    python scripts/precompute_surfaces.py --days 7        # 06:00 job

A surface costs 10-35 s; the archive (108 captures on 2026-10-08) about 32
minutes cold, so the 06:00 job (since 2026-10-13) covers only the last
`--days` days: about 1-3 minutes for a new day's 6 captures, and a pricing
change (a new fingerprint) recomputes a week, not the archive. Older days
are still computed on request.

`--days` also names MISSED CAPTURE DAYS: a session SPY traded in the window
(stored bars, before today) with no complete capture for a ticker. On
2026-10-09 both capture runs crashed and that day was lost for all six
tickers, unnoticed for four days; yfinance serves only today's chain, so a
missed day is permanent and the point of naming it is to fix the cause before
the next one. Clear a day that is known lost with
settings.OPTION_CAPTURE_DAYS_LOST.

Exit 0 when clean, 1 when only missed capture days were found (a flag), 2 if
any capture failed to price or the run itself failed.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select  # noqa: E402

from config import settings  # noqa: E402
from core.options_surface import (  # noqa: E402
    ChainArchive,
    NoRateError,
    SurfaceCache,
    compute_for,
    pricing_fingerprint,
    surface_inputs,
)
from db.models import AssetORM, MarketDataORM  # noqa: E402
from db.repositories.dividends import TimescaleDividendRepo  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("precompute_surfaces")


def exit_code(failed: int, missed: int) -> int:
    """2: a surface failed to price (a failure); 1: only missed capture days (a flag); 0: clean."""
    return 2 if failed else 1 if missed else 0


def missed_days(sessions, captures, tickers, lost) -> list:
    """(ticker, session) pairs with no complete capture, known-lost days excluded."""
    have = {(c.ticker, c.day) for c in captures if not c.partial}
    return [
        (t, d)
        for d in sorted(sessions)
        if d.isoformat() not in lost
        for t in tickers
        if (t, d) not in have
    ]


async def sessions_between(session, start: date, end: date) -> list:
    """Days SPY has a stored bar, start <= day < end: the trading sessions."""
    day = func.date(MarketDataORM.time)
    rows = await session.execute(
        select(day)
        .join(AssetORM, AssetORM.id == MarketDataORM.asset_id)
        .where(AssetORM.symbol == "SPY", day >= start, day < end)
        .distinct()
    )
    return [r[0] for r in rows.all()]


async def run(args: argparse.Namespace) -> int:
    if args.days is not None:
        args.since = date.today() - timedelta(days=args.days)
    archive = ChainArchive(Path(settings.OPTION_CHAIN_ARCHIVE_DIR))
    cache = SurfaceCache(Path(settings.OPTIONS_SURFACE_CACHE_DIR))
    code = pricing_fingerprint()
    captures = [
        c
        for c in archive.captures()
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
            logger.info(
                "[%d/%d] %s %s %s %.1fs",
                i,
                len(captures),
                cap.ticker,
                cap.day,
                "cached" if hit else "computed",
                time.perf_counter() - started,
            )
    logger.info(
        "%d captures: %d computed, %d already cached, %d failed",
        len(captures),
        computed,
        hits,
        failed,
    )
    missed = []
    if args.days is not None:
        async with AsyncSessionLocal() as session:
            sessions = await sessions_between(session, args.since, date.today())
        missed = missed_days(
            sessions,
            archive.captures(),
            args.tickers or settings.OPTION_CAPTURE_TICKERS,
            settings.OPTION_CAPTURE_DAYS_LOST,
        )
        for ticker, day in missed:
            print(f"MISSED CAPTURE {ticker} {day}: a trading session with no complete chain")
        logger.info(
            "%d sessions since %s; %d ticker-days missed", len(sessions), args.since, len(missed)
        )
    return exit_code(failed, len(missed))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--tickers", nargs="+", type=str.upper)
    parser.add_argument("--since", type=date.fromisoformat)
    parser.add_argument("--days", type=int, help="the last N days, and name missed capture days")
    try:
        return asyncio.run(run(parser.parse_args()))
    except Exception:  # noqa: BLE001 - must read as "did not run", not as a flag
        logger.exception("surface precompute did not complete")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
