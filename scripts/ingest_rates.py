"""
scripts/ingest_rates.py

Fetch the risk-free rate series (^IRX) into `rate_observations`. Insert-only.

    python scripts/ingest_rates.py                    # routine: newest - 14 days
    python scripts/ingest_rates.py --start 2015-01-02 # fill from a date
    python scripts/ingest_rates.py --as-of 2026-10-12 # print the rate a reader gets

Exit 0 when the run wrote or confirmed observations, 1 when the provider
returned nothing or the write failed. Part of the 06:00 job; see core/rates.py
for the conventions and research/option-pricing-plan-2026-10-08.md for why.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import RISK_FREE_RATE_SERIES  # noqa: E402
from core.rates import RateIngestError, ingest_rates, risk_free_rate  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ingest_rates")


async def run(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as session:
        repo = TimescaleRateRepo(session)
        if args.as_of:
            rate = await risk_free_rate(repo, args.as_of, args.series)
            if rate is None:
                logger.error("%s: nothing stored on or before %s", args.series, args.as_of)
                return 1
            logger.info(
                "%s as of %s: observation %s, quoted %.3f%% (discount), "
                "continuous %.4f%%, bond-equivalent %.4f%%, %d day(s) old%s",
                rate.series, rate.as_of, rate.obs_date, rate.quoted_pct,
                rate.continuous * 100, rate.bond_equivalent * 100, rate.age_days,
                " — STALE" if rate.stale else "",
            )
            return 0
        try:
            report = await ingest_rates(repo, args.series, start=args.start)
        except RateIngestError as exc:
            logger.error("%s", exc)
            return 1
    logger.info(
        "%s: requested %s..%s (exclusive), %d observation(s) served, %d new; newest stored %s",
        report.series, report.start, report.end_exclusive, report.fetched,
        report.inserted, report.newest,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--series", default=RISK_FREE_RATE_SERIES)
    parser.add_argument("--start", type=date.fromisoformat, help="Fetch from this date; insert-only")
    parser.add_argument("--as-of", type=date.fromisoformat, help="Print the rate for this date; writes nothing")
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except Exception:
        logger.exception("Rate ingest failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
