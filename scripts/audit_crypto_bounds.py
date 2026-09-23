"""
scripts/audit_crypto_bounds.py

Check every stored crypto series against the range its coin has actually
traded in.

    python scripts/audit_crypto_bounds.py                 # report
    python scripts/audit_crypto_bounds.py --symbols TIA-USD
    python scripts/audit_crypto_bounds.py --write         # record the findings

WHY BOUNDS AND NOT A DAY-OVER-DAY THRESHOLD
    Six rounds of cleanup in September 2026 were all decided by this one test,
    and nothing else came close. A multiplier on consecutive closes cannot
    separate AAVE-USD's 102.9x day (CORRECT — the 100:1 LEND->AAVE
    redenomination) from USDE-USD's 2,121x (fabricated — a dollar-pegged
    stablecoin). The question is not "is this move large" but "has this asset
    ever been worth that", and only bounds answer it.

    Bounds also catch what a jump test structurally cannot see: a wholly wrong
    series with no jump in it at all, sitting at a plausible-looking level.
    That is exactly what the seventeen wrong assets were.

WHAT --write DOES, AND DOES NOT
    Records the finding on the asset: `bounds_status`, `bounds_checked_at`,
    and for a TRIM the suggested cut date as `bounds_suggested_valid_from`.

    A SUGGESTION, deliberately not the enforced floor. `history_valid_from` is
    what ingestion obeys, and setting it means deleting bars — a destructive
    act that stays a human decision. This script never deletes anything and
    never changes what ingestion will fetch.

    Bars before an existing `history_valid_from` are excluded from the check,
    because those were already deliberately removed; judging a series on
    history someone has explicitly disowned would re-raise closed findings.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select, update  # noqa: E402

from core.crypto_identity import META_COINGECKO_ID  # noqa: E402
from core.ingest import history_floor  # noqa: E402
from core.price_bounds import Bounds, check_bounds  # noqa: E402
from db.models import AssetORM, MarketDataORM  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("audit-crypto-bounds")

META_BOUNDS_STATUS = "bounds_status"
META_BOUNDS_CHECKED_AT = "bounds_checked_at"
META_BOUNDS_SUGGESTED_FROM = "bounds_suggested_valid_from"


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="*", help="check only these")
    parser.add_argument("--write", action="store_true",
                        help="record findings (never deletes, never trims)")
    args = parser.parse_args(argv)

    from data_pipeline.dynamic_universe import DynamicUniverse

    async with get_session() as session:
        query = (select(AssetORM.id, AssetORM.symbol, AssetORM.metadata_)
                 .where(AssetORM.asset_class == "crypto")
                 .order_by(AssetORM.symbol))
        if args.symbols:
            query = query.where(AssetORM.symbol.in_([s.upper() for s in args.symbols]))
        assets = (await session.execute(query)).all()

        by_id = {}
        for asset_id, symbol, meta in assets:
            coin_id = (meta or {}).get(META_COINGECKO_ID)
            if coin_id:
                by_id.setdefault(coin_id, []).append((asset_id, symbol, meta or {}))

        no_reference = [s for _, s, m in assets
                        if not (m or {}).get(META_COINGECKO_ID)]
        if not by_id:
            logger.error("No assets carry a %s — run audit_crypto_identity.py "
                         "first.", META_COINGECKO_ID)
            return 1

        bounds = {b.coingecko_id: b
                  for b in DynamicUniverse().fetch_crypto_bounds_by_id(by_id)}
        logger.info("Bounds fetched for %d of %d coin ids.", len(bounds), len(by_id))

        reports, unpriced = [], []
        for coin_id, rows in by_id.items():
            bound = bounds.get(coin_id)
            for asset_id, symbol, meta in rows:
                if bound is None:
                    unpriced.append(symbol)
                    continue
                floor = history_floor(meta)
                # low and high as well as close: a bar that STRADDLES a
                # redenomination has an in-range close and an absurd intraday
                # range, and a close-only check proposes cutting at that very
                # bar. See AAVE-USD 2020-10-03.
                stmt = (select(MarketDataORM.time, MarketDataORM.close,
                               MarketDataORM.low, MarketDataORM.high)
                        .where(MarketDataORM.asset_id == asset_id))
                if floor is not None:
                    stmt = stmt.where(MarketDataORM.time >= floor)
                bars = [(t.date(), float(c), float(lo or 0), float(hi or 0))
                        for t, c, lo, hi in (await session.execute(stmt)).all() if c]
                reports.append(
                    (asset_id, meta, check_bounds(symbol, bars, bound))
                )

        # An empty series is vacuously inside any range. Reporting the 19
        # cleared wrong assets as "clean" alongside genuinely verified ones
        # would be true and useless — they are clean because there is nothing
        # left to be wrong.
        empty = [r for _, _, r in reports if r.total == 0]
        checked = [r for _, _, r in reports if r.total > 0]

        order = (Bounds.UNUSABLE, Bounds.SCATTERED, Bounds.TRIM, Bounds.CLEAN)
        print()
        for verdict in order:
            group = [r for r in checked if r.verdict is verdict]
            print(f"=== {verdict.value.upper()} ({len(group)}) ===")
            if verdict is Bounds.CLEAN:
                print("  " + ", ".join(r.symbol for r in group) + "\n")
                continue
            for r in sorted(group, key=lambda r: -r.pct_outside):
                print("  " + r.describe())
            print()
        if empty:
            print(f"NO BARS — nothing to check ({len(empty)}): "
                  + ", ".join(r.symbol for r in empty) + "\n")
        if unpriced:
            print(f"NO BOUNDS PUBLISHED ({len(unpriced)}): {', '.join(unpriced)}")
        if no_reference:
            print(f"NO COINGECKO ID ({len(no_reference)}): {', '.join(no_reference)}")

        if not args.write:
            print("\nNothing written. Re-run with --write to record the findings.")
            return 0

        now = datetime.now(timezone.utc).isoformat()
        for asset_id, meta, report in reports:
            patch = dict(meta)
            if report.total == 0:
                # Do not record a verdict we did not earn.
                continue
            patch[META_BOUNDS_STATUS] = report.verdict.value
            patch[META_BOUNDS_CHECKED_AT] = now
            if report.verdict is Bounds.TRIM and report.valid_from:
                patch[META_BOUNDS_SUGGESTED_FROM] = report.valid_from.isoformat()
            else:
                patch.pop(META_BOUNDS_SUGGESTED_FROM, None)
            await session.execute(
                update(AssetORM).where(AssetORM.id == asset_id).values(metadata_=patch)
            )
        await session.commit()
        print(f"\nRecorded bounds findings on {len(reports)} asset rows. "
              "No bars were deleted and no floors were set.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
