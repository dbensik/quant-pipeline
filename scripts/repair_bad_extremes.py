"""
scripts/repair_bad_extremes.py

Null the high and low of bars whose CLOSE is sound but whose extremes are not.

    python scripts/repair_bad_extremes.py                  # report, all assets
    python scripts/repair_bad_extremes.py --asset-class equity
    python scripts/repair_bad_extremes.py --apply    # null them

THE DEFECT
    The series is the right coin and the close is inside its real range, but
    one extreme is a bad tick. Measured 2026-09-23: DAI showed a high of $3.67
    on a dollar-pegged stablecoin, WBTC a high of $162,188 against its own
    all-time high of $125,932, and WETH a low of exactly 0.0 on a day it
    closed at $4,241.

    Close-to-close returns are unaffected, which is why six rounds of cleanup
    never saw it, and why a close-only bounds check could not either. Anything
    reading the range is wrong on these bars.

WHY NULL RATHER THAN CLAMP
    The true extremes are unrecoverable — the provider gave garbage, and no
    arithmetic gets the real intraday range back. Clamping to the open/close
    envelope would put a plausible-looking number in the database that nobody
    could later tell from a real one.

    NULL says "unknown", which is true, and the frontend already handles it:
    `candlestickData.ts` drops bars without a complete OHLC quartet and COUNTS
    them as `droppedIncomplete`, precisely so a candle view showing fewer bars
    than the line view of the same range does not look like a rendering bug.
    481 migrated rows are already this shape.

JUDGED AGAINST THE BAR'S OWN BODY
    Not against the asset's all-time range. The first version used bounds and
    was wrong — it flagged CRO at a high 1.14x its close and ETC at 1.31x,
    ordinary intraday moves that merely grazed CoinGecko's recorded high,
    which is drawn from a different exchange set than the bar. Judging an
    extreme against its own open and close needs no reference data, so this
    covers equities and ETFs too.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select, update  # noqa: E402

from core.ingest import history_floor  # noqa: E402
from core.price_bounds import has_bad_extremes  # noqa: E402
from db.models import AssetORM, MarketDataORM  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("repair-bad-extremes")


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--asset-class", default=None,
                        help="restrict to equity / crypto / etf (default: all)")
    parser.add_argument("--apply", action="store_true",
                        help="null the extremes (the close is never touched)")
    args = parser.parse_args(argv)

    async with get_session() as session:
        query = (select(AssetORM.id, AssetORM.symbol, AssetORM.metadata_)
                 .order_by(AssetORM.symbol))
        if args.asset_class:
            query = query.where(AssetORM.asset_class == args.asset_class)
        if args.symbols:
            query = query.where(AssetORM.symbol.in_([s.upper() for s in args.symbols]))
        rows = (await session.execute(query)).all()
        # Skip anything already known to be the WRONG COIN. Nulling two
        # extremes in a series where every bar belongs to another asset is
        # meaningless work that also makes the series look tended-to.
        assets, skipped_wrong = [], []
        for i, sym, m in rows:
            if (m or {}).get("identity_status") == "wrong_asset":
                skipped_wrong.append(sym)
                continue
            assets.append((i, sym, m or {}))
        logger.info("Scanning %d assets. No reference data needed — a bad tick "
                    "is judged against its own bar.", len(assets))

        victims, total = [], 0
        for asset_id, symbol, meta in assets:
            floor = history_floor(meta)
            stmt = (select(MarketDataORM.time, MarketDataORM.close,
                           MarketDataORM.low, MarketDataORM.high,
                           MarketDataORM.open)
                    .where(MarketDataORM.asset_id == asset_id))
            if floor is not None:
                stmt = stmt.where(MarketDataORM.time >= floor)
            bad = [(t, c, lo, hi) for t, c, lo, hi, op
                   in (await session.execute(stmt)).all()
                   if has_bad_extremes(c, lo, hi, op)]
            if bad:
                victims.append((asset_id, symbol, bad))
                total += len(bad)

        print(f"\n{total} bar(s) across {len(victims)} symbol(s) have a sound "
              f"close and a corrupt extreme.\n")
        for _, symbol, bad in sorted(victims, key=lambda v: -len(v[2])):
            print(f"  {symbol:12} {len(bad):3} bar(s)")
            for t, c, lo, hi in bad[:2]:
                print(f"      {t.date()}  close={c:<14.6g} low={lo!r:<12} high={hi!r}")

        if skipped_wrong:
            holding = [s for s in skipped_wrong]
            print(f"\nSkipped {len(holding)} symbol(s) already flagged wrong_asset "
                  f"— their whole series belongs to another coin.")

        if not args.apply:
            print("\nNothing written. Re-run with --apply to null these extremes.")
            return 0

        for asset_id, symbol, bad in victims:
            for t, _, _, _ in bad:
                await session.execute(
                    update(MarketDataORM)
                    .where(MarketDataORM.asset_id == asset_id, MarketDataORM.time == t)
                    .values(high=None, low=None)
                )
        await session.commit()
        print(f"\nNulled high and low on {total} bar(s). "
              "No close was changed and no bar was deleted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
