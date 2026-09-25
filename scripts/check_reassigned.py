"""
scripts/check_reassigned.py

Flag stored series that stop looking like the instrument they started as.

    python scripts/check_reassigned.py                  # every equity and ETF
    python scripts/check_reassigned.py --symbols PARA AMCR
    python scripts/check_reassigned.py --include-crypto

Exit 0 when nothing is flagged, 1 when anything is, 2 if the check itself
failed — so a database error is never logged as a flag. Run by the 06:00 job
after ingest, so a reassigned ticker interrupts someone instead of waiting to
be found.

WHAT PROBLEM THIS SOLVES
    On 2026-08-07 Yahoo's PARA key began serving an unrelated penny stock, and
    the daily job appended 32 of its bars to Paramount Global's history. It was
    found only because someone ran scripts/find_successors.py by hand. Every
    identity guard here keys off coingecko_id, so the 516 equities and 11 ETFs
    had none. This is that guard: see `detect_reassignment` for why the test
    is a collapse in dollar volume rather than a hole in the dates.

WHAT IT DELIBERATELY DOES NOT DO
    Write anything. Deciding a series belongs to another company, and what to
    do about the bars, is a human call. The PARA repair shows why: the obvious
    fix, a full backfill, destroyed 1390 genuine bars.

    Crypto is excluded by default. Crypto has its own guard in
    core/crypto_identity.py, and its volumes are too unstable for this test —
    wrapped and staked tokens (DAI, WSTETH, STETH) routinely move 50-200x.

CLEARING A FALSE POSITIVE
    After a human has looked at a flagged break and judged it genuine, add its
    date to a list on the asset row so the job stops failing on it:

        UPDATE assets SET metadata = coalesce(metadata, '{}')
            || '{"reassignment_cleared": ["2026-08-07"]}' WHERE symbol = 'XYZ';

    (That replaces the list; include any dates already in it.) Keyed to the
    DATE of the break, and the detector keeps scanning past a cleared one, so
    any other break in the same series still fires.

    Clear only once the break has 20 sessions behind it. Until then the
    reported date can still move by a bar as new sessions arrive, and a cleared
    date that no longer matches will fire again.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import traceback
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from config.settings import (  # noqa: E402
    REASSIGNMENT_MIN_COLLAPSE,
    REASSIGNMENT_MIN_PRIOR_DOLLAR_VOLUME,
    REASSIGNMENT_WINDOW_BARS,
)
from core.corporate_actions import detect_reassignment  # noqa: E402
from db.models import AssetORM, MarketDataORM  # noqa: E402
from db.session import get_session  # noqa: E402

#: Asset metadata key holding the ISO date of a break a human has cleared.
META_REASSIGNMENT_CLEARED = "reassignment_cleared"


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--symbols", nargs="+", help="Check only these tickers")
    parser.add_argument("--include-crypto", action="store_true")
    args = parser.parse_args(argv)

    flagged = 0
    async with get_session() as session:
        query = select(AssetORM.id, AssetORM.symbol, AssetORM.metadata_).order_by(
            AssetORM.symbol
        )
        if args.symbols:
            query = query.where(AssetORM.symbol.in_([s.upper() for s in args.symbols]))
        elif not args.include_crypto:
            query = query.where(AssetORM.asset_class != "crypto")
        assets = (await session.execute(query)).all()

        for asset_id, symbol, meta in assets:
            rows = (
                await session.execute(
                    select(
                        MarketDataORM.time, MarketDataORM.close, MarketDataORM.volume
                    )
                    .where(MarketDataORM.asset_id == asset_id)
                    .order_by(MarketDataORM.time)
                )
            ).all()
            found = detect_reassignment(
                [(t.date(), c, v) for t, c, v in rows],
                window=REASSIGNMENT_WINDOW_BARS,
                min_collapse=REASSIGNMENT_MIN_COLLAPSE,
                min_prior_dollar_volume=REASSIGNMENT_MIN_PRIOR_DOLLAR_VOLUME,
                cleared=frozenset(
                    date.fromisoformat(d)
                    for d in (meta or {}).get(META_REASSIGNMENT_CLEARED, [])
                ),
            )
            if found is None:
                continue
            flagged += 1
            print(f"SUSPECT {symbol}: {found.describe()}")

    print(f"Checked {len(assets)} asset(s); {flagged} suspected reassignment(s).")
    return 1 if flagged else 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception:  # noqa: BLE001 - any failure must read as "did not run"
        traceback.print_exc()
        print("CHECK FAILED: the reassignment check did not complete.")
        sys.exit(2)
