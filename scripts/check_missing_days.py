"""
scripts/check_missing_days.py

Report trading sessions missing from INSIDE stored equity and ETF series.

    python scripts/check_missing_days.py
    python scripts/check_missing_days.py --symbols EIX DXCM

Exit 0 when nothing needs a human, 1 when a hole is older than the ingest
overlap (so ingest will never fill it on its own), 2 if the check itself
failed. Run by the 06:00 job after ingest.

WHAT PROBLEM THIS SOLVES
    Until 2026-09-25 incremental ingest resumed the day after each symbol's
    newest bar, so a day lost while a later one landed was never requested
    again — and nothing looked. 463 of 527 equities lost 2026-08-28; it was
    found a month later because EIX's -26.7% "day" was two days merged into
    one. Ingest now re-requests the last INGEST_OVERLAP_DAYS, which heals a
    recent hole by itself. This reports the holes it cannot reach.

HOW
    The trading calendar is stored SPY plus a fresh Yahoo SPY fetch for recent
    weeks. SPY alone would be blind to a day SPY itself lost, and a day lost
    everywhere is exactly the failure being looked for. Holes younger than the
    overlap are listed as PENDING and do not fail the job: the next run
    retries them.

    Crypto is excluded: it trades every day and SPY is no calendar for it.
    Assets flagged delisted are excluded: EA's July gap is a deliberate stub
    deletion, not a lost day.

WHAT IT DELIBERATELY DOES NOT DO
    Write anything. Filling a hole is an insert of fetched bars:
    `python -m cli.run_pipeline --symbols X --start YYYY-MM-DD`, or the
    overlap on its own for a recent one.

ACCEPTING A HOLE
    If the provider no longer serves a missing day at all, record it so the
    job stops failing on it:

        UPDATE assets SET metadata = coalesce(metadata, '{}')
            || '{"missing_days_accepted": ["2025-11-12"]}' WHERE symbol = 'FISV';

    (That replaces the list; include any dates already in it.)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from config.settings import INGEST_OVERLAP_DAYS  # noqa: E402
from core.corporate_actions import missing_sessions  # noqa: E402
from core.ingest import is_empty_bar  # noqa: E402
from db.models import AssetORM, MarketDataORM  # noqa: E402
from db.session import get_session  # noqa: E402

#: Asset metadata key: ISO dates a human confirmed the provider cannot supply.
META_MISSING_DAYS_ACCEPTED = "missing_days_accepted"

#: The calendar symbol. SPY has traded every NYSE session since 1993.
CALENDAR_SYMBOL = "SPY"

#: How far back the fresh Yahoo calendar reaches. Older sessions come from
#: stored SPY, which was checked complete against Yahoo on 2026-09-25.
FRESH_CALENDAR_DAYS = 120

#: Symbols named per missing day before the list is truncated.
MAX_NAMED = 12


def fresh_calendar(today: date) -> tuple[set[date], str]:
    """Recent SPY sessions straight from Yahoo, or an empty set and why not."""
    from core.adapters import yfinance_adapter

    start = (today - timedelta(days=FRESH_CALENDAR_DAYS)).isoformat()
    try:
        bars = yfinance_adapter.fetch([CALENDAR_SYMBOL], start, today.isoformat())
    except Exception as exc:  # noqa: BLE001 - fall back to stored SPY, say so
        return set(), f"fresh calendar unavailable ({exc!r}); using stored SPY only"
    # is_empty_bar, not `close is not None`: the adapter builds closes with
    # float(), so an empty Yahoo row arrives as NaN. A NaN session let into the
    # calendar would report every symbol MISSING on that day.
    days = {b.ohlcv.timestamp.utc.date() for b in bars if not is_empty_bar(b.ohlcv)}
    if not days:
        return set(), "fresh calendar came back empty; using stored SPY only"
    return days, f"fresh calendar: {len(days)} SPY sessions from Yahoo"


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--symbols", nargs="+", help="Check only these tickers")
    args = parser.parse_args(argv)

    today = datetime.now(timezone.utc).date()
    # A hole younger than this is still inside the overlap: the next ingest
    # re-requests it, so it is pending rather than lost.
    heal_cutoff = today - timedelta(days=INGEST_OVERLAP_DAYS)

    fresh, calendar_note = fresh_calendar(today)
    print(calendar_note)

    lost: dict[date, list[str]] = defaultdict(list)
    pending: dict[date, list[str]] = defaultdict(list)
    async with get_session() as session:
        query = (
            select(AssetORM.id, AssetORM.symbol, AssetORM.metadata_)
            .where(AssetORM.asset_class != "crypto")
            .where(AssetORM.delisted_at.is_(None))
            .order_by(AssetORM.symbol)
        )
        assets = (await session.execute(query)).all()
        by_symbol = {symbol: (asset_id, meta) for asset_id, symbol, meta in assets}
        if CALENDAR_SYMBOL not in by_symbol:
            raise RuntimeError(f"{CALENDAR_SYMBOL} is not in the registry")

        async def days_for(asset_id: int) -> list[date]:
            rows = await session.execute(
                select(MarketDataORM.time)
                .where(MarketDataORM.asset_id == asset_id)
                .order_by(MarketDataORM.time)
            )
            return [t.date() for (t,) in rows]

        calendar = sorted(set(await days_for(by_symbol[CALENDAR_SYMBOL][0])) | fresh)

        targets = (
            [s.upper() for s in args.symbols] if args.symbols else list(by_symbol)
        )
        for symbol in targets:
            if symbol not in by_symbol:
                print(f"SKIP {symbol}: not a registered, undelisted equity or ETF")
                continue
            asset_id, meta = by_symbol[symbol]
            accepted = {
                date.fromisoformat(d)
                for d in (meta or {}).get(META_MISSING_DAYS_ACCEPTED, [])
            }
            for day in missing_sessions(await days_for(asset_id), calendar):
                if day in accepted:
                    continue
                (pending if day >= heal_cutoff else lost)[day].append(symbol)

    for label, holes in (("MISSING", lost), ("PENDING", pending)):
        for day in sorted(holes):
            names = holes[day]
            shown = ", ".join(names[:MAX_NAMED])
            more = f" (+{len(names) - MAX_NAMED} more)" if len(names) > MAX_NAMED else ""
            print(f"{label} {day}: {len(names)} symbol(s) — {shown}{more}")

    lost_bars = sum(len(v) for v in lost.values())
    print(
        f"Checked {len(targets)} asset(s); {lost_bars} missing bar(s) older than "
        f"the {INGEST_OVERLAP_DAYS}-day overlap, "
        f"{sum(len(v) for v in pending.values())} pending."
    )
    if lost_bars:
        print(
            "Fill with: python -m cli.run_pipeline --symbols <SYMS> --start "
            "<a day before the hole> — an insert, it rewrites nothing. If Yahoo "
            "no longer serves the day, accept it (see this script's docstring)."
        )
    return 1 if lost_bars else 0


if __name__ == "__main__":
    logging.disable(logging.WARNING)  # yfinance chatter; our own output is print
    try:
        sys.exit(asyncio.run(main()))
    except Exception:  # noqa: BLE001 - any failure must read as "did not run"
        traceback.print_exc()
        print("CHECK FAILED: the missing-day check did not complete.")
        sys.exit(2)
