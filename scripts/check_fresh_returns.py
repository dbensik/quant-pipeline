"""
scripts/check_fresh_returns.py

Compare the daily returns readers see against a fresh Yahoo fetch, for every
asset read through read-time adjustment. Read-only.

    python scripts/check_fresh_returns.py
    python scripts/check_fresh_returns.py --symbols MO HWM --sessions 60

Exit 0 when every recent return matches, 1 when any day differs by
TOLERANCE_PP or more, 2 if the check itself failed — including when Yahoo
returned nothing for more than MAX_NO_DATA symbols, because a check that
compared only part of the universe has not run. Run by the 06:00 job.

WHY
    Phase 6 of research/dividend-drift-plan-2026-09-27.md. Read-time
    adjustment is only as good as its list of events, and three things can
    make that list wrong without any other check noticing:

    1. An UNLISTED SPINOFF. Yahoo adjusts for it and lists nothing — HWM's
       2020 Arconic separation was found only by comparing returns like this.
       It shows as a one-day mismatch on the spinoff date.
    2. A CORRECTED DIVIDEND. corporate_actions keeps the first row of every
       event, so a dividend Yahoo later corrects is never picked up. It shows
       as a mismatch on that ex-date.
    3. THE MORNING-SPLIT RULE. core.price_adjustment.APPLIED_ON_EX_DATE is an
       unverified assumption about a split effective the morning of a fetch.
       The first real split after the cutover confirms or refutes it here.

    It also enforces the rule the cutover depends on: a served asset must have
    served values on EVERY bar. One missing value makes fetch_range fall back
    to the stored columns for every read of that symbol — silently, with the
    old drift — so any such bar is reported as UNCOVERED and fails the check.

REMEDY FOR A CORRECTED DIVIDEND (duty 2)
    corporate_actions keeps an event's first row, so the fix is a deliberate
    manual update of `value` AND `fetched_at` together — never one without the
    other, since the value only means something with the fetch time it came
    with — plus `evidence` saying where the corrected amount came from:

        UPDATE corporate_actions SET value = <yahoo's current amount>,
               fetched_at = now(), evidence = '<source, date>'
         WHERE asset_id = <id> AND ex_date = '<date>' AND kind = 'dividend';

    Re-run this check; the mismatch on that ex-date should be gone.

    The engine itself reproduced Yahoo to 1e-6 across the registry; this is
    what keeps that true as events arrive.

HOW
    fetch_range(adjust='total') — exactly what every backtest and screener
    reads — against one batch auto_adjust download, over the last `--sessions`
    trading days. With settings.SERVED_PRICES_ENABLED off, fetch_range returns
    stored prices and this compares those instead; the header line says which.

OPEN FILES
    launchd starts the job with a soft limit of 256 open files. The threaded
    download of ~516 symbols needs more, and yfinance reports the shortfall
    per symbol — "unable to open database file" from its cache, "'NoneType'
    object is not subscriptable" from a failed request — then carries on. On
    2026-09-29 and 09-30 that dropped 219 and 200 symbols, and the check
    printed "0 day(s) off" and exited 0 both mornings. Reproduced at
    `ulimit -n 256`: 152 of 516 lost; at a high limit, 0. So the script
    raises its own soft limit, and a shortfall now fails the check instead
    of shrinking it.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import resource
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402
import yfinance as yf  # noqa: E402
from sqlalchemy import select, text  # noqa: E402

from config import settings  # noqa: E402
from db.models import AssetORM  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.session import get_session  # noqa: E402

TOLERANCE_PP = 0.5
DEFAULT_SESSIONS = 30
MAX_LINES = 40
# Symbols Yahoo may return nothing for before the check counts as not run.
# A symbol Yahoo stopped serving is the reassignment and missing-day checks'
# business, so a few are tolerated; a fetch failure drops hundreds.
MAX_NO_DATA = 5
WANT_OPEN_FILES = 4096


def raise_open_file_limit(want: int = WANT_OPEN_FILES) -> int:
    """Raise the soft open-file limit toward `want`, capped by the hard
    limit. Returns the soft limit now in force."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = want if hard == resource.RLIM_INFINITY else min(want, hard)
    if soft != resource.RLIM_INFINITY and soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    return resource.getrlimit(resource.RLIMIT_NOFILE)[0]


def exit_code(mismatches: int, uncovered: int, no_data: int) -> int:
    """2 = did not run (too little fresh data to compare), 1 = flagged, 0 = clean."""
    if no_data > MAX_NO_DATA:
        return 2
    return 1 if mismatches or uncovered else 0


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--symbols", nargs="+", help="Only these tickers")
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    args = parser.parse_args(argv)

    print(
        "reading "
        + ("read-time adjusted prices" if settings.SERVED_PRICES_ENABLED
           else "STORED prices (SERVED_PRICES_ENABLED is off)")
    )
    end = datetime.now(timezone.utc)
    # Calendar days generous enough for `sessions` trading days plus one prior.
    start = end - timedelta(days=int(args.sessions * 1.6) + 10)

    async with get_session() as session:
        query = select(AssetORM.symbol).where(AssetORM.price_basis == "served")
        if args.symbols:
            query = query.where(AssetORM.symbol.in_([s.upper() for s in args.symbols]))
        symbols = list((await session.execute(query.order_by(AssetORM.symbol))).scalars())
        uncovered = [
            (symbol, n) for symbol, n in (
                await session.execute(
                    text(
                        """SELECT a.symbol, count(*) FROM market_data m
                           JOIN assets a ON a.id = m.asset_id
                           WHERE a.price_basis = 'served' AND m.served_close IS NULL
                           GROUP BY 1 ORDER BY 1"""
                    )
                )
            ).all()
            if not args.symbols or symbol in {s.upper() for s in args.symbols}
        ]
        repo = TimescaleMarketDataRepo(session)
        ours = {}
        for symbol in symbols:
            records = await repo.fetch_range(symbol, None, start, end)
            ours[symbol] = pd.Series(
                [r.ohlcv.close for r in records],
                index=[r.ohlcv.timestamp.utc.date() for r in records],
            )

    raise_open_file_limit()
    fresh = yf.download(
        symbols, start=start.date().isoformat(), auto_adjust=True,
        progress=False, threads=True,
    )["Close"]
    if isinstance(fresh, pd.Series):
        fresh = fresh.to_frame(symbols[0])
    fresh.index = [d.date() for d in fresh.index]

    mismatches, no_data = [], []
    for symbol in symbols:
        if symbol not in fresh or fresh[symbol].dropna().empty:
            no_data.append(symbol)
            continue
        both = pd.concat([ours[symbol], fresh[symbol]], axis=1, keys=["ours", "yahoo"]).dropna()
        both = both.iloc[-(args.sessions + 1):]
        gap = (both.ours.pct_change() - both.yahoo.pct_change()).dropna() * 100
        for day, x in gap[gap.abs() >= TOLERANCE_PP].items():
            mismatches.append((symbol, day, x))

    for symbol, n in uncovered:
        print(
            f"UNCOVERED {symbol}: {n} bar(s) without served values — every read "
            "of this symbol is falling back to stored prices"
        )
    for symbol, day, x in mismatches[:MAX_LINES]:
        print(f"MISMATCH {symbol} {day}: {x:+.3f}pp against a fresh Yahoo fetch")
    if len(mismatches) > MAX_LINES:
        print(f"... and {len(mismatches) - MAX_LINES} more")
    if no_data:
        print(f"no fresh Yahoo data for {len(no_data)}: {', '.join(no_data)}")
    print(
        f"Compared {len(symbols) - len(no_data)} of {len(symbols)} asset(s) over "
        f"{args.sessions} sessions; "
        f"{len(mismatches)} day(s) off by {TOLERANCE_PP}pp or more; "
        f"{len(uncovered)} served asset(s) with uncovered bars."
    )
    code = exit_code(len(mismatches), len(uncovered), len(no_data))
    if code == 2:
        print(
            f"CHECK FAILED: no fresh Yahoo data for {len(no_data)} symbol(s), "
            f"more than {MAX_NO_DATA} — the comparison did not cover the universe."
        )
    return code


if __name__ == "__main__":
    logging.disable(logging.WARNING)
    try:
        sys.exit(asyncio.run(main()))
    except Exception:  # noqa: BLE001 - any failure must read as "did not run"
        traceback.print_exc()
        print("CHECK FAILED: the fresh-return check did not complete.")
        sys.exit(2)
