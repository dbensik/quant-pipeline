"""
scripts/check_line_concepts.py

Flag DCF lines a filer's statements may be reading wrong. Read-only.

    python scripts/check_line_concepts.py                 # every registered equity
    python scripts/check_line_concepts.py --symbols CHTR ADM
    python scripts/check_line_concepts.py --as-of 2026-10-12

Exit 0 when nothing is flagged, 1 when anything is, 2 if the check itself
failed. Run by the 06:00 job after the SEC refresh, since new filings are what
change the answer.

WHAT IT FLAGS
    CONCEPT  another concept reports a valued company's newest fiscal year
             differently, and valuing on it instead moves value/price by
             settings.LINE_CHECK_MIN_MOVE and LINE_CHECK_MIN_REL. On
             2026-10-13, 7 companies' growth had been read from a part of
             their revenue (CHTR's $889M of $54,774M) and nothing said so.
    STALE    a settings.LINE_CONCEPTS_BY_SYMBOL entry names concepts the
             filer stopped using: the mapped line ends before the list's.
             A mapped line is checked for this only; its entry is its review.
    SPLIT?   a split row with an odd ratio not in
             settings.SPLIT_ROWS_REVIEWED (decision 58): Yahoo serves a
             spin-off as a split, and until the row is read the DCF
             multiplies share counts by it. Checked for every equity, valued
             or refused.
    Logic and the lines checked: core/line_concept_check.py.

CLEARING A FLAG
    Read the filer's statement (the 10-K's, via FilingSummary.xml). If the
    chosen concept is the statement's line, add (symbol, item, concept) with
    what the statement shows to settings.LINE_CONCEPTS_REVIEWED. If not, map
    the right one in LINE_CONCEPTS_BY_SYMBOL. Either way it is a code change
    with its reason beside it, like every map entry.

It values only companies with an alternative (about 180 of 508), and only
those the DCF values: a refused company has no value to move.
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

from config import settings  # noqa: E402
from core import line_concept_check as lcc  # noqa: E402
from core import valuation_service as svc  # noqa: E402
from db.models import AssetORM  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.repositories.valuation import TimescaleValuationRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402


async def value_per_price(session, symbol, as_of, statements):
    try:
        r = await svc.value(
            TimescaleValuationRepo(session),
            TimescaleMarketDataRepo(session),
            TimescaleRateRepo(session),
            symbol,
            as_of,
            statements=statements,
            with_grid=False,
        )
    except svc.ValuationError:
        return None, None
    ps = r["result"]["per_share"]
    return (ps / r["price"] if ps is not None else None), r["lines"]


async def check(symbol: str, as_of: date) -> list[str]:
    async with AsyncSessionLocal() as session:
        splits = lcc.unreviewed_splits(
            symbol,
            [s for s in await TimescaleValuationRepo(session).splits(symbol) if s[0] <= as_of],
        )
        try:
            # load_statements, not filer_statements: its refusals (HUM's
            # managed care) are what the DCF does, and value() trusts them.
            loaded = await svc.load_statements(TimescaleValuationRepo(session), symbol)
        except svc.ValuationError:
            return splits
        st = loaded[0]
        found = splits + [
            f"STALE {symbol}: {why}"
            for item in lcc.CHECKED_LINES
            if (why := lcc.stale_map(st, item, as_of))
        ]
        alts = [
            a
            for item in lcc.CHECKED_LINES
            if item not in st.line_concepts  # a map entry is its review
            for a in lcc.alternatives(st, item, as_of, settings.LINE_CHECK_MIN_SHARE)
            if (symbol, a.item, a.concept) not in settings.LINE_CONCEPTS_REVIEWED
        ]
        if not alts:
            return found
        base, base_lines = await value_per_price(session, symbol, as_of, loaded)
        if base is None:
            return found
        for a in alts:
            alt_st = lcc.with_line(st, a.item, (a.concept,))
            alt, alt_lines = await value_per_price(session, symbol, as_of, (alt_st, *loaded[1:]))
            line = lcc.CHECKED_LINES[a.item]
            if (
                alt is None
                or not lcc.same_basis(base_lines[line]["source"], alt_lines[line]["source"])
                or not lcc.material(
                    base, alt, settings.LINE_CHECK_MIN_MOVE, settings.LINE_CHECK_MIN_REL
                )
            ):
                continue
            found.append(
                f"CONCEPT {symbol} {a.item} FY {a.period_end}: uses {a.used} "
                f"{a.used_value / 1e6:,.0f}M; {a.concept} reports {a.value / 1e6:,.0f}M; "
                f"value/price {base:.2f} -> {alt:.2f}"
            )
    return found


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--symbols", nargs="+", help="Check only these tickers")
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    args = parser.parse_args(argv)

    async with AsyncSessionLocal() as session:
        query = select(AssetORM.symbol).where(
            AssetORM.asset_class == "equity", AssetORM.metadata_["cik"].isnot(None)
        )
        if args.symbols:
            query = query.where(AssetORM.symbol.in_([s.upper() for s in args.symbols]))
        symbols = sorted(r[0] for r in (await session.execute(query)).all())

    flagged = 0
    for symbol in symbols:
        for line in await check(symbol, args.as_of):
            flagged += 1
            print(line, flush=True)
    print(f"Checked {len(symbols)} equities as of {args.as_of}; {flagged} line(s) flagged.")
    return 1 if flagged else 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception:  # noqa: BLE001 - any failure must read as "did not run"
        traceback.print_exc()
        print("CHECK FAILED: the line-concept check did not complete.")
        sys.exit(2)
