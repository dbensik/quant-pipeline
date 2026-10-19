"""
scripts/register_new_members.py

Register the equities that joined an index, and flag what a human must judge
(decision 44, research/new-index-members-2026-10-19.md). Runs in the 06:00
job right after the snapshot that records them.

    python scripts/register_new_members.py --dry-run   # the plan, nothing written
    python scripts/register_new_members.py

For each current member of an equity index in config.settings.NEW_MEMBER_INDEXES
that the registry does not hold (after the share-class and dated-rename
mapping the screener uses):
  - a CIK some registered asset already carries is a probable RENAME: flagged,
    not registered (re-key that row, as PSKY -> SKYD on 2026-10-19);
  - no CIK in SEC's ticker map: flagged, not registered;
  - otherwise registered as an equity with its CIK, backfilled insert-only
    from NEW_MEMBER_BACKFILL_START, and marked `served` when every bar has
    served values. A series that opens on zero-volume bars is flagged with a
    suggested history_valid_from; nothing is trimmed.
Crypto members are never registered: a reviewed one left out on purpose
(CRYPTO_MEMBERS_NOT_REGISTERED) is counted; any other is FLAGGED as a new coin,
with its CoinGecko class (crypto, stablecoin, tokenized; decision 46).

Exit 0: nothing needs a human. 1: something was flagged. 2: the run failed.
SEC's ticker map is read only when an equity joiner exists (needs SEC_USER_AGENT).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sqlalchemy import select, text  # noqa: E402

from config import settings  # noqa: E402
from core.ingest import ingest_symbols  # noqa: E402
from core.membership_reconstruction import renames_from_rows  # noqa: E402
from core.new_members import (  # noqa: E402
    placeholder_floor,
    plan_registrations,
    registry_form,
    report,
)
from db.models import AssetORM  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.repositories.universe import TimescaleUniverseRepo  # noqa: E402
from db.session import get_session  # noqa: E402

logger = logging.getLogger("register_new_members")


def classify_new_coins(symbols: list[str]) -> dict[str, str]:
    """
    CoinGecko's class for each new crypto member (decision 46), so the flag
    says whether it is pegged. Best effort: an unreachable CoinGecko leaves it
    "unclassified", and the member is flagged either way.
    """
    if not symbols:
        return {}
    import time

    from core.crypto_class import classify
    from data_pipeline.dynamic_universe import DynamicUniverse

    universe = DynamicUniverse()
    out: dict[str, str] = {}
    try:
        references: dict[str, str] = {}
        for ref in universe.fetch_crypto_references(pages=3):
            references.setdefault(ref.symbol.upper() + "-USD", ref.coingecko_id)
        for symbol in symbols:
            if symbol in references:
                time.sleep(settings.COINGECKO_REQUEST_DELAY_SECONDS)
                out[symbol] = classify(universe.fetch_coin_categories(references[symbol]))
    except Exception as exc:  # noqa: BLE001 — classification only informs the flag
        logger.warning("could not classify new crypto members: %s", exc)
    return out


async def run(dry_run: bool) -> int:
    today = datetime.now(timezone.utc)
    with open(settings.SP500_TICKER_RENAMES_CSV, newline="") as fh:
        renames = renames_from_rows(csv.DictReader(fh))

    async with get_session() as session:
        universe = TimescaleUniverseRepo(session)
        members, classes = {}, {}
        for index, (asset_class, _label) in settings.NEW_MEMBER_INDEXES.items():
            membership = await universe.members_as_of(index, today)
            if not membership.observed:
                print(f"FAILED: no snapshot of {index!r}; nothing to compare against.")
                return 2
            members[index], classes[index] = membership.symbols, asset_class

        rows = (await session.execute(select(AssetORM.symbol, AssetORM.metadata_))).all()
        registered = {symbol for symbol, _ in rows}
        registered_ciks: dict[int, list[str]] = {}
        for symbol, metadata in rows:
            metadata = metadata or {}
            for cik in [metadata.get("cik"), *(metadata.get("cik_predecessors") or [])]:
                if cik:
                    registered_ciks.setdefault(int(cik), []).append(symbol)

        # SEC only when an equity joiner exists: most mornings there is none.
        ticker_ciks: dict[str, int] = {}
        titles: dict[int, str] = {}
        known = settings.CRYPTO_MEMBERS_NOT_REGISTERED
        probe = plan_registrations(
            members, classes, registered, {}, registered_ciks, renames, today.date(), known
        )
        if probe.no_cik:
            from core.sec import SECGateway

            for row in SECGateway().company_tickers().values():
                ticker_ciks[registry_form(row["ticker"].upper())] = int(row["cik_str"])
                titles[int(row["cik_str"])] = row["title"]
        plan = plan_registrations(
            members, classes, registered, ticker_ciks, registered_ciks, renames, today.date(), known
        )
        crypto_classes = classify_new_coins(plan.crypto_unregistered)

        if dry_run:
            print(f"dry run: would register {[s for s, _ in plan.register]}")
            lines, code = report(plan, {}, {}, [], crypto_classes)
            print("\n".join(lines) or "nothing to do")
            return code

        written: dict[str, tuple[int, date | None]] = {}
        placeholders: dict[str, date] = {}
        empty: list[str] = []
        if plan.register:
            for symbol, cik in plan.register:
                label = settings.NEW_MEMBER_INDEXES[plan.index_of[symbol]][1]
                session.add(
                    AssetORM(
                        symbol=symbol,
                        asset_class="equity",
                        source="yfinance",
                        metadata_={
                            "index": label,
                            "cik": cik,
                            "cik_source": "ticker_map; company_tickers.json (live)",
                            "cik_sec_title": titles.get(cik),
                            "cik_predecessors": [],
                            "registered_by": f"register_new_members {today.date()}",
                        },
                    )
                )
            await session.commit()

            start = datetime.fromisoformat(settings.NEW_MEMBER_BACKFILL_START).replace(
                tzinfo=timezone.utc
            )
            repo = TimescaleMarketDataRepo(session)
            symbols = [s for s, _ in plan.register]
            await ingest_symbols(repo=repo, symbols=symbols, start=start, skip_delisted=False)

            for symbol in symbols:
                bars = (
                    await session.execute(
                        text(
                            "SELECT m.time, m.volume, m.served_close FROM market_data m "
                            "JOIN assets a ON a.id = m.asset_id "
                            "WHERE a.symbol = :s AND a.asset_class = 'equity' ORDER BY m.time"
                        ),
                        {"s": symbol},
                    )
                ).all()
                if not bars:
                    empty.append(symbol)
                    continue
                written[symbol] = (len(bars), bars[0][0].date())
                floor = placeholder_floor([b[0].date() for b in bars], [b[1] for b in bars])
                if floor is not None:
                    placeholders[symbol] = floor
                if all(b[2] is not None for b in bars):
                    await session.execute(
                        text(
                            "UPDATE assets SET price_basis = 'served' "
                            "WHERE symbol = :s AND asset_class = 'equity'"
                        ),
                        {"s": symbol},
                    )
            await session.commit()

    lines, code = report(plan, written, placeholders, empty, crypto_classes)
    print("\n".join(lines) or "No new index members.")
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="Print the plan; write nothing")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    try:
        return asyncio.run(run(args.dry_run))
    except Exception as exc:  # noqa: BLE001 — the wrapper needs 2, not a traceback exit 1
        logger.exception("register_new_members failed: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
