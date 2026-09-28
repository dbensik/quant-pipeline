"""
scripts/backfill_served.py

Phase 4 of research/dividend-drift-plan-2026-09-27.md: give every stored
equity and ETF bar its served (auto_adjust=False) values, and record the
splits and dividends across its history.

    python scripts/backfill_served.py --symbols MO NVDA HWM   # a pilot
    python scripts/backfill_served.py                          # the registry

WHAT IT WRITES, AND ONLY THAT
    served_* and fetched_at on rows that already exist and have none
    (`fill_served`, NULL-guarded), and new rows in corporate_actions
    (`write_actions`, first row kept). It never inserts a bar and never touches
    the adjusted columns. Running ingest over the full history instead would
    INSERT every bar Yahoo has and we do not — the placeholder and
    reassigned-ticker bars the AMCR and PARA repairs removed on purpose.

WHAT IT SKIPS
    Crypto: no splits or dividends; it keeps its current basis (plan: out of
    scope). Assets whose recorded identity blocks ingest (PARA: Yahoo's key now
    serves another company, so its served values would be that company's).
    Actions outside an asset's history floor/ceiling, exactly as ingest does.

FETCH ERRORS ARE NOT "NO DATA" — AND THIS ONLY PARTLY TELLS THEM APART
    A download that raises is retried and, if it still fails, reported as an
    ERROR. But yf.download reports a PER-TICKER failure (HOLX: "possibly
    delisted; no price data found") by printing it and returning an empty
    frame, not by raising — so a per-ticker rate limit would also come back
    empty. The real guard is the coverage check in scripts/gate_served.py:
    every stored bar left without served values must be accounted for. On
    2026-09-27 all were — 8 symbols Yahoo serves nothing for, 2 post-deal
    stubs, and PARA, skipped by design.

Idempotent: re-running fills nothing already filled and records no action
already recorded.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from core.adapters import yfinance_adapter  # noqa: E402
from core.crypto_identity import metadata_allows_ingest  # noqa: E402
from core.ingest import (  # noqa: E402
    DEFAULT_BACKFILL_START,
    history_ceiling,
    history_floor,
    retag,
)
from db.models import AssetORM  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.session import get_session  # noqa: E402

ATTEMPTS = 3
BACKOFF_SECONDS = 5


def fetch_with_retries(symbol: str) -> tuple[list, str | None]:
    """Full history, or ([], error) after ATTEMPTS failed downloads."""
    error = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            return (
                yfinance_adapter.fetch(
                    [symbol],
                    DEFAULT_BACKFILL_START.strftime("%Y-%m-%d"),
                    date.today().isoformat(),
                    raise_errors=True,
                ),
                None,
            )
        except Exception as exc:  # noqa: BLE001 - reported, never swallowed
            error = repr(exc)
            time.sleep(BACKOFF_SECONDS * attempt)
    return [], error


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--symbols", nargs="+", help="Only these tickers")
    parser.add_argument("--report", help="Write per-symbol results as JSON here")
    args = parser.parse_args(argv)

    results = []
    async with get_session() as session:
        repo = TimescaleMarketDataRepo(session)
        query = select(AssetORM.symbol).where(AssetORM.asset_class != "crypto")
        if args.symbols:
            query = query.where(AssetORM.symbol.in_([s.upper() for s in args.symbols]))
        symbols = list((await session.execute(query.order_by(AssetORM.symbol))).scalars())

        for index, symbol in enumerate(symbols, start=1):
            asset = await repo.find_asset(symbol)
            outcome = {"symbol": symbol}
            if not metadata_allows_ingest(asset.metadata):
                outcome["skipped"] = "identity blocks ingest"
            else:
                records, error = fetch_with_retries(symbol)
                if error:
                    outcome["error"] = error
                else:
                    floor, ceiling = history_floor(asset.metadata), history_ceiling(asset.metadata)
                    records = [
                        retag(r, asset.asset_class, asset.source)
                        for r in records
                        if (floor is None or r.ohlcv.timestamp.utc >= floor)
                        and (ceiling is None or r.ohlcv.timestamp.utc < ceiling)
                    ]
                    outcome["fetched"] = len(records)
                    outcome["served_filled"] = await repo.fill_served(records)
                    outcome["actions_recorded"] = await repo.write_actions(records)
            results.append(outcome)
            print(f"[{index}/{len(symbols)}] {json.dumps(outcome)}", flush=True)

    errors = [r["symbol"] for r in results if "error" in r]
    print(
        f"Done: {len(results)} asset(s); "
        f"{sum(r.get('served_filled', 0) for r in results)} bar(s) filled; "
        f"{sum(r.get('actions_recorded', 0) for r in results)} action(s) recorded; "
        f"{len(errors)} fetch error(s){': ' + ', '.join(errors) if errors else ''}."
    )
    if args.report:
        Path(args.report).write_text(json.dumps(results, indent=1))
    return 1 if errors else 0


if __name__ == "__main__":
    logging.disable(logging.WARNING)
    sys.exit(asyncio.run(main()))
