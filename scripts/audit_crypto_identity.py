"""
scripts/audit_crypto_identity.py

Check that every crypto ticker we store is the coin we meant.

    python scripts/audit_crypto_identity.py              # report only
    python scripts/audit_crypto_identity.py --pages 4    # widen the reference set
    python scripts/audit_crypto_identity.py --write      # record verdicts on the assets

WHY THIS EXISTS
    `_fetch_top_100_crypto_tickers` keeps CoinGecko's SYMBOL and throws away
    its stable `id` and `name`, then hands "<SYM>-USD" to Yahoo. A ticker is
    not an identifier. Audited 2026-09-17: 22 of 99 crypto assets held a
    different token's entire history, 27,076 bars — Uniswap stored as "UNICORN
    Token", Aptos as "Apricot Finance", Sui as "Salmonation".

WHAT --write DOES, AND WHAT IT DOES NOT
    It records the verdict in `assets.metadata` (jsonb, so no migration):
    coingecko_id, verified_name, identity_status, identity_checked_at.

    It does NOT delete or re-ingest anything. The history of a WRONG_ASSET is
    not repairable by refetching — it belongs to another coin — and a backfill
    under the current mapping just re-imports the same wrong token. Fixing the
    mapping is a separate, deliberate step.

    The update is written explicitly here because the repository's
    `_get_or_create_asset` short-circuits on the hot path: for a symbol that
    already exists it returns the id without touching metadata, so a payload
    attached at insert time would never reach the 99 rows that matter.
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

from core.crypto_identity import (  # noqa: E402
    META_COINGECKO_ID,
    META_IDENTITY_CHECKED_AT,
    META_IDENTITY_STATUS,
    META_VERIFIED_NAME,
    Identity,
    ProviderQuote,
    verify_identity,
)
from db.models import AssetORM  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("audit-crypto-identity")


def provider_quote(symbol: str) -> ProviderQuote:
    """What the price provider serves under this ticker. The network call."""
    import yfinance as yf

    try:
        info = yf.Ticker(symbol).info or {}
    except Exception as exc:  # noqa: BLE001 — one bad symbol is not fatal
        logger.debug("no quote for %s: %s", symbol, exc)
        return ProviderQuote()
    name = (info.get("shortName") or info.get("longName") or "").strip()
    # Yahoo suffixes every crypto pair with " USD"; it is not part of the name.
    name = name.removesuffix(" USD").removesuffix(" usd").strip()
    return ProviderQuote(
        name=name or None,
        price=info.get("regularMarketPrice") or info.get("previousClose"),
    )


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pages", type=int, default=2,
                        help="CoinGecko pages of 100 coins to use as reference")
    parser.add_argument("--write", action="store_true",
                        help="record the verdicts on the asset rows")
    args = parser.parse_args(argv)

    from data_pipeline.dynamic_universe import DynamicUniverse

    references = {r.symbol: r for r in
                  DynamicUniverse().fetch_crypto_references(pages=args.pages)}
    if not references:
        logger.error("No CoinGecko reference data — cannot verify anything.")
        return 1

    async with get_session() as session:
        assets = (await session.execute(
            select(AssetORM.id, AssetORM.symbol, AssetORM.metadata_)
            .where(AssetORM.asset_class == "crypto")
            .order_by(AssetORM.symbol)
        )).all()

        checks = []
        for index, (asset_id, symbol, meta) in enumerate(assets, 1):
            base = symbol.removesuffix("-USD")
            check = verify_identity(symbol, references.get(base),
                                    provider_quote(symbol))
            checks.append((asset_id, symbol, meta or {}, check))
            if index % 25 == 0:
                logger.info("  ...%d/%d", index, len(assets))

        by_status = {s: [c for c in checks if c[3].status is s] for s in Identity}
        print(f"\nReference set: {len(references)} coins "
              f"({args.pages} page(s)).  Assets checked: {len(checks)}.\n")
        for status in (Identity.WRONG_ASSET, Identity.SUSPECT,
                       Identity.UNVERIFIABLE, Identity.MATCH):
            group = by_status[status]
            print(f"=== {status.value.upper()} ({len(group)}) ===")
            if status is Identity.MATCH:
                print("  " + ", ".join(sym for _, sym, _, _ in group) + "\n")
                continue
            for _, _, _, check in sorted(
                group, key=lambda c: -(c[3].price_gap or 0)
            ):
                print("  " + check.describe())
            print()

        if not args.write:
            print("Nothing written. Re-run with --write to record the verdicts.")
            return 0

        now = datetime.now(timezone.utc).isoformat()
        for asset_id, symbol, meta, check in checks:
            base = symbol.removesuffix("-USD")
            reference = references.get(base)
            patch = dict(meta)
            patch[META_IDENTITY_STATUS] = check.status.value
            patch[META_IDENTITY_CHECKED_AT] = now
            if reference:
                patch[META_COINGECKO_ID] = reference.coingecko_id
                patch[META_VERIFIED_NAME] = reference.name
            await session.execute(
                update(AssetORM).where(AssetORM.id == asset_id).values(metadata_=patch)
            )
        await session.commit()
        print(f"Recorded identity on {len(checks)} asset rows. "
              "No bars were deleted and nothing was re-ingested.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
