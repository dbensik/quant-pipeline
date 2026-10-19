"""
scripts/classify_crypto.py

Record what kind of asset each registered crypto ticker is: crypto,
stablecoin or tokenized, from CoinGecko's own categories (decision 46,
core/crypto_class.py). Crypto screens leave out the pegged two by default.

    python scripts/classify_crypto.py              # report only
    python scripts/classify_crypto.py --write      # record crypto_class on the rows
    python scripts/classify_crypto.py --symbols ZEC-USD CAKE-USD

Needs each asset's `coingecko_id`, which scripts/audit_crypto_identity.py
--write records. One CoinGecko request per coin, paced like every other
CoinGecko call. A coin whose categories cannot be read is reported and left
unclassified, never defaulted to `crypto`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select, text  # noqa: E402

from config.settings import COINGECKO_REQUEST_DELAY_SECONDS  # noqa: E402
from core.crypto_class import META_CRYPTO_CLASS, classify  # noqa: E402
from core.crypto_identity import META_COINGECKO_ID  # noqa: E402
from db.models import AssetORM  # noqa: E402
from db.session import get_session  # noqa: E402


async def run(write: bool, symbols: list[str] | None) -> int:
    from data_pipeline.dynamic_universe import DynamicUniverse

    universe = DynamicUniverse()
    async with get_session() as session:
        query = select(AssetORM.symbol, AssetORM.metadata_).where(AssetORM.asset_class == "crypto")
        if symbols:
            query = query.where(AssetORM.symbol.in_(symbols))
        rows = (await session.execute(query.order_by(AssetORM.symbol))).all()

        counts: Counter = Counter()
        unclassified = []
        for symbol, metadata in rows:
            coin_id = (metadata or {}).get(META_COINGECKO_ID)
            if not coin_id:
                unclassified.append(f"{symbol} (no coingecko_id)")
                continue
            try:
                categories = universe.fetch_coin_categories(coin_id)
            except Exception as exc:  # noqa: BLE001 — reported, never defaulted
                unclassified.append(f"{symbol} ({exc})")
                continue
            finally:
                time.sleep(COINGECKO_REQUEST_DELAY_SECONDS)
            kind = classify(categories)
            counts[kind] += 1
            print(f"{symbol:16} {kind:10} {coin_id}")
            if write:
                patch = {
                    META_CRYPTO_CLASS: kind,
                    "crypto_class_checked_at": datetime.now(timezone.utc).isoformat(),
                }
                await session.execute(
                    text(
                        "UPDATE assets SET metadata = COALESCE(metadata, '{}'::jsonb) "
                        "|| CAST(:patch AS jsonb) WHERE symbol = :s AND asset_class = 'crypto'"
                    ),
                    {"patch": json.dumps(patch), "s": symbol},
                )
        if write:
            await session.commit()

    print(f"{dict(counts)}; unclassified: {', '.join(unclassified) or 'none'}")
    return 1 if unclassified else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--write", action="store_true", help="record crypto_class on the rows")
    parser.add_argument("--symbols", nargs="*", help="classify only these")
    args = parser.parse_args()
    return asyncio.run(run(args.write, args.symbols))


if __name__ == "__main__":
    sys.exit(main())
