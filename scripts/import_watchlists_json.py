"""
scripts/import_watchlists_json.py

One-time migration: reads `watchlists.json` into TimescaleDB via the
0004_watchlists schema.

Run AFTER `alembic upgrade head`:

    python scripts/import_watchlists_json.py --dry-run   # report only
    python scripts/import_watchlists_json.py             # write

Re-runnable. A watchlist whose name already exists in the database is SKIPPED,
not overwritten — the same choice as the portfolio importer, and for the same
reason: this reconciles a hand-edited file, so silently replacing a list
edited in the new UI would lose it.

Source shape:  {"MAG7": ["AAPL", "MSFT", ...], ...}
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.repositories.watchlists import TimescaleWatchlistRepo  # noqa: E402
from db.session import get_session  # noqa: E402

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "watchlists.json"


async def run(path: Path, dry_run: bool) -> int:
    if not path.exists():
        print(f"No file at {path} — nothing to import.")
        return 0

    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        print(f"{path} is not a JSON object; refusing to guess.", file=sys.stderr)
        return 1

    imported = skipped = 0
    async with get_session() as session:
        repo = TimescaleWatchlistRepo(session)

        for name, symbols in payload.items():
            if not isinstance(symbols, list):
                print(f"  SKIP {name!r}: value is {type(symbols).__name__}, not a list")
                skipped += 1
                continue

            tickers = [str(s).upper().strip() for s in symbols if str(s).strip()]
            unique = len(set(tickers))
            note = "" if unique == len(tickers) else f" ({len(tickers) - unique} duplicate(s) collapsed)"
            print(f"  {name!r}: {unique} symbol(s){note}")

            if dry_run:
                continue

            if await repo.get_watchlist(name) is not None:
                print(f"  SKIP {name!r}: already in the database")
                skipped += 1
                continue

            await repo.save_watchlist(name, tickers)
            imported += 1

    verb = "Would import" if dry_run else "Imported"
    print(f"\n{verb} {len(payload) - skipped} watchlist(s); skipped {skipped}.")
    if not dry_run:
        print(f"{imported} written. `{path.name}` is now unused and may be removed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=DEFAULT_PATH)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return asyncio.run(run(args.path, args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
