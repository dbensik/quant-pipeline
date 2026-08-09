"""
scripts/import_portfolios_json.py

One-time migration: reads `portfolios.json` and writes its portfolios and
trades into TimescaleDB via the 0003_portfolios schema.

Run AFTER `alembic upgrade head`:

    python scripts/import_portfolios_json.py --dry-run   # report only
    python scripts/import_portfolios_json.py             # write

Re-runnable. Portfolios that already exist in the database are skipped, not
merged — this reconciles a hand-edited file, and silently appending trades to
an existing portfolio on a second run would double its position.

Source shapes
-------------
`portfolios.json` accumulated TWO incompatible shapes under one key, which is
why the gRPC paper-trading service raised KeyError('cash') against it:

  A. Trade log      {"trades": [{trade_id, date, ticker, action, direction,
                                 quantity, price, costs, broker, notes}, ...]}
  B. Ledger         {"cash": float, "positions": {sym: {quantity,
                                                        average_price}}}

Both import into the one target shape: a portfolio with an `initial_cash` and
a trade log, from which cash and positions are derived.

Shape B has no trade history, so an OPENING TRADE is synthesised per position
at its average price, and `initial_cash` is set to cash + the cost of those
positions. That reproduces exactly the ledger's cash and positions when
re-derived, which is the only property the ledger actually asserted.

`direction` (Long/Short) is dropped: nothing ever read it, and `action`
already signs the trade. A trade whose direction is Short is reported, since
for those rows the original intent is genuinely ambiguous.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.portfolio import BUY, SELL, Trade, derive_state  # noqa: E402
from db.repositories.portfolios import TimescalePortfolioRepo  # noqa: E402
from db.session import get_session  # noqa: E402

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "portfolios.json"
KNOWN_KEYS = {"trades", "cash", "positions"}


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt).replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
    return datetime.now(timezone.utc)


def convert(name: str, data: Dict[str, Any]) -> Tuple[float, List[Trade], Dict, List[str]]:
    """
    One JSON portfolio to (initial_cash, trades, metadata, warnings).

    Pure, so the conversion is testable without a database.
    """
    warnings: List[str] = []
    trades: List[Trade] = []
    # Anything that is not a recognised shape key is preserved rather than
    # dropped: MAG7PortTest1 carried `constituents` and `weights`.
    metadata = {k: v for k, v in data.items() if k not in KNOWN_KEYS} or None

    for index, raw in enumerate(data.get("trades") or []):
        action = str(raw.get("action", "")).upper()
        if action not in (BUY, SELL):
            warnings.append(
                f"{name}: trade {index} has unusable action "
                f"{raw.get('action')!r}; skipped."
            )
            continue
        direction = str(raw.get("direction", "")).upper()
        if direction == "SHORT":
            warnings.append(
                f"{name}: trade {index} was recorded direction=Short alongside "
                f"action={raw.get('action')}. `direction` is dropped; the trade "
                f"is imported as a plain {action}. Verify the sign."
            )
        try:
            quantity = float(raw.get("quantity", 0) or 0)
            price = float(raw.get("price", 0) or 0)
        except (TypeError, ValueError):
            warnings.append(f"{name}: trade {index} has a non-numeric amount; skipped.")
            continue
        if quantity <= 0 or price <= 0:
            warnings.append(
                f"{name}: trade {index} has quantity={quantity} price={price}; skipped."
            )
            continue

        trades.append(
            Trade(
                ticker=str(raw.get("ticker", "")).upper(),
                action=action,
                quantity=quantity,
                price=price,
                ts=_parse_date(raw.get("date")),
                costs=float(raw.get("costs", 0) or 0),
                broker=raw.get("broker") or None,
                notes=raw.get("notes") or None,
            )
        )

    if "cash" in data and "positions" in data:
        # Ledger shape: synthesise the opening trades that produce it.
        cash = float(data.get("cash") or 0.0)
        opening = datetime(1970, 1, 1, tzinfo=timezone.utc)
        cost = 0.0
        for ticker, position in (data.get("positions") or {}).items():
            quantity = float(position.get("quantity", 0) or 0)
            average = float(position.get("average_price", 0) or 0)
            if quantity == 0 or average <= 0:
                continue
            cost += quantity * average
            trades.append(
                Trade(
                    ticker=str(ticker).upper(),
                    action=BUY if quantity > 0 else SELL,
                    quantity=abs(quantity),
                    price=average,
                    ts=opening,
                    notes="Opening balance imported from portfolios.json",
                )
            )
        # initial_cash chosen so derived cash lands back on the ledger's value.
        return cash + cost, trades, metadata, warnings

    return 100_000.0, trades, metadata, warnings


async def run(path: Path, dry_run: bool) -> int:
    if not path.exists():
        print(f"No file at {path} — nothing to import.")
        return 0

    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        print(f"{path} is not a JSON object; refusing to guess.", file=sys.stderr)
        return 1

    # Legacy single-portfolio file, the same case PortfolioManager handled.
    if "cash" in payload and "positions" in payload:
        payload = {"Default Portfolio": payload}

    imported = skipped = 0
    async with get_session() as session:
        repo = TimescalePortfolioRepo(session)

        for name, data in payload.items():
            if not isinstance(data, dict):
                print(f"  SKIP {name!r}: not an object")
                skipped += 1
                continue

            initial_cash, trades, metadata, warnings = convert(name, data)
            for warning in warnings:
                print(f"  WARN {warning}")

            state = derive_state(trades, initial_cash)
            print(
                f"  {name!r}: {len(trades)} trade(s), initial cash "
                f"{initial_cash:,.2f} -> derived cash {state.cash:,.2f}, "
                f"{len(state.positions)} open position(s)"
            )

            if dry_run:
                continue

            if await repo.get_portfolio(name) is not None:
                print(f"  SKIP {name!r}: already in the database")
                skipped += 1
                continue

            await repo.create_portfolio(
                name=name, initial_cash=initial_cash, metadata=metadata
            )
            for trade in trades:
                await repo.add_trade(name, trade)
            imported += 1

    verb = "Would import" if dry_run else "Imported"
    print(f"\n{verb} {len(payload) - skipped} portfolio(s); skipped {skipped}.")
    if not dry_run:
        print(f"{imported} written. `{path.name}` is now unused and may be removed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=DEFAULT_PATH)
    parser.add_argument(
        "--dry-run", action="store_true", help="Report what would happen; write nothing"
    )
    args = parser.parse_args()
    return asyncio.run(run(args.path, args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
