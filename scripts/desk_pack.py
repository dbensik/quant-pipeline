"""
scripts/desk_pack.py

Write a sourced, point-in-time data pack for one name, for the Equity Research
Desk (decision 39). Read-only against the database; writes only into --out.

    python scripts/desk_pack.py MSFT --out <run folder>
    python scripts/desk_pack.py MSFT --as-of 2026-06-30 --out <run folder>
    python scripts/desk_pack.py SPY --out <dir> --trigger 800 --by 2026-12-31          # P(above 800)
    python scripts/desk_pack.py SPY --out <dir> --trigger 700 --by 2026-12-31 --below  # P(below 700)

Files (each listed with its sha256 in pack.json, ready for the desk manifest's
inputs table):

    screen.json             sector screen for the desk's conflict rule
    facts.json              every stored SEC fact filed on or before as_of,
                            with accession number and EDGAR directory URL
    statements.json         annual and quarterly statements, as filed
    prices.json             returns, volatility, drawdowns, 52-week range,
                            month-end closes (total-return and quoted)
    valuation.json          the DCF as of as_of, every input with its source,
                            and the first-year growth the price implies
    valuation_history.json  value/price at each past filing, with decision 21's
                            caveat that it does not rank returns
    options.json            ATM term structure and, with --trigger/--by, the
                            options-implied probability (desk Step 1.5)
    pack.json               status, pipeline commit, sha256s, seat routing,
                            what the pack does not cover

Exit 0 complete; 2 not in the registry (gather data the desk's usual way);
3 refused by the sector screen (the desk must not research it — nothing but
screen.json is written).
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text  # noqa: E402

from analysis.performance_analyzer import periods_per_year  # noqa: E402
from core import desk_pack as dp  # noqa: E402
from core import research_financials  # noqa: E402
from core import valuation_service as svc  # noqa: E402
from core.options_surface import json_safe  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.repositories.valuation import TimescaleValuationRepo  # noqa: E402
from db.session import AsyncSessionLocal  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
HISTORY_START = datetime(2010, 1, 1, tzinfo=timezone.utc)


def _commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, timeout=5
        )
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def _latest_surface(symbol: str, as_of: date):
    import json

    folder = ROOT / "data" / "surfaces" / symbol
    days = sorted(p for p in folder.glob("*.json") if date.fromisoformat(p.stem) <= as_of)
    return json.loads(days[-1].read_text()) if days else None


async def _facts(session, ciks: list[int], as_of: date) -> list[dict]:
    rows = await session.execute(
        text(
            "select cik, taxonomy, concept, unit, period_start, period_end, value, fy, fp, "
            "form, filed, accn, frame from fundamental_facts "
            "where cik = any(:ciks) and filed <= :as_of "
            "order by concept, period_end, filed"
        ),
        {"ciks": ciks, "as_of": as_of},
    )
    out = []
    for r in rows.mappings():
        fact = dict(r)
        fact["edgar_url"] = dp.edgar_url(fact["cik"], fact["accn"])
        out.append(fact)
    return out


async def build(args: argparse.Namespace) -> int:
    symbol, as_of, out_dir = args.symbol.upper(), args.as_of, Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    notes: dict = {}
    end = datetime.combine(as_of, time.max, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        prices = TimescaleMarketDataRepo(session)
        asset = await prices.find_asset(symbol)
        if asset is None:
            notes["reason"] = f"{symbol} is not in the pipeline's registry"
            dp.write_manifest(out_dir, symbol, as_of, "not-in-registry", _commit(), notes)
            print(f"{symbol}: not in the registry — gather data the desk's usual way.")
            return 2

        meta = asset.metadata or {}
        scr = dp.screen(symbol, asset.asset_class, meta.get("sector"))
        dp.write_json(out_dir / "screen.json", scr)
        if scr["refuse"]:
            dp.write_manifest(out_dir, symbol, as_of, "refused-conflict", _commit(), notes)
            print(f"{symbol}: REFUSED — {scr['reason']}. Nothing else was gathered.")
            return 3

        # Filing facts, point in time, with provenance.
        ciks = [int(c) for c in [meta.get("cik"), *(meta.get("cik_predecessors") or [])] if c]
        facts = await _facts(session, ciks, as_of) if ciks else []
        dp.write_json(
            out_dir / "facts.json",
            {
                "ciks": ciks,
                "as_of": as_of,
                "count": len(facts),
                "concepts": sorted({f["concept"] for f in facts}),
                "facts": facts,
                "note": "Stored concepts are the DCF's allow-list; see pack.json not_covered.",
            }
            if ciks
            else {"ciks": [], "count": 0, "reason": "no SEC filer for this asset"},
        )

        # Statements as filed (the Research page's source).
        data = TimescaleValuationRepo(session)
        statements: dict = {}
        try:
            st, *_ = await svc.filer_statements(data, symbol)
            for label, quarterly in (("annual", False), ("quarterly", True)):
                reason = research_financials.unusable_reason(st, as_of, quarterly)
                statements[label] = (
                    {"reason": reason}
                    if reason
                    else json_safe(research_financials.build(st, symbol, as_of, quarterly))
                )
        except svc.ValuationError as exc:
            statements = {"reason": exc.reason}
        dp.write_json(out_dir / "statements.json", statements)

        # Prices.
        total = await prices.fetch_range(symbol, None, HISTORY_START, end, adjust="total")
        split = await prices.fetch_range(symbol, None, HISTORY_START, end, adjust="split")
        days = [r.ohlcv.timestamp.utc.date() for r in total]
        stats = dp.price_stats(
            days,
            [r.ohlcv.close for r in total],
            [r.ohlcv.close for r in split],
            [r.ohlcv.volume or 0.0 for r in split],
            periods_per_year([r.ohlcv.timestamp.utc for r in total]),
        )
        dp.write_json(
            out_dir / "prices.json",
            {
                "stats": stats,
                "month_end_total_return": dp.month_end_closes(days, [r.ohlcv.close for r in total]),
                "month_end_quoted": dp.month_end_closes(days, [r.ohlcv.close for r in split]),
                "note": "total_return = splits and dividends reinvested; quoted = split-adjusted only",
            },
        )

        # Valuation, for the valuation-sanity seat only.
        rates = TimescaleRateRepo(session)
        if asset.asset_class == "equity":
            try:
                valuation = json_safe(await svc.value(data, prices, rates, symbol, as_of))
            except svc.ValuationError as exc:
                valuation = {"refused": exc.reason}
            try:
                points = await svc.history(data, prices, rates, symbol)
                history = [p for p in json_safe(points) if p["as_of"] <= as_of.isoformat()]
            except svc.ValuationError as exc:
                history = {"refused": exc.reason}
        else:
            valuation = {"refused": f"the DCF values equities; {symbol} is {asset.asset_class}"}
            history = []
        dp.write_json(out_dir / "valuation.json", valuation)
        dp.write_json(
            out_dir / "valuation_history.json",
            {"caveat": dp.VALUE_PRICE_CAVEAT, "points": history},
        )

    # Options, from the cached surface (computed nightly by the 06:00 job).
    dp.write_json(
        out_dir / "options.json",
        dp.options_view(
            _latest_surface(symbol, as_of), args.trigger, args.by, above=not args.below
        ),
    )

    result = dp.write_manifest(out_dir, symbol, as_of, "complete", _commit(), notes)
    print(f"{symbol} as of {as_of}: {len(result.files)} files written to {out_dir}")
    for name, digest in result.files.items():
        print(f"  {name}  {digest}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("symbol")
    parser.add_argument("--out", required=True, help="Folder to write the pack into")
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date(),
        help="Point in time (default today, UTC)",
    )
    parser.add_argument("--trigger", type=float, help="Price level for the implied probability")
    parser.add_argument("--by", type=date.fromisoformat, help="Date the trigger is judged by")
    parser.add_argument("--below", action="store_true", help="P(below trigger) instead of above")
    args = parser.parse_args()
    if (args.trigger is None) != (args.by is None):
        parser.error("--trigger and --by go together")
    return asyncio.run(build(args))


if __name__ == "__main__":
    raise SystemExit(main())
