"""
scripts/reconcile_timescale_migration.py

Phase 2 exit-gate check: prove TimescaleDB holds exactly the data the legacy
SQLite database holds, modulo the rows the migration deliberately skipped.

    python scripts/reconcile_timescale_migration.py

Exits non-zero if any check fails, so it can gate a deploy or be re-run after a
partial migration.

Checks
------
1. Row totals            SQLite non-empty bars == TimescaleDB market_data rows
2. Asset registry        one asset per distinct legacy ticker, correctly classified
3. Per-ticker row counts every ticker matches, listing any that do not
4. Date coverage         per-ticker min/max timestamps match
5. Value fidelity        OHLCV compared row-for-row on a sample of tickers

Phase 2 — TimescaleDB Schema & Repository Layer
"""

import asyncio
import sqlite3
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from sqlalchemy import text

from db.session import get_session
from scripts.migrate_sqlite_to_timescale import (
    DEFAULT_SQLITE_PATH,
    DEFAULT_TABLE,
    classify_asset_class,
)

SAMPLE_TICKERS = ["AAPL", "MSFT", "BTC-USD", "ETH-USD", "JPM", "SOL-USD"]

# Float tolerance for OHLCV comparison. Both sides store IEEE-754 doubles and the
# migration does no arithmetic, so values should be bit-identical; this guards
# only against representation noise in transit.
TOLERANCE = 1e-9


def fail(msg: str) -> bool:
    print(f"  FAIL  {msg}")
    return False


def ok(msg: str) -> bool:
    print(f"  ok    {msg}")
    return True


async def main() -> int:
    conn = sqlite3.connect(DEFAULT_SQLITE_PATH)
    conn.row_factory = sqlite3.Row

    non_empty = (
        "NOT (Open IS NULL AND High IS NULL AND Low IS NULL AND Close IS NULL)"
    )
    passed = True

    async with get_session() as session:

        # -- 1. row totals ---------------------------------------------------
        print("\n[1] Row totals")
        sq_total = conn.execute(
            f"SELECT COUNT(*) FROM {DEFAULT_TABLE}"
        ).fetchone()[0]
        sq_real = conn.execute(
            f"SELECT COUNT(*) FROM {DEFAULT_TABLE} WHERE {non_empty}"
        ).fetchone()[0]
        sq_empty = sq_total - sq_real
        ts_total = await session.scalar(text("SELECT COUNT(*) FROM market_data"))

        print(f"        SQLite total rows      : {sq_total:>9,}")
        print(f"        SQLite empty bars      : {sq_empty:>9,}  (intentionally skipped)")
        print(f"        SQLite migratable rows : {sq_real:>9,}")
        print(f"        TimescaleDB rows       : {ts_total:>9,}")
        passed &= (
            ok(f"{ts_total:,} == {sq_real:,}")
            if ts_total == sq_real
            else fail(f"TimescaleDB {ts_total:,} != SQLite migratable {sq_real:,}")
        )

        # -- 2. asset registry -----------------------------------------------
        print("\n[2] Asset registry")
        sq_tickers = {r[0] for r in conn.execute(
            f"SELECT DISTINCT Ticker FROM {DEFAULT_TABLE}"
        ) if r[0]}
        rows = (await session.execute(
            text("SELECT symbol, asset_class FROM assets")
        )).all()
        ts_assets = {sym: cls for sym, cls in rows}

        missing = sq_tickers - set(ts_assets)
        extra = set(ts_assets) - sq_tickers
        passed &= (
            ok(f"{len(ts_assets)} assets, one per legacy ticker")
            if not missing and not extra
            else fail(f"missing={sorted(missing)[:10]} extra={sorted(extra)[:10]}")
        )

        misclassified = [
            s for s, cls in ts_assets.items() if cls != classify_asset_class(s)
        ]
        passed &= (
            ok(f"asset_class correct for all {len(ts_assets)} assets")
            if not misclassified
            else fail(f"misclassified: {misclassified[:10]}")
        )
        counts: dict = {}
        for cls in ts_assets.values():
            counts[cls] = counts.get(cls, 0) + 1
        print(f"        breakdown: {counts}")

        # -- 3 & 4. per-ticker counts and date coverage ----------------------
        print("\n[3] Per-ticker row counts")
        sq_counts = {
            r["Ticker"]: r["n"]
            for r in conn.execute(
                f"SELECT Ticker, COUNT(*) n FROM {DEFAULT_TABLE} "
                f"WHERE {non_empty} GROUP BY Ticker"
            )
        }
        ts_rows = (await session.execute(text(
            "SELECT a.symbol, COUNT(*) n, MIN(m.time) lo, MAX(m.time) hi "
            "FROM market_data m JOIN assets a ON a.id = m.asset_id "
            "GROUP BY a.symbol"
        ))).all()
        ts_counts = {sym: n for sym, n, _, _ in ts_rows}
        ts_ranges = {sym: (lo, hi) for sym, _, lo, hi in ts_rows}

        mismatched = [
            (t, sq_counts.get(t, 0), ts_counts.get(t, 0))
            for t in sorted(sq_tickers)
            if sq_counts.get(t, 0) != ts_counts.get(t, 0)
        ]
        passed &= (
            ok(f"all {len(sq_counts)} tickers match on row count")
            if not mismatched
            else fail(f"{len(mismatched)} mismatched: {mismatched[:10]}")
        )

        print("\n[4] Date coverage")
        sq_ranges = {
            r["Ticker"]: (r["lo"], r["hi"])
            for r in conn.execute(
                f"SELECT Ticker, MIN(Timestamp) lo, MAX(Timestamp) hi "
                f"FROM {DEFAULT_TABLE} WHERE {non_empty} GROUP BY Ticker"
            )
        }
        bad_ranges = []
        for t, (lo, hi) in sq_ranges.items():
            if t not in ts_ranges:
                bad_ranges.append((t, "absent"))
                continue
            ts_lo, ts_hi = ts_ranges[t]
            if (ts_lo.strftime("%Y-%m-%d %H:%M:%S") != lo
                    or ts_hi.strftime("%Y-%m-%d %H:%M:%S") != hi):
                bad_ranges.append((t, f"{lo}..{hi} vs {ts_lo}..{ts_hi}"))
        passed &= (
            ok(f"all {len(sq_ranges)} tickers match on first/last timestamp")
            if not bad_ranges
            else fail(f"{len(bad_ranges)} range mismatches: {bad_ranges[:5]}")
        )

        # -- 5. value fidelity ------------------------------------------------
        print("\n[5] Value fidelity (row-for-row OHLCV)")
        for ticker in SAMPLE_TICKERS:
            if ticker not in sq_tickers:
                print(f"  skip  {ticker} (not in source)")
                continue
            sq_bars = {
                r["Timestamp"]: (r["Open"], r["High"], r["Low"], r["Close"], r["Volume"])
                for r in conn.execute(
                    f"SELECT * FROM {DEFAULT_TABLE} WHERE Ticker = ? AND {non_empty}",
                    (ticker,),
                )
            }
            ts_bars = {
                t.strftime("%Y-%m-%d %H:%M:%S"): (o, h, lo_, c, v)
                for t, o, h, lo_, c, v in (await session.execute(
                    text(
                        "SELECT m.time, m.open, m.high, m.low, m.close, m.volume "
                        "FROM market_data m JOIN assets a ON a.id = m.asset_id "
                        "WHERE a.symbol = :sym"
                    ),
                    {"sym": ticker},
                )).all()
            }

            diffs = 0
            for ts, sq_vals in sq_bars.items():
                ts_vals = ts_bars.get(ts)
                if ts_vals is None:
                    diffs += 1
                    continue
                for a, b in zip(sq_vals, ts_vals):
                    if a is None and b is None:
                        continue
                    if a is None or b is None or abs(float(a) - float(b)) > TOLERANCE:
                        diffs += 1
                        break
            passed &= (
                ok(f"{ticker:<9} {len(sq_bars):>5,} bars identical")
                if diffs == 0
                else fail(f"{ticker}: {diffs} differing bars of {len(sq_bars):,}")
            )

    conn.close()
    print("\n" + ("PASS — Phase 2 exit gate satisfied." if passed
                  else "FAIL — reconciliation found differences."))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
