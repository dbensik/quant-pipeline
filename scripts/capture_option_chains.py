"""
scripts/capture_option_chains.py

Snapshot today's option chains to dated Parquet, raw.

    python scripts/capture_option_chains.py                  # default universe
    python scripts/capture_option_chains.py --dry-run -v     # fetch, write nothing
    python scripts/capture_option_chains.py --tickers SPY --out-dir /tmp/x

WHY THIS EXISTS
    yfinance serves only the CURRENT chain. There is no history endpoint and no
    backfill to buy later from this source, so every trading day this does not
    run is a day permanently missing from the archive. The archive is the asset.

    The task note recorded this job as "delivered 2026-08-03". On 2026-09-14
    neither this script, its wrapper, its launchd agent nor a single Parquet
    file existed on the machine, and git history had never contained them.
    Roughly 30 trading days were lost to a delivery that was never installed.

RAW, ON PURPOSE
    Vendor columns are written untouched: no mids, no filtering of zero bids,
    no IV recomputation. Any normalisation applied on the way in is a decision
    that can never be revisited, because the raw quote is gone. What IS added
    is structural and provenance only: `underlying`, `expiry` and `right` (the
    vendor frames carry none of them, and one file holds every expiry), plus
    when and under what market state the snapshot was taken.

LAYOUT
    <archive>/<TICKER>/<trade_date>.parquet           complete capture
    <archive>/<TICKER>/<trade_date>.partial.parquet   some expiries failed

    Skip is PER TICKER PER DATE. A 15:45 run that writes three of six tickers
    and dies leaves the 17:00 run real work to do; a date-level skip would turn
    the safety net into a no-op and half-capture the day permanently. A partial
    file is kept (the quotes in it are still irreplaceable) but does not count
    as done, so the next run retries and replaces it.

THE TRADE-DATE GATE
    A ticker is written only if its newest daily bar is dated today (ET). That
    refuses a holiday fire, where yfinance hands back the previous session's
    quotes without any error. When EVERY ticker is refused against the same
    earlier date, the market is closed and the run exits 0 — nine holidays a
    year each firing twice would otherwise be eighteen false alarms, and a
    check that cries wolf gets deleted. A MIXED result (some tickers have
    today's bar, some do not) is a real failure and exits 1.

    Consequence worth knowing: a Mac asleep through both 15:45 and 17:00 that
    wakes after midnight is refused, and that day is lost. Visible and
    notified, rather than recorded under the wrong date.

EXIT CODE
    Non-zero if ANY ticker failed, was partial, or was refused on a day the
    market was open. The 2026-08-25 lesson from snapshot_universes.py: "2 of 3
    succeeded, exit 0" is how ten index-days vanished unnoticed.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings

log = logging.getLogger("option_capture")

ET = ZoneInfo("America/New_York")
SOURCE = "yfinance"

# Per-expiry retry. Yahoo's transient failures are short; three tries with a
# growing pause rides them out without turning a dead endpoint into a long hang.
RETRIES = 3
BACKOFF_SECONDS = 2.0
# Politeness between chain calls. ~90 calls a run, so this adds ~20 s total.
PAUSE_SECONDS = 0.25


# ---------------------------------------------------------------------------
# Pure pieces
# ---------------------------------------------------------------------------

def plan_expiries(expiries: Iterable[str], today: date, max_dte: int) -> list[str]:
    """Expiries from today through `max_dte` calendar days out, in order."""
    out = []
    for e in expiries:
        dte = (date.fromisoformat(e) - today).days
        if 0 <= dte <= max_dte:
            out.append(e)
    return sorted(out)


def archive_path(root: Path, ticker: str, trade_date: date) -> Path:
    return root / ticker / f"{trade_date.isoformat()}.parquet"


def partial_path(root: Path, ticker: str, trade_date: date) -> Path:
    return root / ticker / f"{trade_date.isoformat()}.partial.parquet"


def chain_frame(
    calls: pd.DataFrame,
    puts: pd.DataFrame,
    *,
    underlying: str,
    expiry: str,
    provenance: dict,
) -> pd.DataFrame:
    """
    One expiry's calls and puts as a single frame.

    Vendor columns are passed through untouched. Only `underlying`, `expiry`,
    `right` (structural: the vendor frames do not carry them) and the
    provenance fields are added.
    """
    parts = []
    for right, frame in (("C", calls), ("P", puts)):
        f = frame.copy()
        f.insert(0, "right", right)
        f.insert(0, "expiry", expiry)
        f.insert(0, "underlying", underlying)
        parts.append(f)
    out = pd.concat(parts, ignore_index=True)
    for key, value in provenance.items():
        out[key] = value
    return out


def newest_bar_date(history: pd.DataFrame) -> date | None:
    """Session date (ET) of the newest daily bar, or None if there are none."""
    if history is None or history.empty:
        return None
    ts = pd.Timestamp(history.index[-1])
    if ts.tzinfo is None:
        # yfinance daily bars are exchange-local already; naive means ET.
        return ts.date()
    return ts.tz_convert(ET).date()


def write_atomically(frame: pd.DataFrame, path: Path) -> None:
    """Write to a temp name then rename, so a crash never leaves a torn file
    that the next run's skip check would mistake for a finished capture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    frame.to_parquet(tmp, index=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# One ticker
# ---------------------------------------------------------------------------

WRITTEN, SKIPPED, PARTIAL, FAILED, REFUSED = (
    "written", "skipped", "partial", "failed", "refused",
)


@dataclass
class TickerResult:
    ticker: str
    status: str
    rows: int = 0
    expiries_ok: int = 0
    expiries_planned: int = 0
    bar_date: date | None = None
    detail: str = ""
    failed_expiries: list[str] = field(default_factory=list)


def capture_ticker(
    ticker: str,
    make_ticker: Callable[[str], object],
    *,
    root: Path,
    today: date,
    now_utc: datetime,
    max_dte: int,
    min_rows: int,
    dry_run: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    vendor_version: str = "",
) -> TickerResult:
    final = archive_path(root, ticker, today)
    if final.exists():
        return TickerResult(ticker, SKIPPED, detail=f"{final.name} already written")

    t = make_ticker(ticker)

    try:
        bar_date = newest_bar_date(t.history(period="5d", interval="1d"))
    except Exception as exc:  # noqa: BLE001 — any vendor error is a failed read
        return TickerResult(ticker, FAILED, detail=f"history: {exc!r}")
    if bar_date != today:
        return TickerResult(
            ticker, REFUSED, bar_date=bar_date,
            detail=f"newest daily bar is {bar_date}, not {today}",
        )

    try:
        planned = plan_expiries(t.options, today, max_dte)
    except Exception as exc:  # noqa: BLE001
        return TickerResult(ticker, FAILED, bar_date=bar_date, detail=f"expiries: {exc!r}")
    if not planned:
        return TickerResult(
            ticker, FAILED, bar_date=bar_date,
            detail=f"no expiries within {max_dte} DTE — an empty list is a failed read",
        )

    frames: list[pd.DataFrame] = []
    failed: list[str] = []
    for expiry in planned:
        chain = None
        for attempt in range(1, RETRIES + 1):
            try:
                chain = t.option_chain(expiry)
                break
            except Exception as exc:  # noqa: BLE001
                log.warning("%s %s attempt %d/%d: %r", ticker, expiry, attempt, RETRIES, exc)
                if attempt < RETRIES:
                    sleep(BACKOFF_SECONDS * attempt)
        if chain is None:
            failed.append(expiry)
            continue

        under = getattr(chain, "underlying", None) or {}
        market_time = under.get("regularMarketTime")
        provenance = {
            "trade_date": today.isoformat(),
            "captured_at_utc": pd.Timestamp(now_utc),
            "spot": under.get("regularMarketPrice"),
            "underlying_market_time_utc": (
                pd.Timestamp(market_time, unit="s", tz="UTC") if market_time else pd.NaT
            ),
            "market_state": under.get("marketState"),
            "source": SOURCE,
            "vendor_version": vendor_version,
        }
        frames.append(
            chain_frame(chain.calls, chain.puts, underlying=ticker, expiry=expiry,
                        provenance=provenance)
        )
        sleep(PAUSE_SECONDS)

    rows = sum(len(f) for f in frames)
    result = TickerResult(
        ticker, WRITTEN, rows=rows, expiries_ok=len(frames),
        expiries_planned=len(planned), bar_date=bar_date, failed_expiries=failed,
    )

    if rows < min_rows:
        result.status = FAILED
        result.detail = f"{rows} contracts is below the {min_rows} floor — a failed read, not written"
        return result

    frame = pd.concat(frames, ignore_index=True)
    if failed:
        result.status = PARTIAL
        result.detail = f"expiries failed after {RETRIES} tries: {', '.join(failed)}"
        target = partial_path(root, ticker, today)
    else:
        target = final

    if dry_run:
        result.detail = (result.detail + " " if result.detail else "") + f"(dry run: would write {target})"
        return result

    write_atomically(frame, target)
    if target == final:
        # A complete capture supersedes an earlier partial one.
        partial_path(root, ticker, today).unlink(missing_ok=True)
    return result


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def run_exit_code(results: Sequence[TickerResult]) -> int:
    """
    0 when every ticker is written or skipped, or when every ticker was
    refused against one shared earlier bar date (the market is closed).
    1 otherwise.
    """
    if not results:
        return 1
    statuses = {r.status for r in results}
    if statuses <= {WRITTEN, SKIPPED}:
        return 0
    if statuses == {REFUSED} and len({r.bar_date for r in results}) == 1 \
            and results[0].bar_date is not None:
        return 0
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[3])
    parser.add_argument("--tickers", nargs="+", default=list(settings.OPTION_CAPTURE_TICKERS))
    parser.add_argument("--out-dir", type=Path, default=settings.OPTION_CHAIN_ARCHIVE_DIR)
    parser.add_argument("--max-dte", type=int, default=settings.OPTION_CAPTURE_MAX_DTE)
    parser.add_argument("--dry-run", action="store_true", help="fetch, write nothing")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    # yfinance is chatty at DEBUG; its detail is not what this log is for.
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)

    import yfinance as yf

    now_utc = datetime.now(UTC)
    today = now_utc.astimezone(ET).date()
    log.info(
        "option capture: %s, trade date %s (ET), <=%d DTE, into %s%s",
        " ".join(args.tickers), today, args.max_dte, args.out_dir,
        " [DRY RUN]" if args.dry_run else "",
    )

    results = []
    for ticker in args.tickers:
        r = capture_ticker(
            ticker.upper(), yf.Ticker,
            root=args.out_dir, today=today, now_utc=now_utc,
            max_dte=args.max_dte, min_rows=settings.OPTION_CAPTURE_MIN_ROWS,
            dry_run=args.dry_run, vendor_version=getattr(yf, "__version__", ""),
        )
        log.info(
            "%-5s %-8s rows=%-6d expiries=%d/%d %s",
            r.ticker, r.status.upper(), r.rows, r.expiries_ok, r.expiries_planned, r.detail,
        )
        results.append(r)

    code = run_exit_code(results)
    done = sum(1 for r in results if r.status in (WRITTEN, SKIPPED))
    if code == 0 and all(r.status == REFUSED for r in results):
        log.info("MARKET CLOSED: every ticker's newest bar is %s. Nothing to capture.",
                 results[0].bar_date)
    else:
        log.info("Captured %d of %d ticker(s). exit %d", done, len(results), code)
    return code


if __name__ == "__main__":
    sys.exit(main())
