"""
scripts/probe_tiingo_delisted.py

Ask Tiingo for the price history of S&P 500 names Yahoo no longer serves, and
report what comes back. Read-only: it writes a CSV report and nothing else.

    python scripts/probe_tiingo_delisted.py --sample 40      # a spread of cases
    python scripts/probe_tiingo_delisted.py --symbols SIVB CELG PARA
    python scripts/probe_tiingo_delisted.py                  # every candidate

Needs TIINGO_API_KEY in .env (free plan). Candidates come from
research/tiingo-probe-candidates-2026-10-04.csv: one row per departed ticker,
with its membership window and the Tiingo ticker to ask for.

WHAT IT IS TESTING
    Tiingo's published ticker list shows a series covering the membership
    window for most of the 173 tickers Yahoo cannot price (checked
    2026-10-04). A listing is not data. Three things only a fetch settles:

    1. Are the bars there? Sessions returned against sessions expected.
    2. For a REUSED ticker, which company does the API serve? PARA is listed
       three times and the price endpoint is keyed by ticker alone. The report
       records the name and date range Tiingo answers with, so a wrong
       identity is visible rather than assumed away.
    3. Do the prices agree with ones already trusted? For candidates Yahoo can
       still price, daily returns are compared with Yahoo's.

WHAT IT DOES NOT DO
    Store a bar. Whether Tiingo becomes a source, and how its prices sit
    beside read-time-adjusted Yahoo prices, is decided after reading the
    report. See research/sp500-membership-reconstruction-2026-10-04.md.

Exit 0 when the probe ran, 2 when it could not (no key, no candidates). A
ticker Tiingo cannot price is a finding, not a failure.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import (  # noqa: E402
    ROOT_DIR,
    TIINGO_MAX_REQUESTS_PER_HOUR,
    URL_TIINGO_DAILY,
    tiingo_headers,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("probe-tiingo")

CANDIDATES = ROOT_DIR / "research" / "tiingo-probe-candidates-2026-10-04.csv"
REPORT = ROOT_DIR / "research" / "tiingo-probe-report.csv"
#: A window counts as covered at this share of its expected sessions. Weekdays
#: overstate sessions by about nine holidays a year (3.5%), so 0.93 allows for
#: those and little else.
COVERED_SHARE = 0.93
#: A daily return differing from Yahoo's by more than this is a mismatch —
#: the same 0.5pp scripts/check_fresh_returns.py uses.
RETURN_TOLERANCE = 0.005


def window_coverage(
    bar_dates: Sequence[date], first: date, last: date
) -> tuple[int, int, str]:
    """
    (bars inside the window, weekdays in the window, verdict).

    Verdicts: `covered`, `partial`, `outside` (bars exist but none in the
    window — the mark of a ticker now belonging to someone else), `empty`.
    """
    expected = len(pd.bdate_range(first, last))
    inside = sum(1 for d in bar_dates if first <= d <= last)
    if not bar_dates:
        return 0, expected, "empty"
    if inside == 0:
        return 0, expected, "outside"
    if expected and inside >= COVERED_SHARE * expected:
        return inside, expected, "covered"
    return inside, expected, "partial"


def return_mismatches(ours: pd.Series, theirs: pd.Series) -> tuple[int, int]:
    """(days compared, days whose daily return differs by more than tolerance)."""
    both = pd.concat([ours.pct_change(), theirs.pct_change()], axis=1).dropna()
    if both.empty:
        return 0, 0
    return len(both), int(((both.iloc[:, 0] - both.iloc[:, 1]).abs() > RETURN_TOLERANCE).sum())


class Tiingo:
    """Paced to the free plan: a refused request would look like missing data."""

    def __init__(self, headers: dict, per_hour: int = TIINGO_MAX_REQUESTS_PER_HOUR):
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.per_hour = per_hour
        self.sent: list[float] = []

    def _get(self, url: str, params: Optional[dict] = None):
        now = time.monotonic()
        self.sent = [t for t in self.sent if now - t < 3600]
        if len(self.sent) >= self.per_hour - 2:
            wait = 3600 - (now - self.sent[0]) + 5
            logger.info("Hourly limit reached; waiting %.0f min.", wait / 60)
            time.sleep(wait)
        self.sent.append(time.monotonic())
        response = self.session.get(url, params=params, timeout=60)
        if response.status_code == 404:
            return None
        if response.status_code == 429:
            raise RuntimeError("Tiingo refused the request (429): limit reached.")
        response.raise_for_status()
        return response.json()

    def meta(self, ticker: str) -> Optional[dict]:
        return self._get(f"{URL_TIINGO_DAILY}/{ticker}")

    def prices(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        rows = self._get(
            f"{URL_TIINGO_DAILY}/{ticker}/prices",
            {"startDate": start.isoformat(), "endDate": end.isoformat()},
        )
        if not rows:
            return pd.DataFrame()
        frame = pd.DataFrame(rows)
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        return frame.set_index("date")


def yahoo_adjusted(symbol: str, start: date, end: date) -> pd.Series:
    import yfinance as yf

    frame = yf.download(
        symbol, start=start, end=end + pd.Timedelta(days=1),
        auto_adjust=True, progress=False, threads=False,
    )
    if frame.empty:
        return pd.Series(dtype=float)
    close = frame["Close"]
    close = close.iloc[:, 0] if isinstance(close, pd.DataFrame) else close
    close.index = pd.to_datetime(close.index).date
    return close


def pick_sample(candidates: pd.DataFrame, size: int) -> pd.DataFrame:
    """Every kind of case, hardest first: reused tickers, renamed, then plain."""
    order = {"reused": 0, "renamed": 1, "partial_listing": 2, "plain": 3, "control": 4}
    ranked = candidates.assign(_rank=candidates["case"].map(order).fillna(9))
    per_case = max(1, size // max(1, ranked["case"].nunique()))
    picked = ranked.sort_values(["_rank", "symbol"]).groupby("case").head(per_case)
    rest = ranked.drop(picked.index).sort_values(["_rank", "symbol"])
    return pd.concat([picked, rest]).head(size).drop(columns="_rank")


def probe(candidates: pd.DataFrame, tiingo: Tiingo) -> list[dict]:
    report = []
    for row in candidates.itertuples():
        first, last = date.fromisoformat(row.first), date.fromisoformat(row.last)
        ticker = row.tiingo_ticker
        result = {
            "symbol": row.symbol, "case": row.case, "tiingo_ticker": ticker,
            "window_first": first, "window_last": last,
        }
        if not isinstance(ticker, str) or not ticker:
            report.append({**result, "verdict": "no_candidate"})
            continue

        meta = tiingo.meta(ticker) or {}
        bars = tiingo.prices(ticker, first, last)
        inside, expected, verdict = window_coverage(list(bars.index), first, last)
        if verdict == "empty" and meta.get("startDate"):
            # Tiingo knows the ticker but served nothing for the window: say
            # which series it does have, so a reused ticker is recognisable.
            verdict = "outside"
        result.update(
            verdict=verdict, bars=inside, expected=expected,
            tiingo_name=meta.get("name"),
            tiingo_start=meta.get("startDate"), tiingo_end=meta.get("endDate"),
        )

        if row.case == "control" and not bars.empty:
            compared, off = return_mismatches(
                bars["adjClose"], yahoo_adjusted(row.symbol, first, last)
            )
            result.update(returns_compared=compared, returns_off=off)

        logger.info(
            "%-6s %-15s %-8s %s/%s bars  %s",
            row.symbol, row.case, verdict, inside, expected, meta.get("name") or "",
        )
        report.append(result)
    return report


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Probe Tiingo for delisted S&P 500 price history (read-only)."
    )
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--sample", type=int, help="Probe this many, spread across cases.")
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args(argv)

    headers = tiingo_headers()
    if not headers:
        logger.error("TIINGO_API_KEY is not set in the environment or .env.")
        return 2
    if not CANDIDATES.exists():
        logger.error("No candidate list at %s.", CANDIDATES)
        return 2

    candidates = pd.read_csv(CANDIDATES, dtype=str, keep_default_na=False)
    if args.symbols:
        candidates = candidates[candidates["symbol"].isin(args.symbols)]
    elif args.sample:
        candidates = pick_sample(candidates, args.sample)

    report = probe(candidates, Tiingo(headers))

    fields = [
        "symbol", "case", "tiingo_ticker", "window_first", "window_last",
        "verdict", "bars", "expected", "tiingo_name", "tiingo_start",
        "tiingo_end", "returns_compared", "returns_off",
    ]
    with args.report.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(report)

    verdicts = pd.Series([r["verdict"] for r in report]).value_counts()
    logger.info(
        "Probed %d ticker(s): %s. Report: %s",
        len(report),
        ", ".join(f"{count} {name}" for name, count in verdicts.items()),
        args.report,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
