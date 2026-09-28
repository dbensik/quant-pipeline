"""
scripts/gate_served.py

Phase 4 gate of research/dividend-drift-plan-2026-09-27.md: which equities and
ETFs can move to read-time adjustment without their history changing beyond
the intended fixes. Read-only.

    python scripts/gate_served.py --report research/served-gate-2026-09-27.json

WHAT IT PROVES, AND WHAT IT DOES NOT
    The engine's 'total' output equals Yahoo's CURRENT auto_adjust series to
    1e-6 (validated in phase 2). So this compares stored history against Yahoo
    today. A pass means: switching this symbol changes its daily returns only
    on days the plan meant to fix. It does not re-test the engine.

THE RULES (every mismatch of >= 0.5pp on a day must be explained, or it fails)
    coverage  every stored bar has served values. `served` cannot fall back to
              the adjusted columns bar by bar — that mixes two bases, which is
              the problem being fixed.
    dividend  the day is the symbol's own ex-date and the gap is within 0.1pp
              of that dividend's yield — stored showed the raw drop.
    fill      the 2026-08-28 / 08-31 pair, or DXCM/MGM 2026-09-22 / 09-23:
              equal and opposite (sum within 0.05pp). The filled bar was fetched
              after a later ex-date and carries a dividend its neighbours lack.
    seam      a date where at least SEAM_MIN_SYMBOLS symbols mismatch together
              (a fetch-schedule boundary, found from the data rather than
              assumed), and the gap within SEAM_MATCH_PP of the COMPOUNDED yields
              of the dividends between it and the refill — or of a running
              product of them in ex-date order, since the refill can predate the
              last ex-date in the window. Two-sided: measured 2026-09-27 over
              the 364 seam days, gap minus full sum had median -0.006pp and
              95% within [-0.154, +0.024]; the four largest (AEP, IBM, PPG,
              ROL, -0.48 to -0.76) are each one quarterly dividend short.
    schedule  the gap is within 0.05pp of the yield of ONE dividend with an
              ex-date from 3 days before to 21 days after: the bars either side
              were fetched on opposite sides of that dividend. Found on the
              2026-08-17 boundary (the Mac slept through 06:00 from 08-18 to
              08-24), the 2026-08-27 boundary (the DB outage), and on filled
              bars fetched after a later ex-date. 23 cases on 2026-09-27, the
              worst match 0.022pp. ONLY from SCHEDULE_FROM on: before the
              2025-07-16 seam the bars came from one bulk fetch, so a
              dividend-sized gap there is not a schedule effect but a stored
              error (FAST double-counts its 2020 dividends) — left UNEXPLAINED
              for a human, even though switching would correct it.
    Anything else is UNEXPLAINED, and fails the symbol.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402

from core.price_adjustment import Action, ServedBar, adjust, raw_dividends  # noqa: E402

TOLERANCE_PP = 0.5
DIVIDEND_MATCH_PP = 0.1
SCHEDULE_MATCH_PP = 0.05
SEAM_MATCH_PP = 0.2
SCHEDULE_WINDOW = (timedelta(days=-3), timedelta(days=21))
#: Bars began arriving on different days here; before it, one bulk fetch.
SCHEDULE_FROM = date(2025, 7, 16)
FILL_PAIR_PP = 0.05
SEAM_MIN_SYMBOLS = 20
#: Days whose bars were filled after the fact (2026-09-25), each with the day
#: after it, where the pair's gaps cancel.
FILL_PAIRS = {
    "*": (date(2026, 8, 28), date(2026, 8, 31)),
    "DXCM": (date(2026, 9, 22), date(2026, 9, 23)),
    "MGM": (date(2026, 9, 22), date(2026, 9, 23)),
}
#: The seam bound counts dividends up to the outage refill, the as-of date of
#: the segment after the 2025-07-16 seam.
REFILL_AS_OF = date(2026, 8, 10)


def load(con):
    bars = pd.read_sql(
        """SELECT a.symbol, m.time::date AS d, m.close, m.served_open, m.served_high,
                  m.served_low, m.served_close, m.served_volume, m.fetched_at
           FROM market_data m JOIN assets a ON a.id = m.asset_id
           WHERE a.asset_class <> 'crypto' ORDER BY 1, 2""",
        con,
    )
    acts = pd.read_sql(
        """SELECT a.symbol, ca.ex_date, ca.kind, ca.value, ca.fetched_at
           FROM corporate_actions ca JOIN assets a ON a.id = ca.asset_id""",
        con,
    )
    actions = defaultdict(list)
    for r in acts.itertuples():
        actions[r.symbol].append(
            Action(r.ex_date, r.kind, r.value, r.fetched_at.to_pydatetime())
        )
    return bars, actions


def mismatches(g: pd.DataFrame, acts: list) -> tuple[list, dict]:
    """Days where engine 'total' and stored returns differ by >= TOLERANCE_PP,
    and each dividend's yield in the engine's raw basis."""
    served = [
        ServedBar(r.d, r.served_open, r.served_high, r.served_low, r.served_close,
                  r.served_volume, r.fetched_at.to_pydatetime())
        for r in g.itertuples()
    ]
    total = pd.Series([b.close for b in adjust(served, acts, "total")], index=g.d)
    raw = pd.Series([b.close for b in adjust(served, acts, "none")], index=g.d)
    stored = pd.Series(g.close.values, index=g.d)
    gap = (total.pct_change() - stored.pct_change()) * 100

    days = list(g.d)
    yields = {}
    for ex, amount in raw_dividends(acts):
        before = [d for d in days if d < ex]
        if before and raw[before[-1]]:
            yields[ex] = amount / raw[before[-1]] * 100
    found = [(d, float(x)) for d, x in gap.items() if abs(x) >= TOLERANCE_PP]
    return found, yields


def classify(symbol, found, yields, seams):
    pair = FILL_PAIRS.get(symbol, FILL_PAIRS["*"])
    by_day = dict(found)
    out = []
    for d, x in found:
        if d in yields and abs(x - yields[d]) <= DIVIDEND_MATCH_PP:
            kind = "dividend"
        elif (d in pair or d in FILL_PAIRS["*"]) and _cancels(by_day, pair, symbol):
            kind = "fill"
        elif d in seams and _matches_running_sum(x, d, yields):
            kind = "seam"
        elif d >= SCHEDULE_FROM and any(
            abs(abs(x) - y) <= SCHEDULE_MATCH_PP
            for ex, y in yields.items()
            if SCHEDULE_WINDOW[0] <= ex - d <= SCHEDULE_WINDOW[1]
        ):
            kind = "schedule"
        else:
            kind = "unexplained"
        out.append({"day": d.isoformat(), "gap_pp": round(x, 3), "kind": kind})
    return out


def _matches_running_sum(gap, day, yields):
    """
    Is the gap the combined adjustment of the window's first k dividends, for
    some k? Adjustments compound: k dividends move the price by
    1 - prod(1 - y), not sum(y). The plain sum over-predicts high yielders —
    CAG's nine dividends sum to 9.34pp and compound to 9.02pp, the gap seen.
    """
    remaining = 1.0
    for ex in sorted(e for e in yields if day <= e <= REFILL_AS_OF):
        remaining *= 1.0 - yields[ex] / 100.0
        if abs(gap - (1.0 - remaining) * 100.0) <= SEAM_MATCH_PP:
            return True
    return False


def _cancels(by_day, pair, symbol):
    pairs = [pair] + ([FILL_PAIRS["*"]] if pair != FILL_PAIRS["*"] else [])
    return any(
        a in by_day and b in by_day and abs(by_day[a] + by_day[b]) <= FILL_PAIR_PP
        for a, b in pairs
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--report", help="Write per-symbol verdicts as JSON here")
    args = parser.parse_args(argv)

    url = next(
        line.split("=", 1)[1].strip()
        for line in open(Path(__file__).resolve().parents[1] / ".env")
        if line.startswith("SYNC_DATABASE_URL")
    )
    con = psycopg2.connect(url)
    con.set_session(readonly=True)
    bars, actions = load(con)

    per_symbol = {}
    for symbol, g in bars.groupby("symbol"):
        g = g.reset_index(drop=True)
        uncovered = int(g.served_close.isna().sum())
        if uncovered:
            per_symbol[symbol] = {"bars": len(g), "uncovered": uncovered, "found": [], "yields": {}}
            continue
        found, yields = mismatches(g, actions.get(symbol, []))
        per_symbol[symbol] = {"bars": len(g), "uncovered": 0, "found": found, "yields": yields}

    day_counts = Counter(d for v in per_symbol.values() for d, _ in v["found"])
    seams = {d for d, n in day_counts.items() if n >= SEAM_MIN_SYMBOLS}

    verdicts = []
    for symbol, v in sorted(per_symbol.items()):
        days = classify(symbol, v["found"], v["yields"], seams)
        unexplained = [d for d in days if d["kind"] == "unexplained"]
        if v["uncovered"]:
            verdict = "legacy: incomplete coverage"
        elif unexplained:
            verdict = "fail: unexplained mismatch"
        else:
            verdict = "pass"
        verdicts.append({
            "symbol": symbol, "verdict": verdict, "bars": v["bars"],
            "uncovered": v["uncovered"],
            "explained": Counter(d["kind"] for d in days if d["kind"] != "unexplained"),
            "unexplained": unexplained,
        })

    print(f"seam dates (>= {SEAM_MIN_SYMBOLS} symbols): "
          + ", ".join(f"{d} ({day_counts[d]})" for d in sorted(seams)))
    print("verdicts:", dict(Counter(v["verdict"] for v in verdicts)))
    kinds = Counter()
    for v in verdicts:
        kinds.update(v["explained"])
    print("explained mismatch days:", dict(kinds),
          "| unexplained:", sum(len(v["unexplained"]) for v in verdicts))
    for v in verdicts:
        if v["verdict"] != "pass":
            detail = (f"{v['uncovered']} of {v['bars']} bars without served values"
                      if v["uncovered"] else
                      "; ".join(f"{d['day']} {d['gap_pp']:+}pp" for d in v["unexplained"][:6]))
            print(f"  {v['symbol']:8} {v['verdict']:30} {detail}")
    if args.report:
        Path(args.report).write_text(json.dumps(verdicts, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
