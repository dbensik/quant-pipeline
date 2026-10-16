"""
core/desk_pack.py

A sourced data pack for one name, for the Bensik & Co. Equity Research Desk
(decision 39, 2026-10-16). The desk gathered its own data on every run —
EDGAR full-text searches, figures transcribed by hand, provenance traced
afterwards — and one run's bad figure reached all five analysts. The pipeline
already holds most of that data, refreshed daily from the same primary source
with accession numbers attached, so the desk reads it from here instead.

Everything is POINT IN TIME: facts filed on or before `as_of`, prices on or
before it, the DCF as of it. A pack built for a past date shows only what was
knowable then.

The pure parts — price statistics, the sector screen, the options-implied
probability — live here without a database so they can be tested directly;
`build_pack` does the reading. scripts/desk_pack.py is the command.

SEAT ROUTING (pack.json `seats`): the five blind analysts get facts,
statements, prices and the screen — NOT the DCF. A shared valuation would
anchor all five to one view, and independence is the desk's whole product.
The DCF, its history and the options surface go to the valuation-sanity seat;
refutation sees everything.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

#: Sectors the desk refuses outright (its Step 2: utilities, IPPs, ISOs and
#: merchant power are, or plausibly are, clients of Danny's employer). A sector
#: is only a first pass: an ETRM software vendor is not a sector, so the desk
#: still makes the Step 2 judgement on every name the screen passes.
REFUSED_SECTORS = {"Utilities"}

#: What the pack does not cover, stated in every pack so no seat assumes it.
NOT_COVERED = [
    "executive compensation (proxy statements, DEF 14A)",
    "insider transactions (Form 4)",
    "competitive position, market share, pricing power",
    "risk factors and MD&A text (10-K Items 1A and 7)",
    "segment data (XBRL dimensions are not in SEC company facts)",
    "financial concepts beyond the 80 the DCF reads (SEC reports ~560 for a large filer)",
]

#: Travels with the value/price history wherever it goes (decision 21).
VALUE_PRICE_CAVEAT = (
    "Value/price did not rank forward returns: 2010-2026, 20,507 point-in-time "
    "valuations of 386 names, 12-month IC 0.009 (t 0.40). A reading tool, not a "
    "signal (decisions 21, 26)."
)

SEATS = {
    "analysts": ["screen.json", "facts.json", "statements.json", "prices.json"],
    "valuation_sanity": [
        "screen.json",
        "prices.json",
        "valuation.json",
        "valuation_history.json",
        "options.json",
    ],
    "refutation": "all",
}


# ---------------------------------------------------------------------------
# Screen (desk Step 2, first pass)
# ---------------------------------------------------------------------------


def screen(symbol: str, asset_class: Optional[str], sector: Optional[str]) -> Dict[str, Any]:
    """The sector screen. `refuse` True means the desk must not research it."""
    refuse = sector in REFUSED_SECTORS
    return {
        "symbol": symbol,
        "asset_class": asset_class,
        "sector": sector,
        "refuse": refuse,
        "reason": (
            f"sector {sector}: refused by the desk's conflict rule (Step 2)"
            if refuse
            else "not refused by sector — the desk's Step 2 judgement is still required "
            "(ETRM vendors, merchant power and ISO counterparties are not a sector)"
        ),
    }


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


def price_stats(
    days: Sequence[date],
    total: Sequence[float],
    split: Sequence[float],
    volume: Sequence[float],
    bars_per_year: int,
) -> Dict[str, Any]:
    """
    Summary statistics from total-return closes (returns, volatility,
    drawdowns) and split-adjusted closes (the quoted price, the 52-week range).
    Returns over a window are None when the series does not reach back that far.
    """
    if not days:
        return {"bars": 0}
    last_day = days[-1]

    def back(n_days: int) -> Optional[int]:
        target = last_day - timedelta(days=n_days)
        if days[0] > target:
            return None
        lo, hi = 0, len(days) - 1
        while lo < hi:  # last index with day <= target
            mid = (lo + hi + 1) // 2
            if days[mid] <= target:
                lo = mid
            else:
                hi = mid - 1
        return lo

    returns: Dict[str, Optional[float]] = {}
    for label, n in (
        ("1m", 30),
        ("3m", 91),
        ("1y", 365),
        ("3y", 1095),
        ("5y", 1826),
        ("10y", 3652),
    ):
        i = back(n)
        returns[label] = None if i is None else total[-1] / total[i] - 1

    def window(n_days: int) -> int:
        i = back(n_days)
        return 0 if i is None else i

    def max_drawdown(start: int) -> float:
        peak, worst = total[start], 0.0
        for value in total[start:]:
            peak = max(peak, value)
            worst = min(worst, value / peak - 1)
        return worst

    def vol(start: int) -> Optional[float]:
        r = [total[i] / total[i - 1] - 1 for i in range(start + 1, len(total))]
        if len(r) < 2:
            return None
        mean = sum(r) / len(r)
        var = sum((x - mean) ** 2 for x in r) / (len(r) - 1)
        return math.sqrt(var) * math.sqrt(bars_per_year)

    y1 = window(365)
    m3 = window(91)
    return {
        "bars": len(days),
        "first_day": days[0].isoformat(),
        "last_day": last_day.isoformat(),
        "last_close": split[-1],
        "high_52w": max(split[y1:]),
        "low_52w": min(split[y1:]),
        "total_return": returns,
        "volatility_1y": vol(y1),
        "max_drawdown_1y": max_drawdown(y1),
        "max_drawdown_full": max_drawdown(0),
        "avg_dollar_volume_3m": sum(split[i] * volume[i] for i in range(m3, len(days)))
        / max(1, len(days) - m3),
        "bars_per_year": bars_per_year,
    }


def month_end_closes(days: Sequence[date], closes: Sequence[float]) -> List[Dict[str, Any]]:
    """The last close of each month — the series, small enough to ship."""
    out: Dict[Tuple[int, int], Tuple[date, float]] = {}
    for d, c in zip(days, closes, strict=True):
        out[(d.year, d.month)] = (d, c)
    return [{"day": d.isoformat(), "close": c} for d, c in out.values()]


# ---------------------------------------------------------------------------
# Options-implied probability (desk Step 1.5, Check 1)
# ---------------------------------------------------------------------------


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _black76_call(forward: float, strike: float, t: float, sigma: float) -> float:
    """Undiscounted Black-76 call value."""
    if sigma <= 0 or t <= 0:
        return max(forward - strike, 0.0)
    sd = sigma * math.sqrt(t)
    d1 = (math.log(forward / strike) + 0.5 * sd * sd) / sd
    return forward * _norm_cdf(d1) - strike * _norm_cdf(d1 - sd)


def smile_iv(smile: Sequence[Dict[str, Any]], forward: float, strike: float) -> Optional[float]:
    """
    Implied vol at `strike`, interpolated linearly in log-moneyness over the
    OUT-of-the-money quotes (puts below the forward, calls above) — the liquid
    side. Flat beyond the outermost quote rather than extrapolated.
    """
    points = sorted(
        (math.log(q["strike"] / forward), q["iv"])
        for q in smile
        if q.get("iv") is not None
        and q["iv"] > 0
        and (
            (q["strike"] < forward and not q["is_call"])
            or (q["strike"] >= forward and q["is_call"])
        )
    )
    if not points:
        return None
    x = math.log(strike / forward)
    if x <= points[0][0]:
        return points[0][1]
    if x >= points[-1][0]:
        return points[-1][1]
    for (x0, v0), (x1, v1) in zip(points, points[1:], strict=False):
        if x0 <= x <= x1:
            return v0 if x1 == x0 else v0 + (v1 - v0) * (x - x0) / (x1 - x0)
    return None


def implied_probability_above(
    smile: Sequence[Dict[str, Any]], forward: float, t: float, strike: float
) -> Optional[float]:
    """
    Risk-neutral P(S_T > strike) from the smile: minus the slope of the
    undiscounted call price in strike, by central difference, each call priced
    at its OWN interpolated vol. With a flat smile this is N(d2); with skew it
    differs, which is the point — N(d2) at one vol ignores the smile's slope.
    """
    h = strike * 1e-4
    lo, hi = smile_iv(smile, forward, strike - h), smile_iv(smile, forward, strike + h)
    if lo is None or hi is None:
        return None
    p = -(_black76_call(forward, strike + h, t, hi) - _black76_call(forward, strike - h, t, lo)) / (
        2 * h
    )
    return min(1.0, max(0.0, p))


def options_view(
    surface: Optional[Dict[str, Any]],
    trigger: Optional[float] = None,
    by: Optional[date] = None,
    above: bool = True,
) -> Dict[str, Any]:
    """
    The options picture for the valuation seat and the desk's gate: the ATM
    term structure and, given a trigger and a date, the implied probability on
    the first expiry on or after that date. A RISK-NEUTRAL probability — the
    baseline the desk's own number must beat, not a forecast.
    """
    if surface is None:
        return {"available": False, "reason": "no archived option chain for this ticker"}
    out: Dict[str, Any] = {
        "available": True,
        "capture_day": surface.get("day"),
        "captured_at": surface.get("captured_at"),
        "rate": surface.get("rate"),
        "term_structure": surface.get("term_structure"),
    }
    if trigger is None or by is None:
        return out
    expiries = sorted(surface.get("expiries", []), key=lambda e: e["expiry"])
    chosen = next((e for e in expiries if date.fromisoformat(e["expiry"]) >= by), None)
    if chosen is None:
        out["trigger"] = {
            "price": trigger,
            "by": by.isoformat(),
            "probability": None,
            "reason": "no listed expiry on or after that date",
        }
        return out
    smile = surface.get("smiles", {}).get(chosen["expiry"], [])
    p_above = implied_probability_above(smile, chosen["forward"], chosen["T"], trigger)
    out["trigger"] = {
        "price": trigger,
        "by": by.isoformat(),
        "direction": "above" if above else "below",
        "expiry_used": chosen["expiry"],
        "forward": chosen["forward"],
        "probability": None if p_above is None else (p_above if above else 1 - p_above),
        "measure": "risk-neutral",
        "desk_band": "REFRAME if outside 0.15-0.85 (desk Step 1.5, Check 1)",
    }
    return out


# ---------------------------------------------------------------------------
# Writing the pack
# ---------------------------------------------------------------------------


def edgar_url(cik: int, accn: str) -> str:
    """The filing's directory on EDGAR, where the desk verifies an accession."""
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accn.replace('-', '')}/"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=1, sort_keys=True, default=_default) + "\n")


def _default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


@dataclass
class PackResult:
    status: str  # "complete" | "refused-conflict" | "not-in-registry"
    out_dir: Path
    files: Dict[str, str]  # name -> sha256


def write_manifest(
    out_dir: Path,
    symbol: str,
    as_of: date,
    status: str,
    commit: Optional[str],
    notes: Dict[str, Any],
) -> PackResult:
    """pack.json: what was built, from which pipeline commit, every file's sha256."""
    files = {p.name: sha256_of(p) for p in sorted(out_dir.glob("*.json")) if p.name != "pack.json"}
    write_json(
        out_dir / "pack.json",
        {
            "symbol": symbol,
            "as_of": as_of,
            "status": status,
            "generated_at": datetime.now(timezone.utc),
            "pipeline_commit": commit,
            "files": files,
            "seats": SEATS,
            "not_covered": NOT_COVERED,
            "notes": notes,
        },
    )
    return PackResult(status=status, out_dir=out_dir, files=files)
