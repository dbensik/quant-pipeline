"""
core/options_surface.py

The archive of option-chain captures, and priced surfaces cached on disk.
Tier 2 phase 3 of research/option-pricing-plan-2026-10-08.md.

A surface costs 10-35 s of CPU (pricing/chains.py: de-americanised IV over a
few thousand contracts), so it is computed once and cached. The cache is only
as good as its key. A surface depends on:

    the capture file          (its bytes, hashed)
    the risk-free rate        (series, observation date, value)
    the dividend schedule     (the projected ex-dates and amounts used)
    the pricing code          (the source of every pricing/ module it runs)
    the pricing settings      (IV bounds, tree steps, every OPTIONS_* knob)

and all five go into the key. The code fingerprint is DERIVED by hashing the
source, never hand-bumped: a version constant someone forgets to bump serves
stale surfaces silently, while a hash recomputes even for a comment edit,
which is the safe direction.

One file per ticker-day, with the key inside it; a different key overwrites
it. Stale versions do not pile up.

THE RATE, AND WHY A SURFACE COMPUTED TODAY IS RECOMPUTED TOMORROW
    The rate is the ^IRX observation on or before the capture date. Today's
    observation is not stored until tomorrow morning (core/rates.py never
    stores an unfinished session), so a surface for today's capture uses
    yesterday's rate, and tomorrow its key changes and it is recomputed with
    the right one. That is the cache working, not a bug.

The realised-vol cone is NOT in the cached surface: it is computed from
stored price bars, which are not in the key, so the router builds it fresh
on every request.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Tuple

import numpy as np
import pandas as pd

from config import settings
from pricing.chains import price_chain, smile, term_structure
from pricing.dividends import Dividend, project_dividends

PRICING_SOURCES = ("chains.py", "binomial.py", "black_scholes.py", "dividends.py")
PRICING_SETTINGS = (
    "OPTIONS_IV_BOUNDS",
    "BINOMIAL_STEPS",
    "OPTIONS_MIN_BID",
    "OPTIONS_EXCLUDE_DTE_BELOW",
    "OPTIONS_FORWARD_PAIRS",
    "OPTIONS_FORWARD_TOL",
    "OPTIONS_FORWARD_MAX_PASSES",
    "OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS",
    "OPTIONS_STALE_TRADE_DAYS",
    "OPTIONS_DIVIDEND_HORIZON_DAYS",
)


def pricing_fingerprint(pricing_dir: Optional[Path] = None) -> str:
    """Hash of the pricing source and the settings that change its output."""
    pricing_dir = pricing_dir or Path(__file__).resolve().parents[1] / "pricing"
    h = hashlib.sha256()
    for name in PRICING_SOURCES:
        h.update(name.encode())
        h.update((pricing_dir / name).read_bytes())
    h.update(json.dumps({k: getattr(settings, k) for k in PRICING_SETTINGS}, sort_keys=True, default=str).encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CaptureFile:
    ticker: str
    day: date
    path: Path
    rows: int
    partial: bool


class ChainArchive:
    """The capture job's directory: <root>/<TICKER>/<YYYY-MM-DD>[.partial].parquet."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def captures(self) -> List[CaptureFile]:
        import pyarrow.parquet as pq

        out: List[CaptureFile] = []
        if not self.root.is_dir():
            return out
        for path in sorted(self.root.glob("*/*.parquet")):
            stem = path.name[: -len(".parquet")]
            partial = stem.endswith(".partial")
            day_text = stem[: -len(".partial")] if partial else stem
            try:
                day = date.fromisoformat(day_text)
            except ValueError:
                continue
            rows = pq.ParquetFile(path).metadata.num_rows
            out.append(CaptureFile(path.parent.name.upper(), day, path, rows, partial))
        return out

    def find(self, ticker: str, day: date) -> Optional[CaptureFile]:
        """The complete capture for a ticker-day; a partial one is not served."""
        path = self.root / ticker.upper() / f"{day.isoformat()}.parquet"
        if not path.is_file():
            return None
        import pyarrow.parquet as pq

        return CaptureFile(ticker.upper(), day, path, pq.ParquetFile(path).metadata.num_rows, False)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

class DividendHistory(Protocol):
    async def dividend_history(self, symbol: str, on_or_before: date) -> List[Tuple[date, float]]:
        """Ex-date and amount of every recorded dividend up to a date."""
        ...


@dataclass(frozen=True)
class SurfaceInputs:
    capture: CaptureFile
    file_sha256: str
    rate_series: str
    rate_obs_date: date
    rate_quoted_pct: float
    rate_continuous: float
    rate_stale: bool
    dividends: Tuple[Dividend, ...]
    code: str

    def key(self) -> str:
        payload = {
            "ticker": self.capture.ticker,
            "day": self.capture.day.isoformat(),
            "file": self.file_sha256,
            "rate": [self.rate_series, self.rate_obs_date.isoformat(), repr(self.rate_quoted_pct)],
            "dividends": [[d.ex_date.isoformat(), repr(d.amount), d.projected] for d in self.dividends],
            "code": self.code,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def projected_dividends(history: Sequence[Tuple[date, float]], day: date) -> Tuple[Dividend, ...]:
    known = [(d, a) for d, a in history if d <= day]
    return tuple(project_dividends(known, day, day + timedelta(days=settings.OPTIONS_DIVIDEND_HORIZON_DAYS)))


# ---------------------------------------------------------------------------
# The surface payload (JSON-safe)
# ---------------------------------------------------------------------------

def json_safe(value: Any) -> Any:
    """Recursively: numpy -> Python, NaN/Inf -> None, dates -> ISO strings."""
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return [json_safe(v) for v in value.tolist()]
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _records(frame: pd.DataFrame) -> List[Dict[str, Any]]:
    return json_safe(frame.to_dict(orient="records"))


def build_surface(frame: pd.DataFrame, inputs: SurfaceInputs) -> Dict[str, Any]:
    started = time.perf_counter()
    result = price_chain(frame, inputs.rate_continuous, list(inputs.dividends))
    ts = term_structure(result)
    smiles = {
        expiry: _records(smile(result.rows, expiry))
        for expiry in result.expiries["expiry"]
    } if not result.expiries.empty else {}
    payload = {
        "key": inputs.key(),
        "ticker": inputs.capture.ticker,
        "day": inputs.capture.day.isoformat(),
        "captured_at": result.rows.attrs["captured_at"].isoformat(),
        "rate": {
            "series": inputs.rate_series,
            "observation_date": inputs.rate_obs_date.isoformat(),
            "quoted_pct": inputs.rate_quoted_pct,
            "continuous": inputs.rate_continuous,
            "stale": inputs.rate_stale,
        },
        "dividends": [
            {"ex_date": d.ex_date.isoformat(), "amount": d.amount, "projected": d.projected}
            for d in inputs.dividends
        ],
        "quality": result.quality,
        "expiries": _records(result.expiries),
        "term_structure": _records(ts),
        "smiles": smiles,
        "compute_seconds": round(time.perf_counter() - started, 2),
        "computed_at": datetime.now().astimezone().isoformat(),
    }
    return json_safe(payload)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class SurfaceCache:
    """
    One JSON file per ticker-day under `root`, holding its key. A request
    whose key differs recomputes and overwrites. Writes are atomic (temp file
    then rename). A per-key lock makes concurrent misses share one compute;
    a global semaphore queues misses across ticker-days.
    """

    def __init__(self, root: Path, max_concurrent: int = settings.OPTIONS_SURFACE_MAX_CONCURRENT) -> None:
        self.root = Path(root)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._slots = asyncio.Semaphore(max_concurrent)

    def path(self, ticker: str, day: date) -> Path:
        return self.root / ticker.upper() / f"{day.isoformat()}.json"

    def read(self, ticker: str, day: date, key: str) -> Optional[Dict[str, Any]]:
        path = self.path(ticker, day)
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        return payload if payload.get("key") == key else None

    def write(self, ticker: str, day: date, payload: Dict[str, Any]) -> None:
        path = self.path(ticker, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, allow_nan=False))
        os.replace(tmp, path)

    async def get_or_compute(
        self,
        inputs: SurfaceInputs,
        compute: Callable[[], Dict[str, Any]],
        run_in_thread: Callable,
    ) -> Tuple[Dict[str, Any], bool]:
        """(payload, hit). `compute` is blocking; it runs via `run_in_thread`."""
        ticker, day, key = inputs.capture.ticker, inputs.capture.day, inputs.key()
        cached = self.read(ticker, day, key)
        if cached is not None:
            return cached, True
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            cached = self.read(ticker, day, key)  # another request filled it
            if cached is not None:
                return cached, True
            async with self._slots:
                payload = await run_in_thread(compute)
            self.write(ticker, day, payload)
        self._locks.pop(key, None)
        return payload, False


class NoRateError(LookupError):
    pass


async def surface_inputs(capture: CaptureFile, rates: Any, dividends: DividendHistory, code: str) -> SurfaceInputs:
    """Everything a surface depends on, read as of the capture date."""
    from core.rates import risk_free_rate

    rate = await risk_free_rate(rates, capture.day)
    if rate is None:
        raise NoRateError(f"No {settings.RISK_FREE_RATE_SERIES} observation on or before {capture.day}")
    history = await dividends.dividend_history(capture.ticker, capture.day)
    return SurfaceInputs(
        capture=capture,
        file_sha256=await asyncio.to_thread(file_sha256, capture.path),
        rate_series=rate.series,
        rate_obs_date=rate.obs_date,
        rate_quoted_pct=rate.quoted_pct,
        rate_continuous=rate.continuous,
        rate_stale=rate.stale,
        dividends=projected_dividends(history, capture.day),
        code=code,
    )


def compute_for(inputs: SurfaceInputs) -> Callable[[], Dict[str, Any]]:
    def compute() -> Dict[str, Any]:
        return build_surface(pd.read_parquet(inputs.capture.path), inputs)
    return compute
