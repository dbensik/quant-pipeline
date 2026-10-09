"""
core/options_surface.py: the cache key, the cache, the archive reader, and the
realised-vol cone's point-in-time rule.

The key is what keeps a cached surface honest, so each input it depends on is
changed ALONE and the key must change. A surface is expensive (10-35 s), so a
hit must not compute, and concurrent misses must compute once.
"""

import asyncio
import json
import shutil
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from config import settings
from core.models import OHLCV, Asset, MarketDataRecord, Timestamp
from core.options_surface import (
    ChainArchive,
    SurfaceCache,
    SurfaceInputs,
    build_surface,
    file_sha256,
    json_safe,
    pricing_fingerprint,
)
from pricing.dividends import Dividend

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "spy_chain_2026-10-07_slice.parquet"
DAY = date(2026, 10, 7)


@pytest.fixture
def archive_dir(tmp_path):
    (tmp_path / "SPY").mkdir()
    shutil.copy(FIXTURE, tmp_path / "SPY" / "2026-10-07.parquet")
    shutil.copy(FIXTURE, tmp_path / "SPY" / "2026-10-08.partial.parquet")
    return tmp_path


@pytest.fixture
def inputs(archive_dir):
    capture = ChainArchive(archive_dir).find("SPY", DAY)
    return SurfaceInputs(
        capture=capture,
        file_sha256=file_sha256(capture.path),
        rate_series="^IRX",
        rate_obs_date=DAY,
        rate_quoted_pct=4.037,
        rate_continuous=0.041141,
        rate_stale=False,
        dividends=(Dividend(date(2026, 12, 18), 1.889, True),),
        code="code-v1",
    )


# --- the key ----------------------------------------------------------------

@pytest.mark.parametrize(
    "change",
    [
        dict(file_sha256="0" * 64),
        dict(rate_obs_date=date(2026, 10, 6)),
        dict(rate_quoted_pct=4.038),
        dict(dividends=(Dividend(date(2026, 12, 18), 1.99, True),)),
        dict(dividends=(Dividend(date(2026, 12, 19), 1.889, True),)),
        dict(dividends=()),
        dict(code="code-v2"),
    ],
)
def test_every_input_changes_the_key(inputs, change):
    assert replace(inputs, **change).key() != inputs.key()


def test_identical_inputs_give_the_same_key(inputs):
    assert replace(inputs).key() == inputs.key()


def test_fingerprint_follows_the_pricing_source(tmp_path):
    src = Path(__file__).resolve().parents[2] / "pricing"
    for name in ("chains.py", "binomial.py", "black_scholes.py", "dividends.py"):
        shutil.copy(src / name, tmp_path / name)
    before = pricing_fingerprint(tmp_path)
    assert pricing_fingerprint(tmp_path) == before
    with open(tmp_path / "binomial.py", "a") as fh:
        fh.write("\n# a comment is enough: recomputing is the safe direction\n")
    assert pricing_fingerprint(tmp_path) != before


@pytest.mark.parametrize("name,value", [("BINOMIAL_STEPS", 400), ("OPTIONS_MIN_BID", 0.05), ("OPTIONS_DIVIDEND_HORIZON_DAYS", 400)])
def test_fingerprint_follows_the_pricing_settings(monkeypatch, name, value):
    before = pricing_fingerprint()
    monkeypatch.setattr(settings, name, value)
    assert pricing_fingerprint() != before


# --- the cache ----------------------------------------------------------------

async def _sync(fn):
    return fn()


@pytest.mark.asyncio
async def test_a_hit_does_not_compute(tmp_path, inputs):
    cache, calls = SurfaceCache(tmp_path), []

    def compute():
        calls.append(1)
        return {"key": inputs.key(), "value": 1}

    first, hit1 = await cache.get_or_compute(inputs, compute, _sync)
    second, hit2 = await cache.get_or_compute(inputs, compute, _sync)
    assert (hit1, hit2, len(calls)) == (False, True, 1)
    assert first == second


@pytest.mark.asyncio
async def test_a_new_key_recomputes_and_overwrites_the_one_file(tmp_path, inputs):
    root = tmp_path / "cache"
    cache, calls = SurfaceCache(root), []

    def make(inp):
        def compute():
            calls.append(inp.key())
            return {"key": inp.key()}
        return compute

    await cache.get_or_compute(inputs, make(inputs), _sync)
    other = replace(inputs, rate_quoted_pct=4.1)
    payload, hit = await cache.get_or_compute(other, make(other), _sync)
    assert not hit and len(calls) == 2 and payload["key"] == other.key()
    assert [p.name for p in (root / "SPY").iterdir()] == ["2026-10-07.json"]
    assert cache.read("SPY", DAY, inputs.key()) is None


@pytest.mark.asyncio
async def test_concurrent_misses_compute_once(tmp_path, inputs):
    cache, calls = SurfaceCache(tmp_path), []

    def compute():
        calls.append(1)
        return {"key": inputs.key()}

    async def slow(fn):
        await asyncio.sleep(0.05)
        return fn()

    results = await asyncio.gather(*(cache.get_or_compute(inputs, compute, slow) for _ in range(4)))
    assert len(calls) == 1
    assert sorted(hit for _, hit in results) == [False, True, True, True]


def test_the_cache_refuses_to_write_invalid_json(tmp_path):
    with pytest.raises(ValueError):
        SurfaceCache(tmp_path).write("SPY", DAY, {"key": "k", "x": float("nan")})


def test_json_safe():
    out = json_safe({"a": np.float64("nan"), "b": [np.int64(3), float("inf")], "c": date(2026, 1, 2)})
    assert out == {"a": None, "b": [3, None], "c": "2026-01-02"}


# --- the archive -----------------------------------------------------------------

def test_archive_lists_captures_and_serves_only_complete_ones(archive_dir):
    archive = ChainArchive(archive_dir)
    caps = archive.captures()
    assert [(c.ticker, c.day, c.partial, c.rows) for c in caps] == [
        ("SPY", DAY, False, 198),
        ("SPY", date(2026, 10, 8), True, 198),
    ]
    assert archive.find("SPY", date(2026, 10, 8)) is None
    assert archive.find("spy", DAY).rows == 198
    assert ChainArchive(archive_dir / "missing").captures() == []


# --- the payload -----------------------------------------------------------------

def test_surface_payload_is_valid_json_with_nulls_for_nan(inputs):
    import pandas as pd

    frame = pd.read_parquet(inputs.capture.path)
    # The real slice happens to carry no NaN, so plant one where the payload
    # will carry it: Yahoo's IV on an out-of-the-money call at the top strike.
    top = frame[(frame["right"] == "C") & (frame["expiry"] == "2026-11-20")]["strike"].idxmax()
    frame.loc[top, "impliedVolatility"] = float("nan")
    payload = build_surface(frame, inputs)
    json.dumps(payload, allow_nan=False)  # raises on NaN
    assert payload["key"] == inputs.key()
    assert len(payload["term_structure"]) == 2
    assert sum(len(v) for v in payload["smiles"].values()) > 50
    planted = [p for p in payload["smiles"]["2026-11-20"] if p["strike"] == frame.loc[top, "strike"] and p["is_call"]]
    assert planted and planted[0]["vendor_iv"] is None


# --- the cone: point in time -----------------------------------------------------

class BarsRepo:
    def __init__(self, closes, start=datetime(2015, 1, 2, tzinfo=timezone.utc)):
        rng = np.random.default_rng(5)
        self.asset = Asset(symbol="XYZ", asset_class="equity", source="yfinance", price_basis="served")
        self.records = []
        for i, c in enumerate(closes):
            o = c * np.exp(rng.normal(0, 0.003))
            hi, lo = max(o, c) * 1.004, min(o, c) * 0.996
            ts = Timestamp(utc=datetime.fromordinal(start.toordinal() + i).replace(tzinfo=timezone.utc))
            self.records.append(MarketDataRecord(asset=self.asset, ohlcv=OHLCV(open=o, high=hi, low=lo, close=c, volume=1.0, timestamp=ts)))

    async def find_asset(self, symbol, asset_class=None):
        return self.asset if symbol == "XYZ" else None

    async def fetch_range(self, symbol, asset_class, start, end, source=None, adjust="total"):
        return [r for r in self.records if start <= r.ohlcv.timestamp.utc <= end]


@pytest.mark.asyncio
async def test_cone_ignores_bars_after_the_capture_date():
    from api.routers.options import build_cone

    closes = 100 * np.exp(np.cumsum(np.random.default_rng(9).normal(0, 0.012, 900)))
    full = BarsRepo(closes)
    as_of = full.records[600].ohlcv.timestamp.utc.date()
    # Bars after the capture date, wildly volatile: must not leak into the cone.
    wild = closes.copy()
    wild[601:] *= np.exp(np.random.default_rng(1).normal(0, 0.2, 299))
    a = await build_cone(full, "XYZ", as_of)
    b = await build_cone(BarsRepo(wild), "XYZ", as_of)
    assert a == b
    assert a.bars == 601
