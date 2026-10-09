"""
api/routers/options.py without a database: the archive and cache point at a
temp directory holding the 198-row SPY fixture, and the rate and dividend
repositories are fakes satisfying their Protocols.
"""

import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from api.main import app
from api.routers import options as R
from core.options_surface import ChainArchive, SurfaceCache
from core.rates import RateObservation

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "spy_chain_2026-10-07_slice.parquet"


class FakeRates:
    def __init__(self, rows):
        self.rows = {o.obs_date: o for o in rows}

    async def insert(self, observations):
        return 0

    async def newest_date(self, series):
        return max(self.rows) if self.rows else None

    async def latest_on_or_before(self, series, as_of):
        days = [d for d in self.rows if d <= as_of]
        return self.rows[max(days)] if days else None


class FakeDividends:
    def __init__(self):
        self.calls = []

    async def dividend_history(self, symbol, on_or_before):
        self.calls.append((symbol, on_or_before))
        hist = [(date(2025, 12, 19), 1.993), (date(2026, 3, 20), 1.797), (date(2026, 6, 18), 1.904), (date(2026, 9, 18), 1.889)]
        return [(d, a) for d, a in hist if d <= on_or_before]


OBS = RateObservation("^IRX", date(2026, 10, 7), 4.037, "yfinance", datetime(2026, 10, 8, tzinfo=timezone.utc))


@pytest.fixture
def options_client(client, tmp_path):
    (tmp_path / "chains" / "SPY").mkdir(parents=True)
    shutil.copy(FIXTURE, tmp_path / "chains" / "SPY" / "2026-10-07.parquet")
    cache = SurfaceCache(tmp_path / "surfaces")
    divs = FakeDividends()
    app.dependency_overrides[R.get_chain_archive] = lambda: ChainArchive(tmp_path / "chains")
    app.dependency_overrides[R.get_surface_cache] = lambda: cache
    app.dependency_overrides[R.get_rate_repo] = lambda: FakeRates([OBS])
    app.dependency_overrides[R.get_dividend_repo] = lambda: divs
    app.dependency_overrides[R.get_pricing_fingerprint] = lambda: "test-code"
    client.divs = divs
    client.cache_dir = tmp_path / "surfaces"
    return client


def test_archive(options_client):
    body = options_client.get("/api/v1/options/archive").json()
    assert body["total_rows"] == 198
    assert body["tickers"] == [{"ticker": "SPY", "captures": [{"day": "2026-10-07", "rows": 198, "partial": False}]}]


def test_surface_computes_then_hits_the_cache(options_client):
    first = options_client.get("/api/v1/options/surface", params={"ticker": "spy", "day": "2026-10-07"})
    assert first.status_code == 200, first.text
    a = first.json()
    assert a["cache_hit"] is False
    assert a["rate"]["observation_date"] == "2026-10-07" and a["rate"]["quoted_pct"] == 4.037
    assert [d["ex_date"] for d in a["dividends"]][0] == "2026-12-18"
    assert len(a["term_structure"]) == 2 and all(t["atm_iv"] for t in a["term_structure"])
    assert sum(len(v) for v in a["smiles"].values()) > 50
    # Dividends are read point-in-time: up to the capture date.
    assert options_client.divs.calls[-1] == ("SPY", date(2026, 10, 7))
    b = options_client.get("/api/v1/options/surface", params={"ticker": "SPY"}).json()  # default: latest day
    assert b["cache_hit"] is True and b["day"] == "2026-10-07"
    assert b["term_structure"] == a["term_structure"]
    assert [p.name for p in (options_client.cache_dir / "SPY").iterdir()] == ["2026-10-07.json"]


def test_surface_without_bars_returns_no_cone_and_says_why(options_client):
    # The API fixture repo has no SPY bars: the surface must still come back.
    body = options_client.get("/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-07"}).json()
    assert body["cone"] is None and "not a registered asset" in body["cone_reason"]


def test_surface_is_valid_json(options_client):
    text = options_client.get("/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-07"}).text
    json.loads(text, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))


def test_surface_errors(options_client):
    assert options_client.get("/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-06"}).status_code == 404
    assert options_client.get("/api/v1/options/surface", params={"ticker": "QQQ"}).status_code == 404
    app.dependency_overrides[R.get_rate_repo] = lambda: FakeRates([])
    r = options_client.get("/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-07"})
    assert r.status_code == 409 and "^IRX" in r.json()["detail"]


def test_price_european_models_agree(options_client):
    body = options_client.post("/api/v1/options/price", json=dict(
        spot=100, strike=105, expiry_years=0.5, rate=0.04, dividend_yield=0.01, sigma=0.25, right="call", mc_paths=200_000,
    )).json()
    quotes = {q["model"]: q for q in body["quotes"]}
    assert set(quotes) == {"black_scholes", "crr_tree", "monte_carlo"}
    bsm = quotes["black_scholes"]["price"]
    assert abs(quotes["crr_tree"]["price"] - bsm) < 1e-3
    assert abs(quotes["monte_carlo"]["price"] - bsm) < 3 * quotes["monte_carlo"]["std_error"]
    assert abs(quotes["crr_tree"]["delta"] - quotes["black_scholes"]["delta"]) < 1e-3


def test_price_american_is_the_tree_only_with_its_premium(options_client):
    body = options_client.post("/api/v1/options/price", json=dict(
        spot=100, strike=120, expiry_years=0.5, rate=0.05, sigma=0.25, right="put", style="american",
    )).json()
    assert [q["model"] for q in body["quotes"]] == ["crr_tree"]
    assert body["quotes"][0]["early_exercise_premium"] > 0.1


def test_price_validates(options_client):
    assert options_client.post("/api/v1/options/price", json=dict(
        spot=-1, strike=100, expiry_years=0.5, rate=0.04, sigma=0.2, right="call")).status_code == 422


def test_pricers(options_client):
    ids = [p["id"] for p in options_client.get("/api/v1/options/pricers").json()]
    assert ids == ["black_scholes", "crr_tree", "monte_carlo"]


def test_realised_vol(options_client):
    r = options_client.get("/api/v1/options/realised-vol", params={"symbol": "MSFT", "window": 20, "as_of": "2025-01-31"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body["estimators"]) == {"close_to_close", "parkinson", "garman_klass", "rogers_satchell", "yang_zhang"}
    assert [w["window"] for w in body["cone"]["windows"]] == [10, 20, 60, 120]
    for w in body["cone"]["windows"]:
        assert w["min"] <= w["p25"] <= w["median"] <= w["p75"] <= w["max"]
    assert options_client.get("/api/v1/options/realised-vol", params={"symbol": "BTC-USD"}).status_code == 422
    assert options_client.get("/api/v1/options/realised-vol", params={"symbol": "NOPE"}).status_code == 404
