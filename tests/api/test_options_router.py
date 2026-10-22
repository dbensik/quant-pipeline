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
        hist = [
            (date(2025, 12, 19), 1.993),
            (date(2026, 3, 20), 1.797),
            (date(2026, 6, 18), 1.904),
            (date(2026, 9, 18), 1.889),
        ]
        return [(d, a) for d, a in hist if d <= on_or_before]


OBS = RateObservation(
    "^IRX", date(2026, 10, 7), 4.037, "yfinance", datetime(2026, 10, 8, tzinfo=timezone.utc)
)


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
    assert body["tickers"] == [
        {"ticker": "SPY", "captures": [{"day": "2026-10-07", "rows": 198, "partial": False}]}
    ]


def test_surface_computes_then_hits_the_cache(options_client):
    first = options_client.get(
        "/api/v1/options/surface", params={"ticker": "spy", "day": "2026-10-07"}
    )
    assert first.status_code == 200, first.text
    a = first.json()
    assert a["cache_hit"] is False
    assert a["rate"]["observation_date"] == "2026-10-07" and a["rate"]["quoted_pct"] == 4.037
    assert [d["ex_date"] for d in a["dividends"]][0] == "2026-12-18"
    assert len(a["term_structure"]) == 2 and all(t["atm_iv"] for t in a["term_structure"])
    assert sum(len(v) for v in a["smiles"].values()) > 50
    # Dividends are read point-in-time: up to the capture date.
    assert options_client.divs.calls[-1] == ("SPY", date(2026, 10, 7))
    b = options_client.get(
        "/api/v1/options/surface", params={"ticker": "SPY"}
    ).json()  # default: latest day
    assert b["cache_hit"] is True and b["day"] == "2026-10-07"
    assert b["term_structure"] == a["term_structure"]
    assert [p.name for p in (options_client.cache_dir / "SPY").iterdir()] == ["2026-10-07.json"]


def test_surface_without_bars_returns_no_cone_and_says_why(options_client):
    # The API fixture repo has no SPY bars: the surface must still come back.
    body = options_client.get(
        "/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-07"}
    ).json()
    assert body["cone"] is None and "not a registered asset" in body["cone_reason"]


def test_surface_is_valid_json(options_client):
    text = options_client.get(
        "/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-07"}
    ).text
    json.loads(text, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))


def test_surface_errors(options_client):
    assert (
        options_client.get(
            "/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-06"}
        ).status_code
        == 404
    )
    assert (
        options_client.get("/api/v1/options/surface", params={"ticker": "QQQ"}).status_code == 404
    )
    app.dependency_overrides[R.get_rate_repo] = lambda: FakeRates([])
    r = options_client.get("/api/v1/options/surface", params={"ticker": "SPY", "day": "2026-10-07"})
    assert r.status_code == 409 and "^IRX" in r.json()["detail"]


def test_price_european_models_agree(options_client):
    body = options_client.post(
        "/api/v1/options/price",
        json=dict(
            spot=100,
            strike=105,
            expiry_years=0.5,
            rate=0.04,
            dividend_yield=0.01,
            sigma=0.25,
            right="call",
            mc_paths=200_000,
        ),
    ).json()
    quotes = {q["model"]: q for q in body["quotes"]}
    assert set(quotes) == {"black_scholes", "crr_tree", "monte_carlo"}
    bsm = quotes["black_scholes"]["price"]
    assert abs(quotes["crr_tree"]["price"] - bsm) < 1e-3
    assert abs(quotes["monte_carlo"]["price"] - bsm) < 3 * quotes["monte_carlo"]["std_error"]
    assert abs(quotes["crr_tree"]["delta"] - quotes["black_scholes"]["delta"]) < 1e-3


def test_price_american_is_the_tree_only_with_its_premium(options_client):
    body = options_client.post(
        "/api/v1/options/price",
        json=dict(
            spot=100,
            strike=120,
            expiry_years=0.5,
            rate=0.05,
            sigma=0.25,
            right="put",
            style="american",
        ),
    ).json()
    assert [q["model"] for q in body["quotes"]] == ["crr_tree"]
    assert body["quotes"][0]["early_exercise_premium"] > 0.1


def test_price_validates(options_client):
    assert (
        options_client.post(
            "/api/v1/options/price",
            json=dict(spot=-1, strike=100, expiry_years=0.5, rate=0.04, sigma=0.2, right="call"),
        ).status_code
        == 422
    )


def test_pricers(options_client):
    ids = [p["id"] for p in options_client.get("/api/v1/options/pricers").json()]
    assert ids == ["black_scholes", "crr_tree", "monte_carlo"]


def test_realised_vol(options_client):
    r = options_client.get(
        "/api/v1/options/realised-vol",
        params={"symbol": "MSFT", "window": 20, "as_of": "2025-01-31"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body["estimators"]) == {
        "close_to_close",
        "parkinson",
        "garman_klass",
        "rogers_satchell",
        "yang_zhang",
    }
    assert [w["window"] for w in body["cone"]["windows"]] == [10, 20, 60, 120]
    for w in body["cone"]["windows"]:
        assert w["min"] <= w["p25"] <= w["median"] <= w["p75"] <= w["max"]
    assert (
        options_client.get("/api/v1/options/realised-vol", params={"symbol": "BTC-USD"}).status_code
        == 422
    )
    assert (
        options_client.get("/api/v1/options/realised-vol", params={"symbol": "NOPE"}).status_code
        == 404
    )


# -- /quotes: the payoff page's premiums (decision 54) -------------------------


def test_quotes_default_to_the_expiry_nearest_a_month(options_client):
    body = options_client.get("/api/v1/options/quotes", params={"ticker": "spy"}).json()
    assert body["ticker"] == "SPY" and body["day"] == "2026-10-07"
    assert body["expiries"] == ["2026-10-16", "2026-11-20"]
    # 9 days against 44: 44 is nearer 30.
    assert body["expiry"] == "2026-11-20"
    assert body["spot"] == 777.22


def test_quotes_are_the_captured_bid_and_ask_for_both_rights(options_client):
    body = options_client.get(
        "/api/v1/options/quotes", params={"ticker": "SPY", "expiry": "2026-11-20"}
    ).json()
    by_strike = {q["strike"]: q for q in body["quotes"]}
    assert by_strike[770.0] == {
        "strike": 770.0,
        "call_bid": 21.03,
        "call_ask": 21.13,
        "put_bid": 10.13,
        "put_ask": 10.15,
    }
    # A strike quoted for calls only has no put quote, not a zero one.
    assert by_strike[792.0]["put_bid"] is None and by_strike[792.0]["call_ask"] > 0
    strikes = [q["strike"] for q in body["quotes"]]
    assert strikes == sorted(strikes) and len(strikes) == len(set(strikes)) == 50


def test_quotes_flag_expiries_near_a_projected_ex_date(options_client):
    get = options_client.get
    # The fixture history projects the next ex-date to 2026-12-18 (median gap
    # 91 days after 09-18): neither captured expiry is within 5 days of it.
    assert (
        get("/api/v1/options/quotes", params={"ticker": "SPY"}).json()["dividend_date_uncertain"]
        == []
    )
    assert options_client.divs.calls[-1] == ("SPY", date(2026, 10, 7))

    class NearNovember(FakeDividends):
        async def dividend_history(self, symbol, on_or_before):
            # Gaps of 91 days, the last 08-19: projected 11-18, 2 days before 11-20.
            return [(date(2026, 2, 18), 1.0), (date(2026, 5, 20), 1.0), (date(2026, 8, 19), 1.0)]

    app.dependency_overrides[R.get_dividend_repo] = lambda: NearNovember()
    body = get("/api/v1/options/quotes", params={"ticker": "SPY", "expiry": "2026-10-16"}).json()
    assert body["dividend_date_uncertain"] == ["2026-11-20"]
    # Every expiry is flagged, not only the one served.
    assert body["expiry"] == "2026-10-16"


def test_quotes_errors(options_client, tmp_path):
    get = options_client.get
    assert get("/api/v1/options/quotes", params={"ticker": "QQQ"}).status_code == 404
    r = get("/api/v1/options/quotes", params={"ticker": "SPY", "expiry": "2026-12-18"})
    assert r.status_code == 404 and "2026-12-18" in r.json()["detail"]
    # A partial capture is not served, as for the surface: a later partial
    # day leaves the default on the last complete one.
    chains = tmp_path / "chains" / "SPY"
    shutil.copy(chains / "2026-10-07.parquet", chains / "2026-10-08.partial.parquet")
    assert get("/api/v1/options/quotes", params={"ticker": "SPY"}).json()["day"] == "2026-10-07"
    (chains / "2026-10-07.parquet").rename(chains / "2026-10-07.partial.parquet")
    assert get("/api/v1/options/quotes", params={"ticker": "SPY"}).status_code == 404


# -- /position-curve: the curve before expiry (decision 55) ---------------------

STRADDLE = [
    {
        "kind": "call",
        "side": 1,
        "quantity": 1,
        "strike": 770,
        "expiry": "2026-11-20",
        "premium": 21.13,
    },
    {
        "kind": "put",
        "side": 1,
        "quantity": 1,
        "strike": 770,
        "expiry": "2026-11-20",
        "premium": 10.15,
    },
]


def curve_of(client, **body):
    return client.post("/api/v1/options/position-curve", json={"ticker": "SPY", **body})


def test_position_curve_starts_at_the_capture_and_hits_the_cache(options_client):
    r = curve_of(options_client, legs=STRADDLE)
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["day"], body["as_of"], body["last_expiry"]) == ("2026-10-07",) * 2 + (
        "2026-11-20",
    )
    assert (
        body["cache_hit"] is False and curve_of(options_client, legs=STRADDLE).json()["cache_hit"]
    )
    assert body["spot"] == 777.24 and body["rate"] > 0
    spots = [p["spot"] for p in body["points"]]
    assert spots == sorted(spots) and 777.24 in spots and 770.0 in spots
    assert spots[0] == pytest.approx(777.24 * 0.75) and spots[-1] == pytest.approx(777.24 * 1.25)
    for q in body["legs"]:
        assert q["vol_source"] == "interpolated" and 0.05 < q["iv"] < 0.5
        assert q["T_remaining"] == pytest.approx(44 / 365, abs=1 / 365)
    # At the capture's spot a straddle bought at the ask is worth its mids
    # less the spread and the put's early-exercise premium: a small loss.
    at = next(p for p in body["points"] if p["spot"] == 777.24)
    paid = 100 * (21.13 + 10.15)
    model = 100 * sum(q["model_price"] for q in body["legs"])
    assert at["value"] == pytest.approx(model) and at["profit"] == pytest.approx(model - paid)
    assert -60 < at["profit"] < 0
    assert at["gamma"] > 0 and at["vega"] > 0 and at["theta"] < 0


def test_position_curve_on_the_last_expiry_is_the_payoff(options_client):
    for rule in ("sticky_strike", "sticky_moneyness"):
        body = curve_of(
            options_client, legs=STRADDLE, as_of="2026-11-20", vol_rule=rule, low=700, high=850
        ).json()
        for p in body["points"]:
            assert p["profit"] == pytest.approx(100 * (abs(p["spot"] - 770) - 31.28), abs=1e-9)
            assert p["vega"] == 0 and p["theta"] == 0


def test_position_curve_rules_agree_at_the_reference_spot_only(options_client):
    a, b = (
        curve_of(options_client, legs=STRADDLE, vol_rule=rule, low=745, high=810).json()
        for rule in ("sticky_strike", "sticky_moneyness")
    )
    assert [q["iv"] for q in a["legs"]] == pytest.approx([q["iv"] for q in b["legs"]])
    pa = {p["spot"]: p for p in a["points"]}
    pb = {p["spot"]: p for p in b["points"]}
    assert pa[777.24]["value"] == pytest.approx(pb[777.24]["value"])
    # Below the spot, moneyness reads the straddle's strike further up the
    # smile, where the vol is lower (the fixture's quotes reach about 10% out;
    # past them both rules are flat, and agree).
    assert pa[745]["value"] - pb[745]["value"] > 20


def test_position_curve_calendar_at_the_near_expiry(options_client):
    legs = [
        {
            "kind": "call",
            "side": -1,
            "quantity": 1,
            "strike": 780,
            "expiry": "2026-10-16",
            "premium": 5,
        },
        {
            "kind": "call",
            "side": 1,
            "quantity": 1,
            "strike": 780,
            "expiry": "2026-11-20",
            "premium": 15,
        },
        {"kind": "stock", "side": 1, "quantity": 10, "premium": 777.24},
    ]
    body = curve_of(options_client, legs=legs, as_of="2026-10-16").json()
    near, far, stock = body["legs"]
    assert near["T_remaining"] == 0 and far["T_remaining"] > 0
    assert near["model_price"] == pytest.approx(0.0)  # 780 call, spot 777.24
    assert stock == {
        "T_remaining": 0,
        "forward": None,
        "carry": None,
        "iv": None,
        "vol_source": None,
        "model_price": 777.24,
    }
    # The far leg is still alive, so the curve is no payoff: it bends smoothly.
    assert all(p["vega"] > 0 for p in body["points"])


def test_position_curve_errors(options_client):
    assert curve_of(options_client, legs=STRADDLE, day="2026-10-06").status_code == 404
    no_strike = [{**STRADDLE[0], "strike": None}]
    assert curve_of(options_client, legs=no_strike).status_code == 422
    r = curve_of(options_client, legs=[{**STRADDLE[0], "expiry": "2026-12-18"}])
    assert r.status_code == 422 and "2026-12-18" in r.json()["detail"]
    for as_of in ("2026-10-06", "2026-11-21"):
        assert curve_of(options_client, legs=STRADDLE, as_of=as_of).status_code == 422
    assert curve_of(options_client, legs=STRADDLE * 7).status_code == 422
    assert curve_of(options_client, legs=STRADDLE, low=800, high=700).status_code == 422
    assert curve_of(options_client, legs=[]).status_code == 422


def test_position_curve_leaves_out_expiries_it_cannot_price():
    point = {"log_moneyness": 0.0, "iv": 0.2}
    surface = {
        "expiries": [
            {"expiry": "2026-10-07", "T": 0.0, "forward": 777.0},
            {"expiry": "2026-10-09", "T": 0.005, "forward": 777.0},
            {"expiry": "2026-11-20", "T": 0.12, "forward": 779.0},
        ],
        "smiles": {
            "2026-10-07": [point, {"log_moneyness": 0.1, "iv": 0.2}],
            "2026-10-09": [point],
            "2026-11-20": [point, {"log_moneyness": 0.1, "iv": 0.18}],
        },
    }
    assert list(R.expiries_of(surface)) == [date(2026, 11, 20)]
