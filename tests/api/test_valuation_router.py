"""
api/routers/valuation.py without a database: Apple's real facts (the
2026-10-08 fixture) behind a fake ValuationData, synthetic AAPL and SPY bars
that record which price basis each fetch asked for, and a fake rate store.
"""

import gzip
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from api.dependencies import get_market_data_repo
from api.main import app
from api.routers import valuation as R
from core.fundamentals import parse_company_facts
from core.models import OHLCV, Asset, MarketDataRecord, Timestamp
from core.rates import RateObservation

FIX = Path(__file__).resolve().parents[1] / "fixtures"
AAPL_FACTS = parse_company_facts(json.loads(gzip.open(FIX / "companyfacts_aapl_2026-10-08.json.gz").read()),
                                 datetime(2026, 10, 8, tzinfo=timezone.utc))


# Apple's real facts split at 2024-01-01 between a "predecessor" CIK 1 and a
# "successor" CIK 2: the holding-company reorganisation shape (XOM, APA, ...).
SPLIT_AT = date(2024, 1, 1)
CHAINED = {1: [f for f in AAPL_FACTS if f.filed < SPLIT_AT], 2: [f for f in AAPL_FACTS if f.filed >= SPLIT_AT]}


class FakeValuationData:
    EQUITIES = {"AAPL": ([320193], "Information Technology"), "JPM": ([19617], "Financials"),
                "PARA": ([], "Communication Services"), "MSFT": ([789019], "Information Technology"),
                "REORG": ([2, 1], "Information Technology"), "SUCCONLY": ([2], "Information Technology"),
                "PLD": ([1045609], "Real Estate"), "UNH": ([731766], "Health Care")}

    async def equity(self, symbol):
        return self.EQUITIES.get(symbol.upper())

    async def facts(self, ciks, concepts):
        concepts, out = set(concepts), []
        for cik in ciks:
            src = AAPL_FACTS if cik == 320193 else CHAINED.get(cik, [])
            out.extend(f for f in src if f.concept in concepts)
        return out

    async def splits(self, symbol):
        return [(date(2020, 8, 31), 4.0)] if symbol.upper() in ("AAPL", "REORG", "SUCCONLY") else []

    async def filing_dates(self, ciks):
        facts = [f for c in ciks for f in (AAPL_FACTS if c == 320193 else CHAINED.get(c, []))]
        # Chained symbols reach back into the predecessor's filings (2023).
        since = date(2023, 6, 1) if set(ciks) & set(CHAINED) else date(2025, 1, 1)
        return sorted({f.filed for f in facts if f.form in ("10-K", "10-Q") and f.filed >= since})

    async def coverage(self):
        return {"equities": 4, "with_cik": 3, "facts": len(AAPL_FACTS), "ciks_with_facts": 1, "newest_filed": date(2026, 7, 31)}


class Bars:
    def __init__(self):
        self.calls = []
        rng = np.random.default_rng(3)
        days = [date(2023, 1, 2) + timedelta(days=i) for i in range(1380)]
        spy = 400 * np.exp(np.cumsum(rng.normal(0, 0.01, len(days))))
        aapl = 150 * np.exp(np.cumsum(1.1 * np.diff(np.log(np.r_[400, spy])) + rng.normal(0, 0.008, len(days))))
        self.series = {"SPY": dict(zip(days, spy)), "AAPL": dict(zip(days, aapl))}
        self.series["REORG"] = self.series["SUCCONLY"] = self.series["AAPL"]

    async def fetch_range(self, symbol, asset_class, start, end, source=None, adjust="total"):
        self.calls.append((symbol, adjust))
        out = []
        for d, px in sorted(self.series.get(symbol, {}).items()):
            ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            if start <= ts <= end:
                out.append(MarketDataRecord(asset=Asset(symbol=symbol, asset_class="equity", source="yfinance"),
                                            ohlcv=OHLCV(open=px, high=px, low=px, close=px, volume=1.0, timestamp=Timestamp(utc=ts))))
        return out


class Rates:
    def __init__(self, rows=True):
        self.rows = [RateObservation("^IRX", date(2023, 1, 3) + timedelta(days=i), 4.0, "test", datetime(2026, 1, 1, tzinfo=timezone.utc))
                     for i in range(0, 1400, 7)] if rows else []

    async def insert(self, obs):
        return 0

    async def newest_date(self, series):
        return None

    async def latest_on_or_before(self, series, as_of):
        prior = [o for o in self.rows if o.obs_date <= as_of]
        return prior[-1] if prior else None


@pytest.fixture
def vclient(client):
    bars = Bars()
    app.dependency_overrides[R.get_valuation_data] = lambda: FakeValuationData()
    app.dependency_overrides[R.get_valuation_rates] = lambda: Rates()
    app.dependency_overrides[get_market_data_repo] = lambda: bars
    client.bars = bars
    return client


def test_coverage(vclient):
    body = vclient.get("/api/v1/valuation/coverage").json()
    assert body == {"equities": 4, "with_cik": 3, "facts": len(AAPL_FACTS), "ciks_with_facts": 1, "newest_filed": "2026-07-31"}


def test_statements_as_of(vclient):
    body = vclient.get("/api/v1/valuation/AAPL/statements", params={"as_of": "2026-10-08"}).json()
    rev = body["annual"]["revenue"][-1]
    assert (rev["period_end"], rev["value"], rev["filed"]) == ("2025-09-27", 416161000000.0, "2025-10-31")
    assert body["ttm_through"]["cfo"] == "2026-06-27"
    assert body["balances"]["securities_noncurrent"]["value"] > 0
    # The same request a day before the 10-K was filed does not see FY2025.
    early = vclient.get("/api/v1/valuation/AAPL/statements", params={"as_of": "2025-10-31"}).json()
    assert early["annual"]["revenue"][-1]["period_end"] == "2024-09-28"


def test_dcf_returns_a_value_its_inputs_and_where_each_came_from(vclient):
    r = vclient.post("/api/v1/valuation/dcf", json={"symbol": "aapl", "as_of": "2026-10-08"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["symbol"] == "AAPL" and math.isfinite(body["result"]["per_share"])
    assert body["lines"]["cfo"]["source"] == "TTM to 2026-06-27"
    assert body["lines"]["after_tax_interest"]["source"].startswith("estimated")
    assert body["discount"]["cost_of_debt_estimated"] is True
    assert len(body["sensitivity"]["per_share"]) == 5
    # Prices against shares are fetched AS TRADED; beta on total returns.
    assert ("AAPL", "none") in vclient.bars.calls and ("SPY", "total") in vclient.bars.calls


def test_overrides_are_applied_and_reported(vclient):
    base = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08"}).json()
    lower = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08", "wacc": 0.07, "growth_first": 0.10}).json()
    assert lower["result"]["inputs"]["wacc"] == 0.07 and lower["result"]["inputs"]["growth_first"] == 0.10
    assert sorted(lower["overridden"]) == ["growth_first", "wacc"] and base["overridden"] == []
    assert lower["result"]["per_share"] > base["result"]["per_share"]


def test_an_impossible_grid_cell_is_null_not_an_error(vclient):
    body = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08", "wacc": 0.03, "growth_terminal": 0.03}).json()
    assert body["result"]["per_share"] is None and "terminal growth" in body["result"]["reason"]
    assert any(v is None for row in body["sensitivity"]["per_share"] for v in row)


@pytest.mark.parametrize("payload,status,fragment", [
    ({"symbol": "JPM"}, 422, "banks and insurers"),
    ({"symbol": "PLD"}, 422, "FFO/AFFO"),
    ({"symbol": "UNH"}, 422, "managed care"),
    ({"symbol": "PARA"}, 422, "no CIK"),
    ({"symbol": "MSFT"}, 422, "no SEC facts"),
    ({"symbol": "SPY"}, 404, "not a registered equity"),
    ({"symbol": "AAPL", "as_of": "2022-06-01"}, 422, "no AAPL price"),
])
def test_refusals_say_why(vclient, payload, status, fragment):
    r = vclient.post("/api/v1/valuation/dcf", json=payload)
    assert r.status_code == status and fragment in r.json()["detail"]


def test_no_rate_is_409(vclient):
    app.dependency_overrides[R.get_valuation_rates] = lambda: Rates(rows=False)
    r = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08"})
    assert r.status_code == 409


def test_history_is_one_point_the_day_after_each_filing(vclient):
    body = vclient.get("/api/v1/valuation/AAPL/history").json()
    filed = sorted({f.filed for f in AAPL_FACTS if f.form in ("10-K", "10-Q") and f.filed >= date(2025, 1, 1)})
    assert [p["as_of"] for p in body["points"]] == [(d + timedelta(days=1)).isoformat() for d in filed]
    valued = [p for p in body["points"] if p["per_share"] is not None]
    assert valued and all(p["price"] > 0 for p in valued)


def test_a_reorganised_company_is_valued_from_its_whole_filer_chain(vclient):
    apple = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08"}).json()
    chain = vclient.post("/api/v1/valuation/dcf", json={"symbol": "REORG", "as_of": "2026-10-08"}).json()
    assert chain["cik"] == 2 and chain["predecessor_ciks"] == [1]
    # Same facts, split across two filers: the same valuation, to the cent.
    assert chain["result"]["per_share"] == pytest.approx(apple["result"]["per_share"], rel=1e-12)
    assert chain["lines"]["growth_first"] == apple["lines"]["growth_first"]
    # What the successor alone loses is the long history: its filings still
    # carry three years of comparatives, but nothing before them.
    full = vclient.get("/api/v1/valuation/REORG/statements", params={"as_of": "2026-10-08"}).json()
    alone = vclient.get("/api/v1/valuation/SUCCONLY/statements", params={"as_of": "2026-10-08"}).json()
    years_full = [p["period_end"][:4] for p in full["annual"]["revenue"]]
    years_alone = [p["period_end"][:4] for p in alone["annual"]["revenue"]]
    assert years_full[0] == "2007" and years_alone[0] >= "2021"
    assert len(years_full) == 19 and len(years_alone) < 6


def test_history_of_a_reorganised_company_includes_its_predecessor_filings(vclient):
    points = vclient.get("/api/v1/valuation/REORG/history").json()["points"]
    pre = [p for p in points if p["as_of"] < SPLIT_AT.isoformat()]
    assert pre, "no point from the predecessor's filings"
    assert all(p["as_of"] > "2023-06-01" for p in points)


def test_beta_alternatives_are_reported_and_an_override_is_used(vclient):
    base = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08"}).json()
    b = base["betas"]
    assert set(b) == {"daily_3y", "daily_3y_blume", "weekly_5y"}
    assert b["daily_3y_blume"]["beta"] == pytest.approx(2 / 3 * b["daily_3y"]["beta"] + 1 / 3)
    assert b["weekly_5y"]["observations"] > 50
    # Decision 11: the beta used is weekly 5y Blume-adjusted; the daily 3y
    # beta of decision 4 is still reported.
    assert base["lines"]["beta"]["value"] == pytest.approx(2 / 3 * b["weekly_5y"]["beta"] + 1 / 3)
    assert base["lines"]["beta"]["source"].startswith("weekly 5y")
    over = vclient.post("/api/v1/valuation/dcf", json={"symbol": "AAPL", "as_of": "2026-10-08", "beta": 1.5}).json()
    assert over["discount"]["cost_of_equity"] == pytest.approx(over["lines"]["risk_free"]["value"] + 1.5 * 0.045)
    assert "beta" in over["overridden"] and over["betas"] == base["betas"]
