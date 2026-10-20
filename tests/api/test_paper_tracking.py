"""
GET /api/v1/portfolios/{name}/tracking and the RESTATED check (decision 51,
research/paper-tracking-2026-10-20.md).

^IRX is 0 here, so neither side earns interest and the only differences left
are the ones under test. The fixture has no corporate actions, so raw and
adjusted prices agree: with the backtest's slippage at 0, the two curves must
be the same curve.
"""

import asyncio
from dataclasses import replace
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from api.paper_tracking import track
from tests.api.conftest import FakeRates
from tests.api.test_paper_strategy import attach_in_the_past

BASE = "/api/v1/portfolios"


@pytest.fixture
def rates() -> FakeRates:
    return FakeRates(quoted_pct=0.0)


def traded(client, portfolio_repo):
    attach_in_the_past(portfolio_repo, "Empty")
    run = client.post(f"{BASE}/Empty/run?dry_run=false").json()
    assert sum(len(s["fills"]) for s in run["sessions"]) >= 6, "the fixture must trade"
    return run


def restate(repo, symbol, day, factor):
    """Scale one stored bar's prices: a restatement after the fact."""
    records = repo.data[symbol]
    for i, r in enumerate(records):
        if r.ohlcv.timestamp.utc.date() == day:
            o = r.ohlcv
            records[i] = replace(
                r,
                ohlcv=replace(
                    o,
                    open=o.open * factor,
                    high=o.high * factor,
                    low=o.low * factor,
                    close=o.close * factor,
                ),
            )
            return
    raise AssertionError(f"no {symbol} bar on {day}")


def test_without_slippage_the_backtest_is_the_paper_record(client, repo, portfolio_repo, rates):
    traded(client, portfolio_repo)
    report = asyncio.run(track("Empty", portfolio_repo, repo, rates, slippage_pct=0.0))
    paper, backtest = dict(report.paper), dict(report.backtest)
    assert set(paper) == set(backtest) and len(paper) > 100
    assert all(paper[d] == pytest.approx(backtest[d], rel=1e-12) for d in paper)
    assert [(f.session, f.ticker, f.action, f.quantity) for f in report.paper_fills] == [
        (f.session, f.ticker, f.action, f.quantity) for f in report.backtest_fills
    ]
    assert report.backtest_slippage == 0.0
    assert report.gap == pytest.approx(0.0, abs=1e-6)
    assert report.other == pytest.approx(0.0, abs=1e-6)
    assert report.restated == []


def test_the_gap_is_split_into_slippage_costs_and_the_rest(client, repo, portfolio_repo):
    traded(client, portfolio_repo)
    body = client.get(f"{BASE}/Empty/tracking").json()
    opens = {
        (r.asset.symbol, r.ohlcv.timestamp.utc.date().isoformat()): r.ohlcv.open
        for s in ("AAPL", "MSFT")
        for r in repo.data[s]
    }
    slippage = sum(
        abs(f["price"] - opens[(f["ticker"], f["session"])]) * f["quantity"]
        for f in body["backtest_fills"]
    )
    assert slippage > 0
    assert body["backtest_slippage"] == pytest.approx(slippage)
    assert body["gap"] == pytest.approx(
        body["backtest_slippage"] + body["costs_difference"] + body["other"]
    )
    # Adverse slippage costs the backtest: the paper record is ahead of it.
    assert body["gap"] > 0
    assert body["paper"][-1]["total"] == pytest.approx(
        client.get(f"{BASE}/Empty").json()["total_equity"]
    )


def test_a_restated_bar_under_a_traded_decision_is_flagged(client, repo, portfolio_repo):
    run = traded(client, portfolio_repo)
    decisions = portfolio_repo._portfolios["Empty"].metadata["strategy"]["decisions"]
    day = next(d for d, v in sorted(decisions.items()) if v.get("AAPL") == 1.0)
    restate(repo, "AAPL", date.fromisoformat(day), 0.5)
    body = client.get(f"{BASE}/Empty/tracking").json()
    # That day, and any later decision whose 20-bar average holds the bar.
    days = [r["day"] for r in body["restated"]]
    assert days[0] == day
    assert all(0 <= (date.fromisoformat(d) - date.fromisoformat(day)).days < 20 for d in days)
    assert body["restated"][0]["then"]["AAPL"] == 1.0
    assert body["restated"][0]["now"]["AAPL"] == -1.0
    assert body["flags"] and body["flags"][0].startswith(f"RESTATED Empty: the decision of {day}")
    # The record itself is never rewritten.
    trades = client.get(f"{BASE}/Empty/trades").json()
    assert len(trades) == sum(len(s["fills"]) for s in run["sessions"])


def test_a_bar_after_the_last_traded_decision_is_not_a_restatement(client, repo, portfolio_repo):
    run = traded(client, portfolio_repo)
    restate(repo, "AAPL", date.fromisoformat(run["newest_bar"]), 0.5)
    assert client.get(f"{BASE}/Empty/tracking").json()["restated"] == []


def test_decisions_on_bars_not_yet_traded_are_not_restatements(client, repo, portfolio_repo):
    # Two weeks of new bars arrive after the run: their weekly decisions are
    # the next run's to trade, not a record that moved.
    traded(client, portfolio_repo)
    for symbol in ("AAPL", "MSFT"):
        last = repo.data[symbol][-1]
        for k in range(1, 15):
            stamp = replace(last.ohlcv.timestamp, utc=last.ohlcv.timestamp.utc + timedelta(days=k))
            repo.data[symbol].append(replace(last, ohlcv=replace(last.ohlcv, timestamp=stamp)))
    assert client.get(f"{BASE}/Empty/tracking").json()["restated"] == []


def test_tracking_needs_a_strategy(client: TestClient):
    assert client.get(f"{BASE}/Growth/tracking").status_code == 409
    assert client.get(f"{BASE}/Nope/tracking").status_code == 404
