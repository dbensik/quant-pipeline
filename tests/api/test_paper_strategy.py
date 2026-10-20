"""
/api/v1/portfolios/{name}/strategy and /run (decision 50,
research/paper-trader-2026-10-20.md). The trading rules are pinned against
PortfolioBacktester in tests/unit/test_paper_trader.py; these cover the HTTP
contract: what may be attached, that an attached portfolio takes no manual
trades, that a dry run writes nothing and a second run writes nothing new.

Fixture bars are daily from 2024-01-01 (400 days, weekends included) and
^IRX is 2% on weekdays. A run in the fixture's past needs an attach date in
it, which the API never allows: those tests write the binding through the
repository.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from core.paper_history import new_york_today
from core.paper_trader import StrategyBinding

BASE = "/api/v1/portfolios"
TERMS = {"borrow_rate": 0.01, "max_gross": 2.0, "maintenance": 0.3}
TREND = {"strategy_id": "asset_class_trend", "symbols": ["AAPL", "MSFT"], "params": {"window": 20}}


def attach_in_the_past(portfolio_repo, name, on=date(2024, 3, 1), **kw):
    binding = StrategyBinding(
        strategy=kw.get("strategy", "asset_class_trend"),
        params=kw.get("params", {"window": 20, "rebalance_frequency": "W"}),
        symbols=tuple(kw.get("symbols", ("AAPL", "MSFT"))),
        weights=kw.get("weights", {"AAPL": 0.5, "MSFT": 0.5}),
        transaction_cost=0.001,
        attached_on=on,
        signal_from=date(2024, 1, 1),
        last_session=on,
    )
    found = portfolio_repo._portfolios[name]
    portfolio_repo._portfolios[name] = type(found)(
        **{**vars(found), "metadata": {**(found.metadata or {}), "strategy": binding.to_metadata()}}
    )


def test_a_strategy_attaches_to_an_empty_portfolio_from_today(client: TestClient):
    response = client.put(f"{BASE}/Empty/strategy", json=TREND)
    assert response.status_code == 200
    body = response.json()
    today = new_york_today().isoformat()
    assert body["attached_on"] == body["last_session"] == today
    assert body["weights"] == {"AAPL": 0.5, "MSFT": 0.5}
    assert body["signal_from"] == "2024-01-01"
    assert body["params"] == {"window": 20, "rebalance_frequency": "ME"}
    assert client.get(f"{BASE}/Empty").json()["strategy"] == body
    # Its cash earns from today, as a backtest's from its first bar, and
    # keeps that start after it is detached.
    listed = {p["name"]: p for p in client.get(BASE).json()}
    assert listed["Empty"]["metadata"]["cash_from"] == today
    client.delete(f"{BASE}/Empty/strategy")
    listed = {p["name"]: p for p in client.get(BASE).json()}
    assert listed["Empty"]["metadata"] == {"cash_from": today}


def test_an_attached_portfolio_earns_interest_before_its_first_trade(client, portfolio_repo):
    attach_in_the_past(portfolio_repo, "Empty")
    found = portfolio_repo._portfolios["Empty"]
    found.metadata["cash_from"] = "2024-03-01"
    body = client.get(f"{BASE}/Empty").json()
    assert body["trade_count"] == 0
    assert body["interest"] > 0
    assert body["cash"] == pytest.approx(50_000.0 + body["interest"])


@pytest.mark.parametrize(
    "payload, status, words",
    [
        ({**TREND, "strategy_id": "nope"}, 404, "nope"),
        ({**TREND, "symbols": ["AAPL", "NOPE"]}, 404, "NOPE"),
        ({**TREND, "symbols": ["AAPL"]}, 422, "2 to 50"),
        ({"strategy_id": "rsi", "symbols": ["AAPL", "MSFT"]}, 422, "exactly 1"),
        ({**TREND, "symbols": ["AAPL", "BTC-USD"]}, 422, "crypto"),
        ({**TREND, "params": {"windo": 3}}, 422, "Unknown parameter"),
        ({"strategy_id": "pairs_trading", "symbols": ["AAPL", "MSFT"]}, 422, "no short terms"),
    ],
)
def test_what_cannot_be_attached(client: TestClient, payload, status, words):
    response = client.put(f"{BASE}/Empty/strategy", json=payload)
    assert response.status_code == status
    assert words in response.json()["detail"]


def test_a_portfolio_with_trades_or_a_strategy_takes_no_new_one(client: TestClient):
    assert client.put(f"{BASE}/Growth/strategy", json=TREND).status_code == 409
    client.put(f"{BASE}/Empty/strategy", json=TREND)
    assert client.put(f"{BASE}/Empty/strategy", json=TREND).status_code == 409


def test_a_strategy_that_shorts_needs_terms_and_keeps_them(client: TestClient):
    client.put(f"{BASE}/Empty/short-terms", json=TERMS)
    pairs = {"strategy_id": "pairs_trading", "symbols": ["AAPL", "MSFT"]}
    assert client.put(f"{BASE}/Empty/strategy", json=pairs).json()["weights"] == {}
    response = client.delete(f"{BASE}/Empty/short-terms")
    assert response.status_code == 409
    assert "detach" in response.json()["detail"]


def test_an_attached_portfolio_takes_no_manual_trades_until_detached(client: TestClient):
    client.put(f"{BASE}/Empty/strategy", json=TREND)
    trade = {"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": 100.0}
    response = client.post(f"{BASE}/Empty/trades", json=trade)
    assert response.status_code == 409
    assert "asset_class_trend" in response.json()["detail"]
    assert client.delete(f"{BASE}/Empty/strategy").status_code == 204
    assert client.get(f"{BASE}/Empty").json()["strategy"] is None
    assert client.post(f"{BASE}/Empty/trades", json=trade).status_code == 201
    assert client.delete(f"{BASE}/Empty/strategy").status_code == 404


def test_removing_a_trade_from_an_attached_portfolio_is_refused(client: TestClient, portfolio_repo):
    attach_in_the_past(portfolio_repo, "Empty")
    client.post(f"{BASE}/Empty/run?dry_run=false")
    trade_id = client.get(f"{BASE}/Empty/trades").json()[0]["id"]
    assert client.delete(f"{BASE}/Empty/trades/{trade_id}").status_code == 409


def test_a_dry_run_shows_the_fills_and_writes_nothing(client: TestClient, portfolio_repo):
    attach_in_the_past(portfolio_repo, "Empty")
    body = client.post(f"{BASE}/Empty/run").json()
    assert body["written"] is False
    fills = [f for s in body["sessions"] for f in s["fills"]]
    assert fills, "the fixture must trade"
    assert all(f["notes"].startswith("asset_class_trend: signal") for f in fills)
    assert client.get(f"{BASE}/Empty/trades").json() == []
    assert client.get(f"{BASE}/Empty").json()["strategy"]["last_session"] == "2024-03-01"


def test_a_run_writes_each_session_once(client: TestClient, portfolio_repo):
    attach_in_the_past(portfolio_repo, "Empty")
    first = client.post(f"{BASE}/Empty/run?dry_run=false").json()
    assert first["written"] is True
    trades = client.get(f"{BASE}/Empty/trades").json()
    assert len(trades) == sum(len(s["fills"]) for s in first["sessions"]) > 0
    assert all(t["broker"] == "paper-trader" for t in trades)
    state = client.get(f"{BASE}/Empty").json()
    assert state["strategy"]["last_session"] == first["last_session"] == first["newest_bar"]
    again = client.post(f"{BASE}/Empty/run?dry_run=false").json()
    assert again["sessions"] == []
    assert len(client.get(f"{BASE}/Empty/trades").json()) == len(trades)
    assert [h["drift"] for h in again["holdings"]] == [False, False]


def test_a_run_another_run_beat_writes_nothing(client: TestClient, portfolio_repo, monkeypatch):
    attach_in_the_past(portfolio_repo, "Empty")

    async def lost(*args, **kwargs):
        return False

    monkeypatch.setattr(portfolio_repo, "record_session", lost)
    response = client.post(f"{BASE}/Empty/run?dry_run=false")
    assert response.status_code == 409
    assert "another run" in response.json()["detail"]


def test_running_a_manual_portfolio_is_409(client: TestClient):
    response = client.post(f"{BASE}/Growth/run")
    assert response.status_code == 409
    assert client.post(f"{BASE}/Nope/run").status_code == 404
