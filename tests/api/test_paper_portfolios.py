"""
/api/v1/portfolios with derived flows and short terms (decision 49,
research/paper-shorts-2026-10-19.md). The accounting is worked by hand in
tests/unit/test_paper_accounting.py; these cover the HTTP contract.

The fixture ^IRX is 2% (bank discount) on every weekday, and "Growth" holds
10 AAPL and 5 MSFT bought in January 2024.
"""

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

from core.price_adjustment import Action

BASE = "/api/v1/portfolios"
TERMS = {"borrow_rate": 0.01, "max_gross": 2.0, "maintenance": 0.3}
FETCHED = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_idle_cash_earns_the_t_bill_rate(client: TestClient):
    body = client.get(f"{BASE}/Growth").json()
    assert body["interest"] > 0
    assert body["cash"] == pytest.approx(97_999.0 + body["interest"])


def test_a_dividend_on_a_holding_is_credited_and_listed(client: TestClient, repo):
    repo.actions["AAPL"] = [Action(date(2024, 3, 1), "dividend", 0.25, FETCHED)]
    body = client.get(f"{BASE}/Growth").json()
    assert body["dividends_received"] == 2.5
    assert body["flows"] == [
        {"day": "2024-03-01", "kind": "dividend", "ticker": "AAPL", "amount": 2.5, "quantity": 10}
    ]


def test_a_split_on_a_holding_rescales_it(client: TestClient, repo):
    repo.actions["MSFT"] = [Action(date(2024, 3, 1), "split", 2.0, FETCHED)]
    msft = next(
        p for p in client.get(f"{BASE}/Growth").json()["positions"] if p["ticker"] == "MSFT"
    )
    assert msft["quantity"] == 10
    assert msft["average_price"] == 100.0


def test_short_terms_are_set_read_back_and_validated(client: TestClient):
    assert client.put(f"{BASE}/Growth/short-terms", json=TERMS).json() == TERMS
    assert client.get(f"{BASE}/Growth").json()["short_terms"] == TERMS
    bad = client.put(f"{BASE}/Growth/short-terms", json={**TERMS, "max_gross": 4.0})
    assert bad.status_code == 422
    assert client.put(f"{BASE}/Nope/short-terms", json=TERMS).status_code == 404


def test_a_portfolio_can_be_created_with_short_terms(client: TestClient):
    client.post(BASE, json={"name": "Hedged", "short_terms": TERMS})
    assert client.get(f"{BASE}/Hedged").json()["short_terms"] == TERMS


def test_a_short_without_terms_is_refused(client: TestClient):
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "SELL", "quantity": 15, "price": 120.0},
    )
    assert response.status_code == 422
    assert "no short terms" in response.json()["detail"]


def test_crypto_cannot_be_shorted(client: TestClient):
    client.put(f"{BASE}/Growth/short-terms", json=TERMS)
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "BTC-USD", "action": "SELL", "quantity": 1, "price": 30_000.0},
    )
    assert response.status_code == 422
    assert "crypto" in response.json()["detail"]


def test_short_proceeds_cannot_pay_for_a_buy(client: TestClient):
    client.put(f"{BASE}/Empty/short-terms", json=TERMS)
    short = client.post(
        f"{BASE}/Empty/trades",
        json={"ticker": "MSFT", "action": "SELL", "quantity": 100, "price": 100.0},
    )
    assert short.status_code == 201
    state = client.get(f"{BASE}/Empty").json()
    assert state["cash"] > state["free_cash"]
    response = client.post(
        f"{BASE}/Empty/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": state["free_cash"] + 50},
    )
    assert response.status_code == 422
    assert "collateral" in response.json()["detail"]


def test_terms_cannot_be_removed_while_a_short_is_open(client: TestClient):
    client.put(f"{BASE}/Empty/short-terms", json=TERMS)
    client.post(
        f"{BASE}/Empty/trades",
        json={"ticker": "MSFT", "action": "SELL", "quantity": 10, "price": 100.0},
    )
    assert client.delete(f"{BASE}/Empty/short-terms").status_code == 409
    client.post(
        f"{BASE}/Empty/trades",
        json={"ticker": "MSFT", "action": "BUY", "quantity": 10, "price": 100.0},
    )
    assert client.delete(f"{BASE}/Empty/short-terms").status_code == 204
    assert client.get(f"{BASE}/Empty").json()["short_terms"] is None


def test_the_state_reports_margin_for_a_portfolio_with_terms(client: TestClient):
    client.put(f"{BASE}/Growth/short-terms", json=TERMS)
    body = client.get(f"{BASE}/Growth").json()
    assert body["maintenance_required"] == 0.3 * body["gross_exposure"]
    assert body["margin_excess"] == body["total_equity"] - body["maintenance_required"]
    assert body["below_maintenance"] is False
