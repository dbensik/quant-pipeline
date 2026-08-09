"""
/api/v1/portfolios — CRUD, trade log, derived state, rebalancing previews.

The accounting itself is covered by tests/unit/test_portfolio_state.py; these
cover the HTTP contract, and in particular the two behaviours that had to
change on the way out of PortfolioManager: no silent portfolio substitution,
and no unlimited leverage.

Phase 5 — decommissioning Streamlit
"""

from fastapi.testclient import TestClient

BASE = "/api/v1/portfolios"


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def test_lists_portfolios(client: TestClient):
    names = [p["name"] for p in client.get(BASE).json()]
    assert names == ["Empty", "Growth"]


def test_listing_does_not_include_trade_logs(client: TestClient):
    """
    A listing is rendered as a picker. Loading every trade log to draw it
    would make the cost of listing grow with total trading history.
    """
    assert all("trades" not in p for p in client.get(BASE).json())


def test_creates_a_portfolio(client: TestClient):
    response = client.post(
        BASE, json={"name": "New Fund", "initial_cash": 25_000.0}
    )
    assert response.status_code == 201
    assert response.json()["initial_cash"] == 25_000.0
    assert "New Fund" in [p["name"] for p in client.get(BASE).json()]


def test_duplicate_name_is_409(client: TestClient):
    """409, not 422 — the request is well-formed, it conflicts with state."""
    response = client.post(BASE, json={"name": "Growth"})
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_metadata_round_trips(client: TestClient):
    """
    MAG7PortTest1 carried `constituents` and `weights` that belong to no
    column; they are preserved rather than dropped on import.
    """
    payload = {"constituents": ["AAPL", "MSFT"], "weights": {"AAPL": 0.6}}
    client.post(BASE, json={"name": "Basket", "metadata": payload})
    listed = {p["name"]: p for p in client.get(BASE).json()}
    assert listed["Basket"]["metadata"] == payload


def test_deletes_a_portfolio(client: TestClient):
    assert client.delete(f"{BASE}/Empty").status_code == 204
    assert "Empty" not in [p["name"] for p in client.get(BASE).json()]


def test_deleting_an_unknown_portfolio_is_404(client: TestClient):
    assert client.delete(f"{BASE}/Nope").status_code == 404


# ---------------------------------------------------------------------------
# Derived state — the regression this step exists for
# ---------------------------------------------------------------------------

def test_state_is_derived_for_a_trade_log_portfolio(client: TestClient):
    """
    THE regression. Both portfolios in the live portfolios.json were
    trade-log shaped with no "cash" key, and ExecutionService.GetPortfolio
    did `state["cash"]` — KeyError against real data. Cash is now derived, so
    a trade log alone always answers.
    """
    body = client.get(f"{BASE}/Growth").json()
    # 100,000 - (10*100 + 1 costs) - (5*200) = 97,999
    assert body["cash"] == 97_999.0
    assert body["trade_count"] == 2


def test_state_lists_open_positions_with_cost_basis(client: TestClient):
    positions = {p["ticker"]: p for p in client.get(f"{BASE}/Growth").json()["positions"]}
    assert positions["AAPL"]["quantity"] == 10
    assert positions["AAPL"]["average_price"] == 100.0
    assert positions["MSFT"]["quantity"] == 5


def test_state_values_positions_at_the_latest_stored_close(client: TestClient):
    body = client.get(f"{BASE}/Growth").json()
    aapl = next(p for p in body["positions"] if p["ticker"] == "AAPL")
    assert aapl["last_price"] is not None
    assert aapl["market_value"] == aapl["quantity"] * aapl["last_price"]
    assert body["priced_at"] is not None


def test_prices_can_be_skipped(client: TestClient):
    body = client.get(f"{BASE}/Growth", params={"include_prices": False}).json()
    assert body["market_value"] == 0.0
    assert all(p["last_price"] is None for p in body["positions"])
    # Cash is unaffected by pricing — it comes from the trade log.
    assert body["cash"] == 97_999.0


def test_total_equity_is_cash_plus_market_value(client: TestClient):
    body = client.get(f"{BASE}/Growth").json()
    assert body["total_equity"] == body["cash"] + body["market_value"]


def test_positions_without_a_price_are_reported_not_valued_at_cost(
    client: TestClient,
):
    """
    Valuing an unpriced holding at cost would silently misstate equity while
    looking like a complete answer.
    """
    client.post(BASE, json={"name": "Odd"})
    client.post(
        f"{BASE}/Odd/trades",
        json={"ticker": "EMPTY-USD", "action": "BUY", "quantity": 1, "price": 10.0},
    )
    body = client.get(f"{BASE}/Odd").json()
    assert body["unpriced"] == ["EMPTY-USD"]
    assert body["market_value"] == 0.0


def test_state_of_an_unknown_portfolio_is_404(client: TestClient):
    assert client.get(f"{BASE}/Nope").status_code == 404


# ---------------------------------------------------------------------------
# Trade log
# ---------------------------------------------------------------------------

def test_lists_trades_oldest_first(client: TestClient):
    trades = client.get(f"{BASE}/Growth/trades").json()
    assert [t["ticker"] for t in trades] == ["AAPL", "MSFT"]


def test_records_a_trade_and_it_changes_derived_state(client: TestClient):
    before = client.get(f"{BASE}/Growth").json()["cash"]
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": 50.0},
    )
    assert response.status_code == 201
    assert client.get(f"{BASE}/Growth").json()["cash"] == before - 50.0


def test_recorded_trade_gets_an_id_usable_for_deletion(client: TestClient):
    created = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": 50.0},
    ).json()
    assert client.delete(f"{BASE}/Growth/trades/{created['id']}").status_code == 204
    assert created["id"] not in [
        t["id"] for t in client.get(f"{BASE}/Growth/trades").json()
    ]


def test_ticker_is_normalised_to_uppercase(client: TestClient):
    created = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "aapl", "action": "BUY", "quantity": 1, "price": 50.0},
    ).json()
    assert created["ticker"] == "AAPL"


def test_action_is_case_insensitive(client: TestClient):
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "buy", "quantity": 1, "price": 50.0},
    )
    assert response.status_code == 201
    assert response.json()["action"] == "BUY"


def test_unknown_action_is_422(client: TestClient):
    """
    There is no `direction` field. The Streamlit form recorded Long/Short
    independently of Buy/Sell and nothing ever read it; a short is simply a
    negative net quantity.
    """
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "SHORT", "quantity": 1, "price": 50.0},
    )
    assert response.status_code == 422
    assert "action" in response.json()["detail"]


def test_selling_more_than_held_opens_a_short(client: TestClient):
    client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "SELL", "quantity": 15, "price": 120.0},
    )
    aapl = next(
        p for p in client.get(f"{BASE}/Growth").json()["positions"]
        if p["ticker"] == "AAPL"
    )
    assert aapl["quantity"] == -5


def test_trade_on_an_unknown_portfolio_is_404(client: TestClient):
    response = client.post(
        f"{BASE}/Nope/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": 50.0},
    )
    assert response.status_code == 404


def test_deleting_an_unknown_trade_is_404(client: TestClient):
    assert client.delete(f"{BASE}/Growth/trades/99999").status_code == 404


def test_a_trade_id_from_another_portfolio_cannot_be_deleted(client: TestClient):
    """Trade ids are scoped to their portfolio, so one portfolio cannot reach
    into another's log."""
    created = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": 50.0},
    ).json()
    assert client.delete(f"{BASE}/Empty/trades/{created['id']}").status_code == 404


# ---------------------------------------------------------------------------
# Cash discipline
# ---------------------------------------------------------------------------

def test_a_buy_beyond_available_cash_is_rejected(client: TestClient):
    """
    PortfolioManager.execute_trade never checked cash, so paper trading ran on
    unlimited leverage and made P&L meaningless.
    """
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1_000, "price": 1_000.0},
    )
    assert response.status_code == 422
    assert "Insufficient cash" in response.json()["detail"]


def test_a_rejected_buy_does_not_reach_the_log(client: TestClient):
    before = client.get(f"{BASE}/Growth/trades").json()
    client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1_000, "price": 1_000.0},
    )
    assert client.get(f"{BASE}/Growth/trades").json() == before


def test_overdraft_can_be_opted_into_for_recording_real_trades(client: TestClient):
    """
    Rejecting an overdraft is right for paper trading and wrong for RECORDING
    trades that already happened at a broker, which is the same endpoint.
    """
    response = client.post(
        f"{BASE}/Growth/trades",
        params={"allow_overdraft": True},
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1_000, "price": 1_000.0},
    )
    assert response.status_code == 201
    assert client.get(f"{BASE}/Growth").json()["cash"] < 0


def test_selling_is_never_blocked_by_cash(client: TestClient):
    """A sale raises cash; it can never be the thing that overdraws it."""
    response = client.post(
        f"{BASE}/Growth/trades",
        json={"ticker": "AAPL", "action": "SELL", "quantity": 1_000, "price": 1_000.0},
    )
    assert response.status_code == 201


def test_costs_count_toward_the_cash_check(client: TestClient):
    """
    A trade priced exactly at the available balance still cannot be afforded
    once commission is added.
    """
    available = client.get(f"{BASE}/Growth").json()["cash"]
    affordable = {
        "ticker": "AAPL", "action": "BUY", "quantity": 1, "price": available,
    }
    assert client.post(f"{BASE}/Growth/trades", json=affordable).status_code == 201

    client.post(BASE, json={"name": "Tight", "initial_cash": available})
    with_costs = {**affordable, "costs": 1.0}
    response = client.post(f"{BASE}/Tight/trades", json=with_costs)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Rebalancing
# ---------------------------------------------------------------------------

def test_rebalance_returns_orders_without_recording_them(client: TestClient):
    """
    A PREVIEW. The Streamlit tool executed straight from the preview button,
    so there was no point at which the orders could be inspected and declined.
    """
    before = client.get(f"{BASE}/Growth/trades").json()
    body = client.post(
        f"{BASE}/Growth/rebalance", json={"target_weights": {"AAPL": 0.5}}
    ).json()
    assert body["orders"]
    assert client.get(f"{BASE}/Growth/trades").json() == before


def test_rebalance_sells_a_holding_with_no_target(client: TestClient):
    body = client.post(
        f"{BASE}/Growth/rebalance", json={"target_weights": {"AAPL": 1.0}}
    ).json()
    actions = {o["ticker"]: o["action"] for o in body["orders"]}
    assert actions["MSFT"] == "SELL"


def test_rebalance_reports_tickers_it_could_not_price(client: TestClient):
    body = client.post(
        f"{BASE}/Growth/rebalance", json={"target_weights": {"NOSUCH": 0.5}}
    ).json()
    assert "NOSUCH" in body["unpriced"]
    assert all(o["ticker"] != "NOSUCH" for o in body["orders"])


def test_weights_summing_above_one_are_rejected(client: TestClient):
    response = client.post(
        f"{BASE}/Growth/rebalance",
        json={"target_weights": {"AAPL": 0.7, "MSFT": 0.7}},
    )
    assert response.status_code == 422
    assert "exceed 1.0" in response.json()["detail"]


def test_negative_weights_are_rejected(client: TestClient):
    response = client.post(
        f"{BASE}/Growth/rebalance", json={"target_weights": {"AAPL": -0.5}}
    )
    assert response.status_code == 422


def test_rebalance_on_an_unknown_portfolio_is_404(client: TestClient):
    response = client.post(
        f"{BASE}/Nope/rebalance", json={"target_weights": {"AAPL": 0.5}}
    )
    assert response.status_code == 404


def test_an_unknown_portfolio_never_falls_back_to_another(client: TestClient):
    """
    PortfolioManager.execute_trade did exactly this: when the named portfolio
    was missing and exactly one existed, it silently traded in THAT one, so a
    typo executed against the wrong portfolio.
    """
    for name in ("Growth", "Empty", "Basket"):
        client.delete(f"{BASE}/{name}")
    client.post(BASE, json={"name": "Only One"})
    assert len(client.get(BASE).json()) == 1

    response = client.post(
        f"{BASE}/Typo/trades",
        json={"ticker": "AAPL", "action": "BUY", "quantity": 1, "price": 50.0},
    )
    assert response.status_code == 404
    assert client.get(f"{BASE}/Only One").json()["trade_count"] == 0
