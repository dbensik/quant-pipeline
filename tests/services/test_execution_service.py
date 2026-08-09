"""
services/execution_service — the SIGNED paper-trading path.

Covers the two rules the old JSON-backed implementation got wrong, and the
accounting it never did:

  * A named-but-absent portfolio is REJECTED. PortfolioManager fell back to
    "the only portfolio" when there was exactly one, so a typo traded in a
    different book.
  * A malformed quote timestamp is treated as EXPIRED. The old version logged
    the parse error and proceeded, so the one input that defeats the freshness
    check also skipped it.
  * Positions are valued at the latest stored close. The old version used the
    entry price "as proxy for value in MVP", which made unrealised P&L
    identically zero.

The store is stubbed, so none of this needs a database.

Phase 5 — reconnecting the signed execution layer
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.portfolio import Trade, derive_state
from services.execution_service.portfolio_store import PortfolioNotFound
from services.execution_service.service import ExecutionService
from services.proto import execution_pb2

def fresh() -> str:
    """
    A quote timestamp from RIGHT NOW.

    Deliberately a function. As a module-level constant it was evaluated at
    import, so in a full-suite run — about 40 seconds — every "fresh" quote had
    aged past the 30-second window by the time these tests executed. They
    passed in isolation and failed together, which is the worst way to find
    out.
    """
    return datetime.now(timezone.utc).isoformat()


def stale() -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat()


class FakeContext:
    def __init__(self):
        self.code = None
        self.details_text = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details_text = details


class FakeStore:
    """Stands in for PortfolioStore. No database."""

    def __init__(self, known=("Default Portfolio",), trades=None, prices=None):
        self.known = set(known)
        self.trades = list(trades or [])
        self.prices = prices or {}
        self.appended = []
        self.next_id = 100

    def _check(self, name):
        if name not in self.known:
            raise PortfolioNotFound(f"No portfolio named {name!r}.")

    def state(self, name, prices=None):
        self._check(name)
        return derive_state(self.trades, 100_000.0, prices)

    def latest_prices(self, tickers):
        return {t: self.prices[t] for t in tickers if t in self.prices}

    def append_trade(self, name, symbol, action, quantity, price, when=None):
        self._check(name)
        action = (action or "").upper()
        if action not in ("BUY", "SELL"):
            raise ValueError(f"`action` must be BUY or SELL; got {action!r}.")
        if quantity <= 0:
            raise ValueError("Quantity must be positive.")
        self.appended.append((name, symbol, action, quantity, price))
        self.next_id += 1
        return self.next_id


def trade(ticker, action, quantity, price, day=0):
    return Trade(
        ticker=ticker,
        action=action,
        quantity=quantity,
        price=price,
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=day),
    )


# ---------------------------------------------------------------------------
# ExecuteTrade
# ---------------------------------------------------------------------------

def test_a_fresh_trade_is_recorded():
    store = FakeStore()
    service = ExecutionService(store=store)
    response = service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="BUY", quantity=10, price=200.0, timestamp=fresh()
        ),
        FakeContext(),
    )
    assert response.success is True
    assert store.appended == [("Default Portfolio", "AAPL", "BUY", 10.0, 200.0)]


def test_the_transaction_id_identifies_the_stored_trade():
    """It used to be a unix timestamp, which referred to nothing."""
    service = ExecutionService(store=FakeStore())
    response = service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="BUY", quantity=1, price=1.0, timestamp=fresh()
        ),
        FakeContext(),
    )
    assert response.transaction_id == "101"


def test_a_named_portfolio_is_used():
    store = FakeStore(known={"Growth"})
    service = ExecutionService(store=store)
    service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="BUY", quantity=1, price=1.0,
            timestamp=fresh(), portfolio="Growth",
        ),
        FakeContext(),
    )
    assert store.appended[0][0] == "Growth"


def test_an_unknown_portfolio_is_rejected_not_substituted():
    """
    THE regression. PortfolioManager fell back to "the only portfolio" when
    exactly one existed, so a typo executed against the wrong book.
    """
    store = FakeStore(known={"Growth"})
    service = ExecutionService(store=store)
    response = service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="BUY", quantity=1, price=1.0,
            timestamp=fresh(), portfolio="Growht",
        ),
        FakeContext(),
    )
    assert response.success is False
    assert "No portfolio named" in response.message
    assert store.appended == []


def test_a_stale_quote_is_rejected():
    store = FakeStore()
    service = ExecutionService(store=store)
    response = service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="BUY", quantity=1, price=1.0, timestamp=stale()
        ),
        FakeContext(),
    )
    assert response.success is False
    assert "Quote expired" in response.message
    assert store.appended == []


@pytest.mark.parametrize("timestamp", ["", "not-a-date", "20260809"])
def test_an_unparseable_quote_timestamp_is_treated_as_expired(timestamp):
    """
    The old version logged the parse failure and PROCEEDED, so the one input
    that defeats the freshness check also skipped it.
    """
    store = FakeStore()
    service = ExecutionService(store=store)
    response = service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="BUY", quantity=1, price=1.0, timestamp=timestamp
        ),
        FakeContext(),
    )
    assert response.success is False
    assert store.appended == []


def test_an_invalid_action_is_rejected():
    store = FakeStore()
    service = ExecutionService(store=store)
    response = service.ExecuteTrade(
        execution_pb2.TradeRequest(
            symbol="AAPL", action="SHORT", quantity=1, price=1.0, timestamp=fresh()
        ),
        FakeContext(),
    )
    assert response.success is False
    assert store.appended == []


# ---------------------------------------------------------------------------
# GetPortfolio
# ---------------------------------------------------------------------------

def test_get_portfolio_derives_cash_from_the_trade_log():
    """
    THE original bug. GetPortfolio did `state["cash"]` on a trade-log-shaped
    portfolio and raised KeyError against real data.
    """
    store = FakeStore(trades=[trade("AAPL", "BUY", 10, 100.0)])
    service = ExecutionService(store=store)
    response = service.GetPortfolio(execution_pb2.PortfolioRequest(), FakeContext())

    assert response.cash_balance == pytest.approx(99_000.0)
    assert set(response.positions) == {"AAPL"}


def test_positions_are_valued_at_the_latest_close_not_entry_price():
    """
    The old version used the entry price "as proxy for value in MVP", so
    unrealised P&L was identically zero and total equity was wrong.
    """
    store = FakeStore(
        trades=[trade("AAPL", "BUY", 10, 100.0)], prices={"AAPL": 150.0}
    )
    service = ExecutionService(store=store)
    response = service.GetPortfolio(execution_pb2.PortfolioRequest(), FakeContext())

    assert response.positions["AAPL"].current_value == pytest.approx(1_500.0)
    assert response.total_equity == pytest.approx(99_000.0 + 1_500.0)


def test_an_unpriced_position_falls_back_to_cost_rather_than_vanishing():
    store = FakeStore(trades=[trade("NOPE", "BUY", 10, 50.0)], prices={})
    service = ExecutionService(store=store)
    response = service.GetPortfolio(execution_pb2.PortfolioRequest(), FakeContext())

    assert response.positions["NOPE"].current_value == pytest.approx(500.0)


def test_an_empty_portfolio_reports_its_cash():
    store = FakeStore(trades=[])
    service = ExecutionService(store=store)
    response = service.GetPortfolio(execution_pb2.PortfolioRequest(), FakeContext())

    assert response.cash_balance == pytest.approx(100_000.0)
    assert dict(response.positions) == {}


def test_an_unknown_portfolio_is_a_not_found_status():
    store = FakeStore(known={"Growth"})
    service = ExecutionService(store=store)
    context = FakeContext()
    service.GetPortfolio(
        execution_pb2.PortfolioRequest(portfolio="Nope"), context
    )
    assert context.code is not None
    assert "No portfolio named" in context.details_text


def test_a_short_position_reports_a_negative_quantity():
    store = FakeStore(
        trades=[trade("AAPL", "SELL", 5, 200.0)], prices={"AAPL": 180.0}
    )
    service = ExecutionService(store=store)
    response = service.GetPortfolio(execution_pb2.PortfolioRequest(), FakeContext())

    assert response.positions["AAPL"].quantity == pytest.approx(-5.0)
    assert response.positions["AAPL"].current_value == pytest.approx(-900.0)
