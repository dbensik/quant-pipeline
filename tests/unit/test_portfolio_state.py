"""
core/portfolio.py — deriving cash, positions and P&L from a trade log.

The accounting is the whole point of step 6: `portfolios.json` stored a trade
log AND a cash/positions ledger under one key, they disagreed, and
ExecutionService.GetPortfolio raised KeyError('cash') against the real file.
Deriving one from the other removes the possibility — but only if the
derivation is right, so every expected value below is computed by hand in the
test rather than read back from the implementation.

Phase 5 — decommissioning Streamlit
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.portfolio import (
    Portfolio,
    Trade,
    derive_state,
    rebalancing_orders,
)

START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def trade(ticker, action, quantity, price, day=0, costs=0.0, ident=None):
    return Trade(
        ticker=ticker,
        action=action,
        quantity=quantity,
        price=price,
        ts=START + timedelta(days=day),
        costs=costs,
        id=ident,
    )


# ---------------------------------------------------------------------------
# Cash
# ---------------------------------------------------------------------------

def test_empty_log_leaves_cash_untouched():
    state = derive_state([], 100_000.0)
    assert state.cash == 100_000.0
    assert state.positions == []
    assert state.total_equity == 100_000.0


def test_buying_spends_cash():
    state = derive_state([trade("AAPL", "BUY", 10, 150.0)], 100_000.0)
    assert state.cash == 100_000.0 - 1_500.0


def test_selling_receives_cash():
    state = derive_state(
        [trade("AAPL", "BUY", 10, 150.0), trade("AAPL", "SELL", 10, 160.0, day=1)],
        100_000.0,
    )
    assert state.cash == 100_000.0 - 1_500.0 + 1_600.0


def test_costs_reduce_cash_on_both_sides():
    """Commission is paid whether buying or selling — it is never a credit."""
    bought = derive_state([trade("AAPL", "BUY", 10, 150.0, costs=5.0)], 100_000.0)
    assert bought.cash == 100_000.0 - 1_500.0 - 5.0

    sold = derive_state([trade("AAPL", "SELL", 10, 150.0, costs=5.0)], 100_000.0)
    assert sold.cash == 100_000.0 + 1_500.0 - 5.0


# ---------------------------------------------------------------------------
# Average cost
# ---------------------------------------------------------------------------

def test_adding_to_a_position_re_averages():
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0), trade("AAPL", "BUY", 10, 200.0, day=1)],
        100_000.0,
    )
    position = state.positions[0]
    assert position.quantity == 20
    assert position.average_price == 150.0  # (10*100 + 10*200) / 20


def test_reducing_a_position_keeps_the_cost_basis():
    """
    Selling part of a holding realises P&L but must NOT move the average cost
    of what remains — otherwise the next sale reports the wrong gain.
    """
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0), trade("AAPL", "SELL", 4, 250.0, day=1)],
        100_000.0,
    )
    position = state.positions[0]
    assert position.quantity == 6
    assert position.average_price == 100.0


def test_closing_a_position_removes_it_from_holdings():
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0), trade("AAPL", "SELL", 10, 120.0, day=1)],
        100_000.0,
    )
    assert state.positions == []
    # The gain still counts even though the holding is gone.
    assert state.realised_pnl == pytest.approx(200.0)


def test_flipping_through_zero_rebases_on_the_trade_price():
    """
    Selling 15 of a 10-share holding closes the long and opens a 5-share short.
    The short's cost basis is the price it was opened at, not the old long's.
    """
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0), trade("AAPL", "SELL", 15, 120.0, day=1)],
        100_000.0,
    )
    position = state.positions[0]
    assert position.quantity == -5
    assert position.average_price == 120.0
    assert state.realised_pnl == pytest.approx(10 * 20.0)  # only the 10 closed


# ---------------------------------------------------------------------------
# Realised P&L, including shorts
# ---------------------------------------------------------------------------

def test_long_closed_above_cost_is_a_gain():
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0), trade("AAPL", "SELL", 10, 130.0, day=1)],
        100_000.0,
    )
    assert state.realised_pnl == pytest.approx(300.0)


def test_short_closed_below_cost_is_a_gain():
    """
    The sign trap. A short profits when the price FALLS, so the naive
    (exit - entry) used for longs has to be negated.
    """
    state = derive_state(
        [trade("AAPL", "SELL", 10, 100.0), trade("AAPL", "BUY", 10, 70.0, day=1)],
        100_000.0,
    )
    assert state.realised_pnl == pytest.approx(300.0)


def test_short_closed_above_cost_is_a_loss():
    state = derive_state(
        [trade("AAPL", "SELL", 10, 100.0), trade("AAPL", "BUY", 10, 130.0, day=1)],
        100_000.0,
    )
    assert state.realised_pnl == pytest.approx(-300.0)


def test_realised_pnl_survives_the_position_being_closed():
    """
    Closed positions drop out of `positions`, so their P&L has to have been
    accumulated at the portfolio level — otherwise a round trip reports zero.
    """
    state = derive_state(
        [
            trade("AAPL", "BUY", 10, 100.0),
            trade("AAPL", "SELL", 10, 150.0, day=1),
            trade("MSFT", "BUY", 5, 200.0, day=2),
        ],
        100_000.0,
    )
    assert [p.ticker for p in state.positions] == ["MSFT"]
    assert state.realised_pnl == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------

def test_average_cost_is_order_dependent_and_the_log_is_sorted():
    """
    Feeding the same trades in reverse must give the same answer, because
    derive_state sorts by timestamp. Without the sort a caller handing over an
    unsorted log silently gets a different cost basis — the check below
    confirms order genuinely matters, so the sort is doing work.
    """
    trades = [
        trade("AAPL", "BUY", 10, 100.0, day=0),
        trade("AAPL", "BUY", 10, 200.0, day=1),
        trade("AAPL", "SELL", 15, 300.0, day=2),
    ]
    forward = derive_state(trades, 100_000.0)
    backward = derive_state(list(reversed(trades)), 100_000.0)
    assert forward.positions[0].average_price == backward.positions[0].average_price
    assert forward.realised_pnl == pytest.approx(backward.realised_pnl)

    # The same three trades in a genuinely different ORDER (not just a
    # different input sequence) do produce a different basis.
    shuffled = [
        trade("AAPL", "BUY", 10, 200.0, day=0),
        trade("AAPL", "SELL", 15, 300.0, day=1),
        trade("AAPL", "BUY", 10, 100.0, day=2),
    ]
    assert derive_state(shuffled, 100_000.0).realised_pnl != pytest.approx(
        forward.realised_pnl
    )


# ---------------------------------------------------------------------------
# Market value and unrealised P&L
# ---------------------------------------------------------------------------

def test_unrealised_pnl_uses_the_supplied_price():
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0)], 100_000.0, prices={"AAPL": 130.0}
    )
    position = state.positions[0]
    assert position.market_value == pytest.approx(1_300.0)
    assert position.unrealised_pnl == pytest.approx(300.0)
    assert state.total_equity == pytest.approx(99_000.0 + 1_300.0)


def test_short_position_unrealised_pnl_has_the_right_sign():
    """A short gains when the price falls; quantity is negative, so the
    (price - average) * quantity form already handles it — but only if the
    sign of quantity is preserved rather than abs()'d."""
    state = derive_state(
        [trade("AAPL", "SELL", 10, 100.0)], 100_000.0, prices={"AAPL": 80.0}
    )
    assert state.positions[0].unrealised_pnl == pytest.approx(200.0)


def test_unpriced_positions_are_reported_not_valued_at_cost():
    """
    Valuing an unpriced holding at its cost would silently understate or
    overstate equity while looking like a complete answer.
    """
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0), trade("NOPE", "BUY", 5, 50.0, day=1)],
        100_000.0,
        prices={"AAPL": 100.0},
    )
    assert state.unpriced == ["NOPE"]
    # 250 of cost left the cash, but NOPE contributes nothing to market value.
    assert state.market_value == pytest.approx(1_000.0)
    assert state.total_equity == pytest.approx(state.cash + 1_000.0)


def test_no_prices_means_no_market_value():
    state = derive_state([trade("AAPL", "BUY", 10, 100.0)], 100_000.0)
    assert state.market_value == 0.0
    assert state.unpriced == ["AAPL"]


# ---------------------------------------------------------------------------
# The regression this step exists for
# ---------------------------------------------------------------------------

def test_a_trade_log_portfolio_yields_cash_without_a_cash_field():
    """
    THE regression. Both portfolios in the live portfolios.json were
    trade-log shaped, with no "cash" key, and ExecutionService.GetPortfolio
    did `state["cash"]` — raising KeyError against the user's real data.
    Cash is now derived, so a trade log alone always answers.
    """
    portfolio = Portfolio(name="Crypto", initial_cash=100_000.0, trades=[])
    state = derive_state(portfolio.trades, portfolio.initial_cash)
    assert state.cash == 100_000.0
    assert state.total_equity == 100_000.0


# ---------------------------------------------------------------------------
# Rebalancing
# ---------------------------------------------------------------------------

def test_rebalancing_targets_a_share_of_total_equity():
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0)], 100_000.0, prices={"AAPL": 100.0}
    )
    # Equity = 99,000 cash + 1,000 AAPL = 100,000. A 50% MSFT target at $50
    # needs 50,000 of MSFT = 1,000 shares.
    orders = rebalancing_orders(state, {"MSFT": 0.5}, {"AAPL": 100.0, "MSFT": 50.0})
    msft = next(o for o in orders if o["ticker"] == "MSFT")
    assert msft["action"] == "BUY"
    assert msft["quantity"] == 1_000


def test_rebalancing_sells_a_holding_with_no_target():
    """A position absent from target_weights has a target of zero, so it must
    be sold down — not merely left alone."""
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0)], 100_000.0, prices={"AAPL": 100.0}
    )
    orders = rebalancing_orders(state, {}, {"AAPL": 100.0})
    assert [(o["ticker"], o["action"]) for o in orders] == [("AAPL", "SELL")]


def test_rebalancing_skips_orders_below_the_minimum():
    """
    Asserted in BOTH directions. The first version of this test used a target
    the portfolio already met exactly, so the order list was empty at every
    minimum — including zero — and the threshold was never exercised.
    """
    state = derive_state(
        [trade("AAPL", "BUY", 10, 100.0)], 100_000.0, prices={"AAPL": 100.0}
    )
    # Equity 100,000; AAPL currently 1,000. A 1.5% target is 1,500, so the
    # order is worth 500 — above a 10 minimum, below a 1,000 one.
    included = rebalancing_orders(
        state, {"AAPL": 0.015}, {"AAPL": 100.0}, minimum_order_value=10.0
    )
    assert [(o["ticker"], o["quantity"]) for o in included] == [("AAPL", 5)]

    excluded = rebalancing_orders(
        state, {"AAPL": 0.015}, {"AAPL": 100.0}, minimum_order_value=1_000.0
    )
    assert excluded == []


def test_rebalancing_skips_unpriced_tickers():
    """Sizing an order needs a price; guessing one produces a wrong quantity
    that looks authoritative."""
    state = derive_state([], 100_000.0)
    orders = rebalancing_orders(state, {"AAPL": 0.5, "NOPE": 0.5}, {"AAPL": 100.0})
    assert [o["ticker"] for o in orders] == ["AAPL"]


def test_rebalancing_a_worthless_portfolio_returns_nothing():
    state = derive_state([], 0.0)
    assert rebalancing_orders(state, {"AAPL": 1.0}, {"AAPL": 100.0}) == []
