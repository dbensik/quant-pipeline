"""
Paper portfolios: splits, dividends, interest and shorts (decision 49,
research/paper-shorts-2026-10-19.md), derived by core/portfolio.derive_state
from a MarketHistory. Every expected value is worked by hand here. Each test
was checked failing against the code with the rule it covers removed.
"""

from datetime import date, datetime, time, timedelta, timezone

import pytest

from core.ledger import ShortTerms
from core.portfolio import MARKET_TZ, MarketHistory, Trade, derive_state, trade_refusal
from core.price_adjustment import Action

FETCHED = datetime(2024, 6, 1, 14, tzinfo=timezone.utc)
SHORTABLE = frozenset({"equity", "etf"})
#: 3.65% a year is 0.0001 a night: easy to multiply by hand.
RATE = 0.0365
TERMS = ShortTerms(borrow_rate=0.0365, max_gross=2.0, maintenance=0.3)


def at(day: date) -> datetime:
    """A trade at 10:00 New York on `day`."""
    return datetime.combine(day, time(10), tzinfo=MARKET_TZ)


def trade(ticker, action, quantity, price, day, costs=0.0):
    return Trade(ticker, action, quantity, price, at(day), costs=costs)


def history(as_of, actions=None, closes=None, rates=()):
    return MarketHistory(actions=actions or {}, closes=closes or {}, rates=list(rates), as_of=as_of)


def flat(ticker, first, last, price=100.0):
    """Weekday closes at `price`."""
    out, day = [], first
    while day <= last:
        if day.weekday() < 5:
            out.append((day, price))
        day += timedelta(days=1)
    return {ticker: out}


def daily_rates(first, last, rate=RATE):
    return [(first + timedelta(days=i), rate) for i in range((last - first).days + 1)]


D0 = date(2024, 1, 2)  # a Tuesday


# ---------------------------------------------------------------------------
# Corporate actions
# ---------------------------------------------------------------------------


def test_a_split_mid_holding_changes_quantity_and_cost_but_not_value():
    h = history(
        date(2024, 1, 20),
        actions={"X": [Action(date(2024, 1, 10), "split", 4.0, FETCHED)]},
    )
    state = derive_state([trade("X", "BUY", 10, 400.0, D0)], 100_000.0, {"X": 100.0}, history=h)
    (x,) = state.positions
    assert x.quantity == 40
    assert x.average_price == 100.0
    assert x.unrealised_pnl == 0.0
    assert state.total_equity == 100_000.0
    # Without the split the 10 shares are marked at the post-split $100.
    stale = derive_state([trade("X", "BUY", 10, 400.0, D0)], 100_000.0, {"X": 100.0})
    assert stale.total_equity == 100_000.0 - 3_000.0


def test_a_long_receives_a_dividend_per_raw_share():
    # Served 0.25 after a later 2:1 split: 0.50 per share as it traded then.
    h = history(
        date(2024, 3, 1),
        actions={
            "X": [
                Action(date(2024, 1, 10), "dividend", 0.25, FETCHED),
                Action(date(2024, 2, 1), "split", 2.0, FETCHED),
            ]
        },
    )
    state = derive_state([trade("X", "BUY", 10, 100.0, D0)], 100_000.0, {"X": 50.0}, history=h)
    assert state.dividends_received == pytest.approx(5.0)
    assert state.cash == pytest.approx(100_000.0 - 1_000.0 + 5.0)
    assert state.positions[0].quantity == 20
    assert [f.kind for f in state.flows] == ["dividend", "split"]


def test_a_short_pays_the_dividend():
    h = history(
        date(2024, 1, 20),
        actions={"X": [Action(date(2024, 1, 10), "dividend", 0.5, FETCHED)]},
    )
    state = derive_state([trade("X", "SELL", 10, 100.0, D0)], 100_000.0, {"X": 100.0}, history=h)
    assert state.dividends_paid == pytest.approx(5.0)
    assert state.cash == pytest.approx(101_000.0 - 5.0)


def test_a_dividend_on_a_split_ex_date_is_paid_on_the_shares_before_the_split():
    # Decision 27: Yahoo serves that day's amount in the post-split basis;
    # raw_dividends converts it back to the share held the night before.
    h = history(
        date(2024, 1, 20),
        actions={
            "X": [
                Action(date(2024, 1, 10), "dividend", 0.25, FETCHED),
                Action(date(2024, 1, 10), "split", 2.0, FETCHED),
            ]
        },
    )
    state = derive_state([trade("X", "BUY", 10, 100.0, D0)], 100_000.0, {"X": 50.0}, history=h)
    assert state.dividends_received == pytest.approx(10 * 0.5)


def test_a_dividend_bought_on_its_ex_date_is_not_received():
    h = history(
        date(2024, 1, 20),
        actions={"X": [Action(date(2024, 1, 10), "dividend", 0.5, FETCHED)]},
    )
    state = derive_state(
        [trade("X", "BUY", 10, 100.0, date(2024, 1, 10))], 100_000.0, {"X": 100.0}, history=h
    )
    assert state.dividends_received == 0.0


def test_a_spinoff_factor_is_listed_not_valued():
    h = history(
        date(2024, 1, 20),
        actions={"X": [Action(date(2024, 1, 10), "manual_factor", 0.77, FETCHED)]},
    )
    state = derive_state([trade("X", "BUY", 10, 100.0, D0)], 100_000.0, {"X": 77.0}, history=h)
    assert [(f.kind, f.amount) for f in state.unhandled_actions] == [("manual_factor", 0.77)]
    assert state.positions[0].quantity == 10


# ---------------------------------------------------------------------------
# Interest and borrow
# ---------------------------------------------------------------------------


def test_interest_accrues_nightly_on_free_cash_only():
    # Short $100k on D0: cash $200k, of which $100k is collateral. Ten nights
    # at 0.0001 on free cash alone, compounding: 100k x (1.0001^10 - 1).
    end = D0 + timedelta(days=10)
    h = history(end, closes=flat("X", D0, end), rates=daily_rates(D0, end))
    state = derive_state([trade("X", "SELL", 1000, 100.0, D0)], 100_000.0, {"X": 100.0}, history=h)
    assert state.interest == pytest.approx(100_000.0 * (1.0001**10 - 1), rel=1e-12)
    assert state.borrow_paid == 0.0  # no terms


def test_cash_earns_from_cash_from_when_that_is_before_the_first_trade():
    # Decision 50: an attached strategy's cash earns from the attach date, as
    # a backtest's does from its first bar, not from its first entry.
    end = D0 + timedelta(days=10)
    rates = daily_rates(D0, end)
    h = MarketHistory(actions={}, closes={}, rates=rates, as_of=end, start=D0)
    buy = [trade("X", "BUY", 1, 100.0, D0 + timedelta(days=4))]
    state = derive_state(buy, 100_000.0, {"X": 100.0}, history=h)
    expected = 100_000.0 * 1.0001**4
    expected = (expected - 100.0) * 1.0001**6 - (100_000.0 - 100.0) + 0.0
    assert state.interest == pytest.approx(expected, rel=1e-12)
    # With no trade at all, the initial cash earns.
    idle = derive_state([], 100_000.0, history=h)
    assert idle.interest == pytest.approx(100_000.0 * (1.0001**10 - 1), rel=1e-12)


def test_no_interest_where_the_rate_is_older_than_a_week():
    end = D0 + timedelta(days=20)
    h = history(end, rates=[(D0, RATE)])
    state = derive_state([trade("X", "BUY", 1, 100.0, D0)], 100_000.0, {"X": 100.0}, history=h)
    nights = 8  # D0 .. D0+7: a rate up to 7 days old
    assert state.interest == pytest.approx(99_900.0 * (1.0001**nights - 1), rel=1e-12)


def test_borrow_is_charged_nightly_on_the_previous_close_weekends_included():
    friday = date(2024, 1, 5)
    monday = date(2024, 1, 8)
    h = history(monday, closes=flat("X", date(2024, 1, 1), monday))
    state = derive_state(
        [trade("X", "SELL", 1000, 100.0, friday)],
        100_000.0,
        {"X": 100.0},
        history=h,
        terms=TERMS,
    )
    # Friday, Saturday and Sunday nights on $100k at 0.0001.
    assert state.borrow_paid == pytest.approx(30.0)
    assert state.cash == pytest.approx(200_000.0 - 30.0)


# ---------------------------------------------------------------------------
# What may be recorded
# ---------------------------------------------------------------------------


def refusal(trades, new, terms=TERMS, asset_class="equity", overdraft=False, prices=None):
    return trade_refusal(
        trades,
        new,
        100_000.0,
        prices or {"X": 100.0, "Y": 100.0},
        history=history(D0 + timedelta(days=1)),
        terms=terms,
        asset_class=asset_class,
        shortable=SHORTABLE,
        allow_overdraft=overdraft,
    )


def test_a_buy_cannot_be_paid_with_short_proceeds():
    short = [trade("X", "SELL", 1000, 100.0, D0)]
    assert refusal(short, trade("Y", "BUY", 1000, 100.0, D0)) is None  # free cash $100k
    reason = refusal(short, trade("Y", "BUY", 1001, 100.0, D0))
    assert "Insufficient cash" in reason and "collateral" in reason


def test_a_short_needs_terms_and_a_shortable_asset():
    assert "no short terms" in refusal([], trade("X", "SELL", 1, 100.0, D0), terms=None)
    assert "crypto" in refusal([], trade("X", "SELL", 1, 100.0, D0), asset_class="crypto")
    # Selling what is held is not a short and needs neither.
    held = [trade("X", "BUY", 10, 100.0, D0)]
    assert refusal(held, trade("X", "SELL", 10, 100.0, D0), terms=None) is None


def test_overdraft_bypasses_the_amounts_but_never_the_terms():
    assert refusal([], trade("X", "BUY", 5000, 100.0, D0), overdraft=True) is None
    assert "no short terms" in refusal(
        [], trade("X", "SELL", 1, 100.0, D0), terms=None, overdraft=True
    )


def test_a_short_past_max_gross_is_refused():
    assert refusal([], trade("X", "SELL", 2000, 100.0, D0)) is None  # gross 2.0x
    assert "Gross exposure" in refusal([], trade("X", "SELL", 2001, 100.0, D0))


def test_below_maintenance_is_flagged():
    # Short $150k on $100k; the price rises to 130: equity 100k - 45k = 55k
    # against 30% of $195k = $58.5k.
    trades = [trade("X", "SELL", 1500, 100.0, D0)]
    state = derive_state(trades, 100_000.0, {"X": 130.0}, terms=TERMS)
    assert state.maintenance_required == pytest.approx(58_500.0)
    assert state.margin_excess == pytest.approx(-3_500.0)
    assert state.below_maintenance
    ok = derive_state(trades, 100_000.0, {"X": 125.0}, terms=TERMS)
    assert not ok.below_maintenance
