"""
Short positions in PortfolioBacktester through target signals (decision 48,
research/short-engine-2026-10-19.md), with the accounting in core/ledger.py.

Fills are exact and free (the price is the test) unless a test says
otherwise, and the risk manager is off (1.0, 1.0): its 2% default cap would
shrink every opening. Each expectation is worked by hand in the test. Each
test was checked failing against code with the rule it covers removed.
"""

import numpy as np
import pandas as pd
import pytest

from backtesting.events import FillEvent
from backtesting.portfolio_backtester import PortfolioBacktester
from backtesting.risk_manager import PortfolioRiskManager
from core.ledger import ShortTerms, margin_cut
from execution.simulated_handler import SimulatedExecutionHandler

CAPITAL = 100_000.0
TERMS = ShortTerms(borrow_rate=0.01, max_gross=2.0, maintenance=0.3)


class ExactFills(SimulatedExecutionHandler):
    def __init__(self):
        super().__init__(commission_per_trade=0.0, commission_rate=0.0)

    def execute_order(self, order, current_price):
        return FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=current_price,
            commission=0.0,
        )


def bars(closes, start="2024-01-01", freq="D", opens=None):
    idx = pd.date_range(start, periods=len(closes), freq=freq, tz="UTC")
    close = pd.Series(closes, index=idx, dtype=float)
    open_ = close if opens is None else pd.Series(opens, index=idx, dtype=float)
    return pd.DataFrame(
        {"Open": open_, "High": close, "Low": close, "Close": close, "Volume": 1e6}, index=idx
    )


def targets(frame, values):
    return pd.DataFrame({"target": values}, index=frame.index, dtype=float)


def engine(risk=None):
    return PortfolioBacktester(
        initial_capital=CAPITAL,
        execution_handler=ExactFills(),
        risk_manager=risk or PortfolioRiskManager(1.0, 1.0),
    )


def run(prices, signals, terms=TERMS, accrual=0.0, risk=None):
    bt = engine(risk)
    index = next(iter(prices.values())).index
    portfolio, trades = bt.run(
        prices,
        signals,
        {t: 0.0 for t in prices},
        cash_accrual=pd.Series(accrual, index=index),
        lot_sizes={t: 1.0 for t in prices},
        short_terms=terms,
    )
    return bt, portfolio, trades


def flat_pair(closes_a, closes_b=None):
    a = bars(closes_a)
    b = bars(closes_b if closes_b is not None else [100.0] * len(closes_a))
    return {"A": a, "B": b}


def hold(n, first, rest=np.nan):
    return [first] + [rest] * (n - 1)


# ---------------------------------------------------------------------------
# Opening a short, and borrow
# ---------------------------------------------------------------------------


def test_a_negative_target_from_flat_opens_a_short_at_the_next_open():
    prices = flat_pair([100.0] * 5)
    _, portfolio, trades = run(
        prices,
        {"A": targets(prices["A"], hold(5, -1.0)), "B": targets(prices["B"], [np.nan] * 5)},
        terms=ShortTerms(borrow_rate=0.0, max_gross=2.0, maintenance=0.3),
    )
    assert len(trades) == 1
    assert trades.iloc[0]["direction"] == "SELL"
    assert trades.iloc[0]["timestamp"] == prices["A"].index[1]
    assert portfolio["A_pos"].iloc[-1] == -1000.0
    # Proceeds are collateral: equity is unchanged at a constant price.
    assert portfolio["cash"].iloc[-1] == 200_000.0
    assert portfolio["total"].iloc[-1] == CAPITAL


def test_borrow_matches_the_hand_computation():
    # 1,000 shares short at $100 from day 1 to day 365: borrow accrues on
    # the previous close over 364 calendar days at 1%.
    prices = flat_pair([100.0] * 366)
    _, portfolio, _ = run(
        prices,
        {"A": targets(prices["A"], hold(366, -1.0)), "B": targets(prices["B"], [np.nan] * 366)},
    )
    borrow = 1000 * 100.0 * 0.01 * 364 / 365  # $997.26
    assert portfolio["borrow_paid"].iloc[-1] == pytest.approx(borrow, rel=1e-12)
    assert portfolio["total"].iloc[-1] == pytest.approx(CAPITAL - borrow, rel=1e-12)


def test_borrow_counts_calendar_days_so_a_weekend_costs_three():
    prices = {
        "A": bars([100.0] * 6, start="2024-01-04", freq="B"),  # Thu..Thu
        "B": bars([100.0] * 6, start="2024-01-04", freq="B"),
    }
    _, portfolio, _ = run(
        prices,
        {"A": targets(prices["A"], hold(6, -1.0)), "B": targets(prices["B"], [np.nan] * 6)},
    )
    # Filled Friday; charged Mon (3 days), Tue, Wed, Thu (1 each) = 6 days.
    assert portfolio["borrow_paid"].iloc[-1] == pytest.approx(100_000 * 0.01 * 6 / 365)


def test_a_short_on_a_falling_series_profits_net_of_borrow():
    closes = list(np.linspace(100.0, 50.0, 51))
    prices = flat_pair(closes)
    _, portfolio, _ = run(
        prices,
        {"A": targets(prices["A"], hold(51, -1.0)), "B": targets(prices["B"], [np.nan] * 51)},
    )
    entry = closes[1]
    quantity = int(CAPITAL / entry)
    borrow = sum(quantity * closes[i - 1] * 0.01 / 365 for i in range(2, 51))
    expected = CAPITAL + quantity * (entry - closes[-1]) - borrow
    assert portfolio["total"].iloc[-1] == pytest.approx(expected, rel=1e-12)
    assert portfolio["total"].iloc[-1] > 1.45 * CAPITAL


def test_interest_is_earned_on_free_cash_not_on_the_collateral():
    prices = flat_pair([100.0] * 11)
    _, portfolio, _ = run(
        prices,
        {"A": targets(prices["A"], hold(11, -1.0)), "B": targets(prices["B"], [np.nan] * 11)},
        terms=ShortTerms(borrow_rate=0.0, max_gross=2.0, maintenance=0.3),
        accrual=0.001,
    )
    # Bar 1 earns on $100k before the short; bars 2-10 on free cash only,
    # which compounds on itself: $100k x 1.001^10. On all $200k of cash it
    # would be ~$1,800 more.
    assert portfolio["total"].iloc[-1] == pytest.approx(CAPITAL * 1.001**10, rel=1e-12)


# ---------------------------------------------------------------------------
# Buying power and margin
# ---------------------------------------------------------------------------


def test_gross_exposure_never_exceeds_max_gross():
    # Long A for the equity, then short B: at max_gross 1.5 only $50k of
    # the $100k short fits.
    prices = flat_pair([100.0] * 5)
    _, portfolio, _ = run(
        prices,
        {"A": targets(prices["A"], hold(5, 1.0)), "B": targets(prices["B"], hold(5, -1.0))},
        terms=ShortTerms(borrow_rate=0.0, max_gross=1.5, maintenance=0.3),
    )
    assert (portfolio["gross"] <= 1.5 * portfolio["total"] + 1e-9).all()
    assert portfolio["B_pos"].iloc[-1] == -500.0
    assert portfolio["gross"].iloc[-1] == pytest.approx(150_000.0)


def test_a_long_is_paid_from_free_cash_not_from_short_proceeds():
    # Short B for the equity, then grow A in two steps. Cash is $200k but
    # $100k of it is B's collateral: the second step finds $20k, not $120k.
    prices = flat_pair([100.0] * 5)
    _, portfolio, _ = run(
        prices,
        {
            "A": targets(prices["A"], [np.nan, 0.8, 1.6, np.nan, np.nan]),
            "B": targets(prices["B"], hold(5, -1.0)),
        },
        terms=ShortTerms(borrow_rate=0.0, max_gross=3.0, maintenance=0.3),
    )
    assert portfolio["A_pos"].iloc[-1] == 1000.0
    assert portfolio["cash"].iloc[-1] == pytest.approx(100_000.0)


def test_margin_cut_restores_maintenance_pro_rata():
    assert margin_cut(40_000.0, 160_000.0, 0.3) == pytest.approx(1 / 6)
    assert margin_cut(48_000.0, 160_000.0, 0.3) == 0.0
    assert margin_cut(-1.0, 160_000.0, 0.3) == 1.0


def test_a_breach_at_the_close_covers_at_the_next_open():
    # Short 1,000 at 100. At 160, equity is $40k against 30% of $160k =
    # $48k: cut 1/6, so 167 shares are covered at the next open.
    closes = [100.0, 100.0, 120.0, 160.0, 160.0, 160.0]
    prices = flat_pair(closes)
    _, portfolio, trades = run(
        prices,
        {"A": targets(prices["A"], hold(6, -1.0)), "B": targets(prices["B"], [np.nan] * 6)},
        terms=ShortTerms(borrow_rate=0.0, max_gross=2.0, maintenance=0.3),
    )
    margin = trades[trades["reason"] == "margin"]
    assert len(margin) == 1
    assert margin.iloc[0]["timestamp"] == prices["A"].index[4]
    assert margin.iloc[0]["direction"] == "BUY"
    assert margin.iloc[0]["quantity"] == 167
    assert portfolio["A_pos"].iloc[-1] == -833.0
    assert portfolio["total"].iloc[-1] >= 0.3 * portfolio["gross"].iloc[-1]


def test_equity_at_zero_is_ruin_and_the_run_stops():
    closes = [100.0, 100.0, 250.0, 250.0, 90.0]
    prices = flat_pair(closes)
    bt, portfolio, _ = run(
        prices,
        {"A": targets(prices["A"], hold(5, -1.0)), "B": targets(prices["B"], [np.nan] * 5)},
        terms=ShortTerms(borrow_rate=0.0, max_gross=2.0, maintenance=0.3),
    )
    assert bt.ruined_at == prices["A"].index[2]
    assert (portfolio["total"].iloc[2:] == 0.0).all()


def test_a_drawdown_halt_never_blocks_a_cover():
    # The halt stops new BUYS; a cover is a buy. Blocked, the short would
    # be held while the price rises without limit.
    closes = [100.0, 100.0, 110.0, 120.0, 120.0]
    prices = flat_pair(closes)
    _, portfolio, trades = run(
        prices,
        {
            "A": targets(prices["A"], [-1.0, np.nan, np.nan, 0.0, np.nan]),
            "B": targets(prices["B"], [np.nan] * 5),
        },
        terms=ShortTerms(borrow_rate=0.0, max_gross=2.0, maintenance=0.3),
        risk=PortfolioRiskManager(max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=0.05),
    )
    assert portfolio["A_pos"].iloc[-1] == 0.0
    assert list(trades["direction"]) == ["SELL", "BUY"]


def test_a_flip_from_long_to_short_is_two_fills():
    prices = flat_pair([100.0] * 5)
    _, portfolio, trades = run(
        prices,
        {
            "A": targets(prices["A"], [0.5, -0.5, np.nan, np.nan, np.nan]),
            "B": targets(prices["B"], [np.nan] * 5),
        },
    )
    flip = trades[trades["timestamp"] == prices["A"].index[2]]
    assert list(flip["direction"]) == ["SELL", "SELL"]
    assert list(flip["quantity"]) == [500, 500]
    assert portfolio["A_pos"].iloc[-1] == -500.0


def test_nan_keeps_the_quantity_while_equity_drifts():
    closes = [100.0, 100.0, 110.0, 120.0, 130.0]
    prices = flat_pair(closes)
    _, _, trades = run(
        prices,
        {"A": targets(prices["A"], hold(5, 0.5)), "B": targets(prices["B"], [np.nan] * 5)},
        terms=None,
    )
    assert len(trades) == 1


# ---------------------------------------------------------------------------
# Refusals, and events unchanged
# ---------------------------------------------------------------------------


def test_an_event_minus_one_from_flat_still_does_nothing():
    prices = flat_pair([100.0] * 5)
    events = pd.DataFrame({"signal": [-1, 0, 0, 0, 0]}, index=prices["A"].index)
    bt = engine()
    _, trades = bt.run(
        prices,
        {"A": events, "B": events * 0},
        {"A": 0.5, "B": 0.5},
        cash_accrual=pd.Series(0.0, index=prices["A"].index),
        lot_sizes={"A": 1.0, "B": 1.0},
    )
    assert trades.empty


def test_a_negative_target_without_short_terms_is_refused():
    prices = flat_pair([100.0] * 5)
    with pytest.raises(ValueError, match="short_terms"):
        run(
            prices,
            {"A": targets(prices["A"], hold(5, -1.0)), "B": targets(prices["B"], [np.nan] * 5)},
            terms=None,
        )


def test_mixed_target_and_event_frames_are_refused():
    prices = flat_pair([100.0] * 5)
    events = pd.DataFrame({"signal": [0] * 5}, index=prices["A"].index)
    with pytest.raises(ValueError, match="mix"):
        run(prices, {"A": targets(prices["A"], hold(5, -1.0)), "B": events})


def test_short_terms_with_event_signals_are_refused():
    prices = flat_pair([100.0] * 5)
    events = pd.DataFrame({"signal": [0] * 5}, index=prices["A"].index)
    with pytest.raises(ValueError, match="events cannot short"):
        run(prices, {"A": events, "B": events})


@pytest.mark.parametrize(
    "kwargs",
    [
        {"borrow_rate": -0.01, "max_gross": 2.0, "maintenance": 0.3},
        {"borrow_rate": 0.01, "max_gross": 0.0, "maintenance": 0.3},
        {"borrow_rate": 0.01, "max_gross": 2.0, "maintenance": 0.0},
        {"borrow_rate": 0.01, "max_gross": 4.0, "maintenance": 0.3},
    ],
)
def test_short_terms_that_cannot_hold_are_refused(kwargs):
    with pytest.raises(ValueError):
        ShortTerms(**kwargs)
