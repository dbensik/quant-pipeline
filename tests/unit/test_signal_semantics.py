"""
A signal is an EVENT: 1 enters, -1 exits, 0 holds (alpha_models/base_model.py).

Until 2026-10-14 Backtester liquidated on a 0, while PortfolioBacktester
skipped it. Every strategy that emits position.diff() therefore sold on the
bar after it bought in a single-asset backtest: SPY ma_crossover 2016-2025
held ten times, one day each. mean_reversion, rsi and atr_breakout also set
0.0 where they meant "no change", so even read correctly they held only
while the entry condition lasted. Checked failing against both.
"""

import numpy as np
import pandas as pd

from alpha_models.atr_breakout import ATRBreakoutStrategy
from alpha_models.base_model import BaseAlphaModel
from alpha_models.mean_reversion import MeanReversionStrategy
from alpha_models.rsi_strategy import RSIStrategy
from backtesting.backtester import Backtester
from backtesting.portfolio_backtester import PortfolioBacktester
from execution.simulated_handler import SimulatedExecutionHandler
from portfolio.risk_manager import PortfolioRiskManager

EVENTS = [0, 1, 0, 0, 0, -1, 0, 1, 0, -1]


class Fixed(BaseAlphaModel):
    def __init__(self, signals):
        self.signals = signals

    def generate_signals(self, price_data):
        return pd.DataFrame({"signal": self.signals}, index=price_data.index)


def frame(close):
    idx = pd.bdate_range("2024-01-02", periods=len(close), tz="UTC")
    c = pd.Series(close, index=idx, dtype=float)
    return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1e6})


def single(prices, model):
    bt = Backtester(
        initial_capital=100_000.0,
        execution_handler=SimulatedExecutionHandler(seed=1, commission_rate=0.0),
    )
    results = bt.run(
        price_data=prices,
        model=model,
        cash_accrual=np.zeros(len(prices)),
        lot_size=1.0,
    )
    return results, bt.trade_log


def test_a_zero_holds_and_a_minus_one_exits():
    results, trades = single(frame(np.linspace(100, 110, len(EVENTS))), Fixed(EVENTS))
    held = (results["position"] > 0).astype(int).tolist()
    # Each event acts at the NEXT bar's open (decision 31), so holdings trail
    # EVENTS by one bar; the final -1 is on the last bar and never fills.
    assert held == [0, 0, 1, 1, 1, 1, 0, 0, 1, 1]
    assert [t.direction for t in trades] == ["BUY", "SELL", "BUY"]


def test_both_backtesters_trade_on_the_same_bars():
    prices = frame(np.linspace(100, 110, len(EVENTS)))
    _, single_trades = single(prices, Fixed(EVENTS))
    bt = PortfolioBacktester(
        initial_capital=100_000.0,
        execution_handler=SimulatedExecutionHandler(seed=1, commission_rate=0.0),
        risk_manager=PortfolioRiskManager(max_trade_risk_pct=1.0, max_portfolio_drawdown_pct=0.99),
    )
    _, basket_trades = bt.run(
        {"A": prices},
        {"A": pd.DataFrame({"signal": EVENTS}, index=prices.index)},
        {"A": 1.0},
        cash_accrual=pd.Series(np.zeros(len(prices)), index=prices.index),
        lot_sizes={"A": 1.0},
    )
    assert [(t.timestamp, t.direction) for t in single_trades] == list(
        zip(pd.to_datetime(basket_trades["timestamp"]), basket_trades["direction"], strict=True)
    )


def held_bars(model, close):
    signal = model.generate_signals(frame(close))["signal"]
    return signal.cumsum().clip(0, 1)


def test_mean_reversion_holds_until_the_mean():
    # 40 flat-ish bars, a drop below -1 sd, then a slow climb that stays
    # under the mean for several bars before crossing it.
    rng = np.random.default_rng(0)
    close = list(100 + rng.normal(0, 0.5, 40)) + [96.0, 97.0, 98.0, 98.5, 99.0, 99.5, 102, 103]
    pos = held_bars(MeanReversionStrategy(window=20, threshold=1.0), close)
    z = (pd.Series(close) - pd.Series(close).rolling(20).mean()) / pd.Series(close).rolling(
        20
    ).std()
    entry = int(np.argmax(z.to_numpy() < -1.0))
    exit_ = entry + int(np.argmax(z.to_numpy()[entry:] > 0))
    assert exit_ - entry > 1, "fixture must hold for more than the entry bar"
    assert pos.iloc[entry:exit_].eq(1).all() and pos.iloc[exit_] == 0


def test_rsi_holds_until_the_sell_threshold():
    close = (
        [100 - i for i in range(20)]
        + [80 + 0.5 * i for i in range(8)]
        + [84 + 3 * i for i in range(10)]
    )
    model = RSIStrategy(window=14, buy_threshold=30, sell_threshold=70)
    rsi = model.calculate_rsi(pd.Series(close))
    pos = held_bars(model, close)
    entry = int(np.argmax(rsi.to_numpy() < 30))
    exit_ = entry + int(np.argmax(rsi.to_numpy()[entry:] > 70))
    assert (rsi.iloc[entry:exit_] >= 30).any(), "fixture must leave the buy zone while long"
    assert pos.iloc[entry:exit_].eq(1).all() and pos.iloc[exit_] == 0


def test_atr_breakout_holds_until_below_the_sma():
    close = [100.0] * 20 + [110.0, 108.0, 106.0, 104.0, 103.0, 99.0, 99.0]
    df = frame(close)
    df["High"], df["Low"] = df["Close"] + 1, df["Close"] - 1
    model = ATRBreakoutStrategy(window=14, multiplier=2.0)
    pos = model.generate_signals(df)["signal"].cumsum().clip(0, 1).reset_index(drop=True)
    sma = pd.Series(close).rolling(14).mean()
    upper = sma + 2.0 * model.calculate_atr(df).reset_index(drop=True)
    entry = int(np.argmax((pd.Series(close) > upper).to_numpy()))
    exit_ = entry + int(np.argmax((pd.Series(close) < sma).to_numpy()[entry:]))
    assert (pd.Series(close).iloc[entry + 1 : exit_] <= upper.iloc[entry + 1 : exit_]).any()
    assert pos.iloc[entry:exit_].eq(1).all() and pos.iloc[exit_] == 0


def test_the_random_forest_never_sees_the_bar_it_predicts():
    # Retrained on EVERY bar, so each cut below is a retrain point: training
    # on the row being predicted (its target is the next bar's return) or
    # anchoring the schedule to the end would change an earlier signal.
    from alpha_models.ml_random_forest import RandomForestStrategy

    rng = np.random.default_rng(3)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, 120)))
    df = frame(close)

    def model():
        return RandomForestStrategy(
            n_estimators=5, lookback_window=3, train_window=30, retrain_every=1
        )

    full = model().generate_signals(df)["signal"]
    assert full.abs().sum() > 0, "fixture must trade"
    for cut in (70, 85, 100, 119):
        part = model().generate_signals(df.iloc[:cut])["signal"]
        assert part.equals(full.iloc[:cut]), f"signals before bar {cut} changed"
