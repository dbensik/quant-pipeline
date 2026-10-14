import numpy as np
import pandas as pd

from .base_model import BaseAlphaModel


class ATRBreakoutStrategy(BaseAlphaModel):
    """
    Volatility Expansion Breakout Strategy.

    Logic:
    - ENTER Long if Price > rolling_mean + (multiplier * ATR)
    - EXIT if Price < rolling_mean
    """

    def __init__(self, window: int = 20, multiplier: float = 2.0):
        self.window = window
        self.multiplier = multiplier

    def calculate_atr(self, df: pd.DataFrame) -> pd.Series:
        high_low = df["High"] - df["Low"]
        high_close = np.abs(df["High"] - df["Close"].shift())
        low_close = np.abs(df["Low"] - df["Close"].shift())

        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)

        return true_range.rolling(window=self.window).mean()

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0

        # 1. Indicators
        signals["atr"] = self.calculate_atr(price_data)
        signals["sma"] = price_data["Close"].rolling(window=self.window).mean()

        upper_band = signals["sma"] + (self.multiplier * signals["atr"])

        # 2. Logic (Breakout)
        # 1 = Long, 0 = Flat
        # NaN between the band and the SMA, so the forward fill below holds the
        # trade (0.0 until 2026-10-14: long only while above the band).
        signals["position"] = np.where(price_data["Close"] > upper_band, 1.0, np.nan)

        # Optional: Trailing stop or mean reversion exit?
        # For simple template, exit if price drops below SMA
        signals["position"] = np.where(
            price_data["Close"] < signals["sma"], 0.0, signals["position"]
        )

        # Forward fill position to hold trade
        signals["position"] = signals["position"].ffill().fillna(0)

        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["position"].diff().fillna(0)

        return final_signals
