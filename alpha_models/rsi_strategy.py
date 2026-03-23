import numpy as np
import pandas as pd
from .base_model import BaseAlphaModel

class RSIStrategy(BaseAlphaModel):
    """
    RSI Oscillator Strategy (Mean Reversion / Momentum Exhaustion).
    
    Logic:
    - Buy when RSI < buy_threshold (Oversold, expect reversion up)
    - Sell when RSI > sell_threshold (Overbought, expect reversion down)
    """

    def __init__(self, window: int = 14, buy_threshold: int = 30, sell_threshold: int = 70):
        self.window = window
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def calculate_rsi(self, data: pd.Series) -> pd.Series:
        """Helper to calculate RSI using pandas."""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)

        # Exponential Moving Average for RSI (Wilder's Smoothing)
        avg_gain = gain.ewm(com=self.window - 1, min_periods=self.window).mean()
        avg_loss = loss.ewm(com=self.window - 1, min_periods=self.window).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0
        
        # 1. Calculate RSI
        signals["rsi"] = self.calculate_rsi(price_data["Close"])

        # 2. Logic Stub
        # Implement Logic:
        # if rsi < buy_threshold -> 1 (Long)
        # if rsi > sell_threshold -> 0 (Flat) or -1 (Short)
        
        # Example Implementation (Long Only)
        signals["signal"] = np.where(signals["rsi"] < self.buy_threshold, 1.0, 0.0)
        signals["signal"] = np.where(signals["rsi"] > self.sell_threshold, 0.0, signals["signal"])

        # Convert state to discrete signals
        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["signal"].diff().fillna(0)
        
        return final_signals
