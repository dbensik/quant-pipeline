import numpy as np
import pandas as pd

from .base_model import BaseAlphaModel


class TrendFollowingStrategy(BaseAlphaModel):
    """
    A basic trend-following strategy that goes long when the price is above
    its moving average and goes flat when it falls below.
    """

    def __init__(self, window: int = 50):
        """
        Initializes the strategy with a lookback window for the moving average.

        Args:
            window: The lookback period for calculating the moving average.
        """
        super().__init__()
        if window <= 0:
            raise ValueError("Window must be a positive integer.")
        self.window = window

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates buy/sell signals based on the price's position relative to its moving average.

        A "buy" signal (1) is generated when the price crosses above the moving average.
        A "sell" signal (-1) is generated when the price crosses below the moving average.

        Args:
            price_data: A DataFrame with a 'Close' price column.

        Returns:
            A DataFrame with a 'signal' column (1 for buy, -1 for sell, 0 for hold).
        """
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0

        # Calculate the moving average
        signals["moving_avg"] = price_data["Close"].rolling(window=self.window).mean()

        # --- Determine the desired state (position) ---
        # We want to be LONG (position=1) when the price is above the moving average.
        # We want to be FLAT (position=0) otherwise.
        signals["position"] = np.where(
            price_data["Close"] > signals["moving_avg"], 1.0, 0.0
        )

        # --- Convert positions (states) to signals (actions) ---
        # A signal is the change in position from the previous day.
        # .diff() will be 1 for a buy, -1 for a sell, and 0 for no change.
        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["position"].diff().fillna(0)

        return final_signals
