import numpy as np
import pandas as pd

from .base_model import BaseAlphaModel


class PairsTradingStrategy(BaseAlphaModel):
    """
    A classic pairs trading strategy based on the statistical mean-reversion
    of the spread between two cointegrated assets.

    This strategy calculates the Z-score of the price ratio (spread) and
    generates signals to long the spread when it's oversold (low Z-score)
    and short the spread when it's overbought (high Z-score).
    """

    def __init__(self, window: int = 20, threshold: float = 2.0):
        """
        Initializes the strategy with a lookback window and a Z-score threshold.

        Args:
            window: The lookback period for calculating the moving average and
                    standard deviation of the spread.
            threshold: The Z-score level at which to generate entry signals.
        """
        super().__init__()
        if window <= 1:
            raise ValueError("Window must be greater than 1.")
        if threshold <= 0:
            raise ValueError("Threshold must be a positive number.")
        self.window = window
        self.threshold = threshold

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates simultaneous long and short signals for a pair of assets.

        Args:
            price_data: A DataFrame with exactly two 'Close' price columns,
                        one for each asset in the pair.

        Returns:
            A DataFrame with signal columns for each of the two assets.
            - For asset1: 1=buy, -1=sell/short.
            - For asset2: 1=buy, -1=sell/short.
        """
        if len(price_data.columns) != 2:
            raise ValueError(
                "PairsTradingStrategy requires a DataFrame with exactly two price columns."
            )

        asset1, asset2 = price_data.columns[0], price_data.columns[1]

        # --- 1. Calculate the Spread and its Z-score ---
        spread = price_data[asset1] / price_data[asset2]
        mean_spread = spread.rolling(window=self.window).mean()
        std_spread = spread.rolling(window=self.window).std()
        z_score = (spread - mean_spread) / std_spread

        # --- 2. Determine the desired state (position) for each asset ---
        # This DataFrame will hold the desired position for each asset in the pair.
        positions = pd.DataFrame(index=price_data.index)
        positions[asset1] = 0.0
        positions[asset2] = 0.0

        # Long the spread (Buy asset1, Sell asset2) when Z-score is low
        positions[asset1] = np.where(z_score < -self.threshold, 1.0, positions[asset1])
        positions[asset2] = np.where(z_score < -self.threshold, -1.0, positions[asset2])

        # Short the spread (Sell asset1, Buy asset2) when Z-score is high
        positions[asset1] = np.where(z_score > self.threshold, -1.0, positions[asset1])
        positions[asset2] = np.where(z_score > self.threshold, 1.0, positions[asset2])

        # Exit position when Z-score crosses the mean (e.g., |Z-score| < 0.5)
        exit_condition = np.abs(z_score) < 0.5
        positions[asset1] = np.where(exit_condition, 0.0, positions[asset1])
        positions[asset2] = np.where(exit_condition, 0.0, positions[asset2])

        # Forward-fill positions to hold until an exit signal
        positions = positions.ffill().fillna(0)

        # --- 3. Convert positions (states) to signals (actions) ---
        # A signal is the change in position from the previous day.
        final_signals = positions.diff().fillna(0)

        return final_signals
