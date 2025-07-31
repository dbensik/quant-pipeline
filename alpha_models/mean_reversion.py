import numpy as np
import pandas as pd

from .base_model import BaseAlphaModel


class MeanReversionStrategy(BaseAlphaModel):
    """
    A strategy that generates trading signals based on the assumption that
    an asset's price will revert to its historical mean.
    """

    def __init__(self, window: int = 20, threshold: float = 1.5):
        """
        Initializes the strategy with a lookback window and a Z-score threshold.

        Args:
            window: The lookback period for calculating the moving average and standard deviation.
            threshold: The Z-score level at which to generate signals.
        """
        super().__init__()
        if window <= 0:
            raise ValueError("Window must be a positive integer.")
        if threshold <= 0:
            raise ValueError("Threshold must be a positive number.")
        self.window = window
        self.threshold = threshold

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates buy/sell signals based on the Z-score of the price.

        A "buy" signal (1) is generated when the price is significantly below the mean.
        A "sell" signal (-1) is generated when the position is exited as the price
        reverts back towards the mean.

        Args:
            price_data: A DataFrame with a 'Close' price column.

        Returns:
            A DataFrame with a 'signal' column (1 for buy, -1 for sell, 0 for hold).
        """
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0

        # Calculate the moving average and standard deviation
        signals["moving_avg"] = price_data["Close"].rolling(window=self.window).mean()
        signals["moving_std"] = price_data["Close"].rolling(window=self.window).std()

        # Calculate the Z-score
        signals["z_score"] = (price_data["Close"] - signals["moving_avg"]) / signals[
            "moving_std"
        ]

        # --- Determine the desired state (position) ---
        # We want to be LONG (position=1) when the price is oversold (Z-score is very low).
        # We want to be FLAT (position=0) when the price is overbought or has reverted to the mean.
        # This strategy does not take short positions.
        signals["position"] = np.where(signals["z_score"] < -self.threshold, 1.0, 0.0)

        # Exit the position if the Z-score crosses back above the mean (e.g., 0)
        signals["position"] = np.where(signals["z_score"] > 0, 0.0, signals["position"])

        # Forward-fill the position to hold it until an exit signal is generated
        signals["position"] = signals["position"].ffill().fillna(0)

        # --- Convert positions (states) to signals (actions) ---
        # A signal is the change in position from the previous day.
        # .diff() will be 1 for a buy, -1 for a sell, and 0 for no change.
        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["position"].diff().fillna(0)

        return final_signals
