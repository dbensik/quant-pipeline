import pandas as pd

from .base_model import BaseAlphaModel


class MovingAverageCrossoverStrategy(BaseAlphaModel):
    """
    A strategy that generates trading signals based on the crossover of two
    simple moving averages (SMAs).
    """

    def __init__(self, short_window: int = 40, long_window: int = 100):
        """
        Initializes the strategy with short and long lookback windows.

        Args:
            short_window: The lookback period for the shorter-term moving average.
            long_window: The lookback period for the longer-term moving average.
        """
        super().__init__()
        if short_window <= 0 or long_window <= 0:
            raise ValueError("Window parameters must be positive integers.")
        if short_window >= long_window:
            raise ValueError("The short window must be smaller than the long window.")
        self.short_window = short_window
        self.long_window = long_window

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates buy/sell signals based on the crossover of two SMAs.

        A "buy" signal (1) is generated when the short-term MA crosses above the long-term MA.
        A "sell" signal (-1) is generated when the short-term MA crosses below the long-term MA.

        Args:
            price_data: A DataFrame with a 'Close' price column.

        Returns:
            A DataFrame with a 'signal' column (1 for buy, -1 for sell, 0 for hold).
        """
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0

        # Calculate the short and long moving averages
        signals["short_mavg"] = (
            price_data["Close"]
            .rolling(window=self.short_window, min_periods=1, center=False)
            .mean()
        )
        signals["long_mavg"] = (
            price_data["Close"]
            .rolling(window=self.long_window, min_periods=1, center=False)
            .mean()
        )

        # --- Determine the desired state (position) ---
        # We want to be LONG (position=1) when the short MA is above the long MA.
        # We want to be FLAT (position=0) otherwise.
        # This creates a boolean series (True/False) which is then converted to float (1.0/0.0).
        signals["position"] = (signals["short_mavg"] > signals["long_mavg"]).astype(
            float
        )

        # --- Convert positions (states) to signals (actions) ---
        # A signal is the change in position from the previous day.
        # .diff() will be 1.0 for a buy, -1.0 for a sell, and 0.0 for no change.
        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["position"].diff().fillna(0)

        return final_signals
