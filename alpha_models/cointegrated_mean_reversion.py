import pandas as pd
from .base_model import BaseAlphaModel


class CointegratedMeanReversionStrategy(BaseAlphaModel):
    """
    A strategy that trades the mean-reverting spread of a cointegrated portfolio.
    """

    def __init__(self, weights: dict, window: int = 20, threshold: float = 2.0):
        if not weights:
            raise ValueError("Weights dictionary cannot be empty.")
        self.weights = weights
        self.window = window
        self.threshold = threshold
        self.tickers = list(self.weights.keys())

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates trading signals based on the Z-score of the portfolio spread.

        Args:
            price_data: A DataFrame with 'Close' prices for all assets in the portfolio.

        Returns:
            A DataFrame with a single 'signal' column for the entire portfolio.
        """
        # Ensure we only use the assets we have weights for
        valid_tickers = [t for t in self.tickers if t in price_data.columns]
        portfolio_prices = price_data[valid_tickers]

        # Calculate the weighted portfolio spread
        spread = (portfolio_prices * pd.Series(self.weights)).sum(axis=1)

        # Calculate the Z-score of the spread
        mean = spread.rolling(window=self.window).mean()
        std = spread.rolling(window=self.window).std()
        z_score = (spread - mean) / std

        # Generate signals
        signals = pd.DataFrame(index=z_score.index)
        signals["z_score"] = z_score
        signals["signal"] = 0

        # Long signal
        signals.loc[signals["z_score"] < -self.threshold, "signal"] = 1
        # Short signal
        signals.loc[signals["z_score"] > self.threshold, "signal"] = -1

        # Fill forward to hold positions, but handle exits
        # This is a simplified exit logic; more complex logic could be used
        signals["signal"] = signals["signal"].ffill().fillna(0)

        # Exit when crossing back over the mean
        is_long = (
            (signals["signal"].shift(1) == 1)
            & (signals["z_score"].shift(1) < 0)
            & (signals["z_score"] >= 0)
        )
        is_short = (
            (signals["signal"].shift(1) == -1)
            & (signals["z_score"].shift(1) > 0)
            & (signals["z_score"] <= 0)
        )
        signals.loc[is_long | is_short, "signal"] = 0

        return signals[["signal"]]  # Return only the final signal column
