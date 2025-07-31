import numpy as np
import pandas as pd

from .base_model import BaseAlphaModel


# from tqdm import trange  # Using tqdm for progress visualization in long backtests


class PushResponseStrategy(BaseAlphaModel):
    """
    Implements a trading strategy based on the "Push-Response" concept.

    This version is refactored to be a stateful, walk-forward model that
    conforms to the BaseAlphaModel interface, making it compatible with the
    existing backtesting engine.
    """

    def __init__(
        self,
        tau: int,
        training_window: int,
        num_bins: int = 50,
        threshold: float = 0.0,
    ):
        """
        Initializes the PushResponseStrategy.

        Args:
            tau (int): The time interval for the push and response.
            training_window (int): The size of the rolling window of data to use for fitting the model.
            num_bins (int): The number of bins to discretize push values.
            threshold (float): The minimum expected response to trigger a signal.
        """
        super().__init__()
        if tau < 1 or training_window < 1:
            raise ValueError("tau and training_window must be positive integers.")
        if num_bins < 2:
            raise ValueError("num_bins must be at least 2.")

        self.tau = tau
        self.training_window = training_window
        self.num_bins = num_bins
        self.threshold = threshold
        self.model = None  # Stores the trained model

    def _fit(self, prices: pd.Series):
        """
        Internal method to train the push-response model on a window of historical data.
        """
        df = pd.DataFrame({"price": prices})
        df["push"] = df["price"].diff(self.tau)
        df["response"] = df["price"].diff(self.tau).shift(-self.tau)
        df.dropna(inplace=True)

        if df.empty or len(df) < self.num_bins:
            self.model = None  # Not enough data to fit
            return

        # Use qcut for more robust binning based on quantiles
        df["push_bin"], bin_edges = pd.qcut(
            df["push"], q=self.num_bins, labels=False, retbins=True, duplicates="drop"
        )

        model_data = df.groupby("push_bin")["response"].mean()

        self.model = {
            "bin_edges": bin_edges,
            "expected_responses": model_data,
        }

    def _predict(self, recent_prices: pd.Series) -> int:
        """
        Internal method to generate a signal based on the latest push.
        """
        if self.model is None or len(recent_prices) < self.tau + 1:
            return 0

        latest_push = recent_prices.iloc[-1] - recent_prices.iloc[-1 - self.tau]
        bin_index = np.digitize(latest_push, self.model["bin_edges"]) - 1

        # Handle cases where the push is outside the learned range
        if bin_index < 0 or bin_index >= len(self.model["expected_responses"]):
            return 0

        expected_response = self.model["expected_responses"].iloc[bin_index]

        if expected_response > self.threshold:
            return 1
        elif expected_response < -self.threshold:
            return -1
        else:
            return 0

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates signals using a walk-forward methodology.

        This method iterates through the price data, periodically refitting the
        model on a rolling window of past data and generating a signal for the next step.
        """
        prices = price_data["Close"]
        signals = pd.Series(index=prices.index, dtype=float).fillna(0.0)

        # Start generating signals only after the first training window is available
        start_index = self.training_window + self.tau
        if len(prices) < start_index:
            return pd.DataFrame({"signal": signals})  # Not enough data

        print("Generating signals with walk-forward Push-Response model...")
        # Use trange for a progress bar in the console
        for i in range(start_index, len(prices)):
            # Define the training data for this step
            training_series = prices.iloc[i - self.training_window : i]

            # Fit the model on the training data
            self._fit(training_series)

            # Predict the signal for the current time 'i' using data up to 'i-1'
            # The push is calculated from prices at i-1 and i-1-tau
            prediction_series = prices.iloc[i - self.tau - 1 : i]
            signals.iloc[i] = self._predict(prediction_series)

        return pd.DataFrame({"signal": signals})
