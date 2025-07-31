import pandas as pd

from .base_model import BaseAlphaModel


class BuyAndHoldStrategy(BaseAlphaModel):
    """
    A simple strategy that buys on the first day and holds until the end.
    """

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates a signal to be long (1) for the entire duration of the data.

        Args:
            price_data: DataFrame of historical price data. Although not used
                        for calculation, it's required by the backtester's
                        interface and is used to set the signal's index.

        Returns:
            A DataFrame with a 'signal' column containing '1' for all dates.
        """
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 1  # Signal to be long (hold)
        return signals
