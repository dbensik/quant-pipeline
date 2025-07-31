import pandas as pd

from .base_model import BaseAlphaModel


class IndexRebalancingStrategy(BaseAlphaModel):
    """
    A strategy that generates a signal on specified rebalancing dates.

    This model's role is not to decide *what* to buy, but *when* to
    trigger a portfolio rebalance to predefined target weights. The backtester
    is responsible for executing the trades to meet those weights.
    """

    def __init__(self, rebalance_frequency: str = "M"):
        """
        Initializes the strategy with a rebalancing frequency.

        Args:
            rebalance_frequency: A pandas offset string (e.g., 'M' for
                                 month-end, 'Q' for quarter-end, 'W' for
                                 week-end).
        """
        super().__init__()
        self.rebalance_frequency = rebalance_frequency

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates a '2' (rebalance) signal on rebalance dates, '0' otherwise.

        Args:
            price_data: A DataFrame with a DatetimeIndex. The columns are
                        not used, only the index.

        Returns:
            A DataFrame with a 'signal' column (2 for rebalance, 0 for hold).
        """
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0

        # Create a series that is True on the last business day of the period,
        # a common convention for rebalancing.
        resampled_index = price_data.resample(self.rebalance_frequency).last().index

        # Find the intersection of ideal rebalance dates and actual trading days
        valid_rebalance_dates = price_data.index.intersection(resampled_index)

        # Set signal to 2 (rebalance) on those dates
        signals.loc[valid_rebalance_dates, "signal"] = 2.0

        return signals
