import pandas as pd

from .base_model import BaseAlphaModel


class BasketTradingStrategy(BaseAlphaModel):
    """
    A strategy that facilitates trading a custom basket of assets by generating
    signals on specified rebalancing dates.

    This model's role is not to decide *what* to buy, but *when* to
    trigger a portfolio rebalance to predefined target weights for the basket.
    The PortfolioBacktester is responsible for executing the trades to meet those weights.
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
        This signal is applied to all assets in the basket.

        Args:
            price_data: A DataFrame with a DatetimeIndex. The columns are
                        not used, only the index.

        Returns:
            A DataFrame with a 'signal' column (2 for rebalance, 0 for hold).
        """
        signals = pd.DataFrame(index=price_data.index)
        signals["signal"] = 0.0

        # Use pandas to find the last business day of the specified frequency
        # This is a common convention for periodic rebalancing.
        resampled_index = price_data.resample(self.rebalance_frequency).last().index

        # Find the intersection of our ideal rebalance dates and the actual
        # trading days available in the price data.
        valid_rebalance_dates = price_data.index.intersection(resampled_index)

        # Set the signal to 2 (our special code for 'rebalance') on those dates.
        signals.loc[valid_rebalance_dates, "signal"] = 2.0

        return signals
