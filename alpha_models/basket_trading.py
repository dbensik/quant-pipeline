import pandas as pd

from .base_model import BaseAlphaModel
from .rebalancing import rebalance_dates


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

        # Was:
        #     resampled = price_data.resample(self.rebalance_frequency).last().index
        #     dates = price_data.index.intersection(resampled)
        # which SILENTLY SKIPPED rebalances. `resample` labels each group with
        # the CALENDAR period end, and a calendar month end is a weekend about a
        # third of the time; intersecting against a trading-day index then drops
        # it. Measured on SPY over 2015-2026: 97 monthly rebalances fired where
        # 139 were due — 42 missed, 30% of them — and 13 of 46 quarterly.
        # `rebalance_dates` groups by period and takes the last date actually
        # present, which cannot drop a period that has any trading day at all.
        signals.loc[
            rebalance_dates(price_data.index, self.rebalance_frequency), "signal"
        ] = 2.0

        return signals
