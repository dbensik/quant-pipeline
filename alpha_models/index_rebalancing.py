import pandas as pd

from .base_model import BaseAlphaModel
from .rebalancing import rebalance_dates


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

        # See alpha_models/rebalancing.py. The previous
        # `index.intersection(resample(freq).last().index)` dropped any period
        # whose CALENDAR end fell on a weekend — 42 of 139 monthly rebalances
        # missed on SPY over 2015-2026.
        signals.loc[
            rebalance_dates(price_data.index, self.rebalance_frequency), "signal"
        ] = 2.0

        return signals
