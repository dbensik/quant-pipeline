from abc import ABC, abstractmethod

import pandas as pd


class BaseAlphaModel(ABC):
    """
    An abstract base class for all alpha models.

    This class defines the standard interface that all trading strategy
    models must implement. It enforces a separation of concerns, where the
    model is responsible only for generating trading signals, and a separate
    Backtester class is responsible for evaluating those signals.
    """

    def __init__(self, **kwargs):
        """
        The constructor for the base model.

        Subclasses can override this to accept specific parameters for
        their strategy (e.g., moving average windows). The `**kwargs`
        allows for flexibility.
        """
        pass

    @abstractmethod
    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        The core method of any alpha model.

        This method must be implemented by all subclasses. It takes historical
        price data and returns a DataFrame of trading signals.

        Args:
            price_data (pd.DataFrame): A DataFrame with at least a 'Close'
                                       price column, indexed by date.

        Returns:
            pd.DataFrame: A DataFrame with the same index as `price_data`
                          and a 'signal' column containing the trading
                          signal:
                          -  1: Go long (buy)
                          - -1: Go short (sell) or exit long
                          -  0: Hold or stay flat
        """
        # This provides a clear error if a subclass forgets to implement the method.
        raise NotImplementedError(
            "Subclasses must implement the generate_signals() method."
        )
