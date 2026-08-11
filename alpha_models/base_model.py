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
            pd.DataFrame: A DataFrame with the same index as `price_data`, in
                          one of the four sanctioned shapes below. Which one a
                          strategy uses is DECLARED in alpha_models/registry.py
                          as `signal_shape`, and that declaration is what the
                          portfolio backtest router dispatches on — so a shape
                          that is not declared is not wired up.

        THE FOUR SHAPES

        `per_symbol` — the default, and every single-asset strategy. One
            'signal' column:
                 1: Go long (buy)
                -1: Go short (sell) or exit long
                 0: Hold or stay flat

        `wide_per_asset` — takes a WIDE frame (one close column per symbol) and
            returns ONE POSITION COLUMN PER ASSET, named for the input columns.
            No 'signal' column at all. Values are the same {-1, 0, 1}. This is
            the shape for anything that must compare assets against each other:
            pairs trading today, and asset-allocation strategies next. Sizing
            comes from the caller's `weights` (equal-weight by default), NOT
            from the strategy — the values here are directional, not fractional.

        `wide_portfolio` — takes a wide frame, returns ONE 'signal' column for
            the whole basket traded as a single unit.

        `calendar_shared` — reads only the DatetimeIndex and returns a rebalance
            schedule every symbol shares. Uses `signal == 2`, which means
            "rebalance to target weights" and is deliberately outside the
            {-1, 0, 1} set the other shapes use.

        Historically these existed but were undocumented and dispatched on by
        hardcoded strategy id inside the router; `tests/test_strategy_contract.py`
        called that "an interface inconsistency worth unifying eventually".
        """
        # This provides a clear error if a subclass forgets to implement the method.
        raise NotImplementedError(
            "Subclasses must implement the generate_signals() method."
        )
