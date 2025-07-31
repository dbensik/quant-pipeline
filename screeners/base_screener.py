from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd


class BaseScreener(ABC):
    """
    An abstract base class for all stock screeners.

    This class defines the standard interface for any screener. The goal of a
    screener is to take a universe of tickers and a set of data, and return
    a filtered subset of those tickers that meet specific criteria.
    """

    def __init__(self, **kwargs):
        """
        The constructor for the base screener. Subclasses can accept
        specific parameters for their screening logic.
        """
        pass

    @abstractmethod
    def screen(self, tickers: List[str], data: Dict[str, pd.DataFrame]) -> List[str]:
        """
        The core screening method that must be implemented by all subclasses.

        Args:
            tickers (List[str]): The initial list of ticker symbols to screen.
            data (Dict[str, pd.DataFrame]): A dictionary where keys are ticker
                                            symbols and values are DataFrames
                                            of their price data.

        Returns:
            List[str]: A filtered list of tickers that passed the screen.
        """
        raise NotImplementedError("Subclasses must implement the screen() method.")

    @abstractmethod
    def get_analysis_metric(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Returns a dictionary with the screener's key metric for a single ticker.
        This is used for display purposes in the UI after a screen is run.

        Args:
            price_data (pd.DataFrame): The price data for a single ticker.

        Returns:
            Dict[str, Any]: A dictionary, e.g., {'Momentum (126d)': '15.2%'}
        """
        raise NotImplementedError("Subclasses must implement get_analysis_metric().")
