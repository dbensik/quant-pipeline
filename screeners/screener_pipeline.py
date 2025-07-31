from typing import Dict, List

import pandas as pd

from .base_screener import BaseScreener


class ScreenerPipeline:
    """
    A class that applies a sequence of screeners to a universe of tickers.
    """

    def __init__(self, *screeners: BaseScreener):
        """
        Initializes the pipeline with a series of screener objects.

        Args:
            *screeners: A variable number of instantiated screener objects
                        that inherit from BaseScreener.
        """
        self.screeners = screeners

    def run(self, tickers: List[str], data: Dict[str, pd.DataFrame]) -> List[str]:
        """
        Runs the full screening pipeline.

        The output of each screener becomes the input for the next one.

        Args:
            tickers: The initial universe of tickers.
            data: The full dataset for all tickers.

        Returns:
            The final list of tickers that passed all screens.
        """
        filtered_tickers = tickers
        for screener in self.screeners:
            # The list of tickers gets progressively smaller
            filtered_tickers = screener.screen(filtered_tickers, data)
        return filtered_tickers
