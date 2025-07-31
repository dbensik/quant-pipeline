import pandas as pd

from .base_screener import BaseScreener


class FundamentalScreener(BaseScreener):
    """
    Screens tickers based on fundamental metrics like P/E, P/B, or Dividend Yield.
    """

    def __init__(self, metric: str, min_value: float = None, max_value: float = None):
        """
        Args:
            metric: The fundamental metric to screen on (e.g., 'pe_ratio', 'book_value').
            min_value: The minimum acceptable value for the metric.
            max_value: The maximum acceptable value for the metric.
        """
        self.metric = metric
        self.min_value = min_value
        self.max_value = max_value

    def screen(self, fundamental_data: pd.DataFrame) -> list[str]:
        """
        Filters tickers based on the provided fundamental data.

        Args:
            fundamental_data: A DataFrame containing fundamental data for a universe of tickers.
        """
        # Logic to filter tickers based on the metric's min/max values...
        pass
