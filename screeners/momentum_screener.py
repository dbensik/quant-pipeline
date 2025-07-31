from typing import Any, Dict, List

import pandas as pd

from .base_screener import BaseScreener


class MomentumScreener(BaseScreener):
    """
    A screener that filters for stocks exhibiting strong positive momentum.
    """

    def __init__(self, momentum_window: int = 126, min_momentum: float = 0.10):
        """
        Initializes the MomentumScreener.

        Args:
            momentum_window (int): The number of trading days to calculate
                                   momentum over (e.g., 126 days = ~6 months).
            min_momentum (float): The minimum required return over the window
                                  to pass the screen (e.g., 0.10 = 10%).
        """
        super().__init__()
        self.momentum_window = momentum_window
        self.min_momentum = min_momentum

    def screen(self, tickers: List[str], data: Dict[str, pd.DataFrame]) -> List[str]:
        """
        Filters the list of tickers for those with momentum above the threshold.

        Args:
            tickers: The list of tickers to screen.
            data: A dictionary mapping tickers to their price data.

        Returns:
            A list of tickers that passed the momentum screen.
        """
        passed_tickers = []
        for ticker in tickers:
            if ticker not in data or data[ticker].empty:
                continue

            price_series = data[ticker]["Close"]
            if len(price_series) < self.momentum_window:
                continue

            # Calculate momentum as the percentage change over the window
            momentum = (
                price_series.iloc[-1] / price_series.iloc[-self.momentum_window]
            ) - 1

            if momentum >= self.min_momentum:
                passed_tickers.append(ticker)

        return passed_tickers

    def get_analysis_metric(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """Calculates the momentum for display in the analysis table."""
        column_name = f"Momentum ({self.momentum_window}d)"
        if len(price_data) < self.momentum_window:
            return {column_name: "N/A"}

        price_series = price_data["Close"]
        momentum = (
            price_series.iloc[-1] / price_series.iloc[-self.momentum_window]
        ) - 1
        return {column_name: f"{momentum:.2%}"}
