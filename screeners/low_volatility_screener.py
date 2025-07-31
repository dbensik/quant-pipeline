from typing import Any, Dict, List

import pandas as pd

from .base_screener import BaseScreener


class LowVolatilityScreener(BaseScreener):
    """
    A screener that filters for stocks with the lowest pre-calculated
    90-day volatility. This is very fast as it uses cached data from the DB.
    """

    def __init__(self, quantile: float = 0.25):
        """
        Initializes the LowVolatilityScreener.

        Args:
            quantile (float): The quantile of lowest volatility stocks to keep
                              (e.g., 0.25 keeps the bottom 25%).
        """
        super().__init__()
        self.quantile = quantile

    def screen(self, tickers: List[str], data: Dict[str, pd.DataFrame]) -> List[str]:
        """Filters tickers based on their latest volatility reading."""
        volatilities = {}
        for ticker in tickers:
            if (
                ticker in data
                and not data[ticker].empty
                and "volatility_90d" in data[ticker].columns
            ):
                # Get the last valid volatility reading from the pre-calculated column
                last_vol_value = data[ticker]["volatility_90d"].dropna().iloc[-1]
                if pd.notna(last_vol_value):
                    volatilities[ticker] = last_vol_value

        if not volatilities:
            return []

        vol_series = pd.Series(volatilities)
        # Find the volatility value at the specified quantile (e.g., the 25th percentile)
        cutoff = vol_series.quantile(self.quantile)
        # Return tickers whose volatility is at or below the cutoff
        return vol_series[vol_series <= cutoff].index.tolist()

    def get_analysis_metric(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """Returns the latest volatility for display in the UI analysis table."""
        column_name = "Volatility (90d)"
        if price_data.empty or "volatility_90d" not in price_data.columns:
            return {column_name: "N/A"}

        last_vol = price_data["volatility_90d"].dropna().iloc[-1]
        return {column_name: f"{last_vol:.2%}" if pd.notna(last_vol) else "N/A"}
