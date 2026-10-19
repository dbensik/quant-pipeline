from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from analysis.performance_analyzer import periods_per_year

from .base_screener import BaseScreener


def realised_volatility(close: pd.Series, window: int) -> Optional[float]:
    """
    Annualized standard deviation of the last `window` daily returns.

    Annualized on the series' own calendar (decision 38): 252 for equities,
    365 for crypto. None when there are fewer than `window` returns, so a
    name too young to measure is left unranked rather than ranked on less.
    """
    returns = close.astype(float).pct_change().dropna()
    if len(returns) < window:
        return None
    return float(returns.iloc[-window:].std() * np.sqrt(periods_per_year(close.index)))


class LowVolatilityScreener(BaseScreener):
    """
    Keeps the names whose realised volatility, at the end of the window, falls
    in the bottom `quantile` of the universe.

    FIXED 2026-10-19 (decision 43, research/low-volatility-screener-2026-10-19.md).
    It read a precomputed `volatility_90d` column that only the SQLite store
    had. Since the TimescaleDB cutover the frames it receives carry OHLCV
    only, so it returned an empty list for every screen: 495 S&P names with
    data and 0 kept, even at quantile 1.0. It now measures volatility from the
    closes it is given.
    """

    def __init__(self, quantile: float = 0.25, window: int = 90):
        """
        Args:
            quantile: The fraction of the universe to keep, lowest volatility
                      first (0.25 keeps the bottom 25%).
            window:   Daily returns the volatility is measured over, ending at
                      the last bar. 90 is what the old precomputed column used.
        """
        super().__init__()
        self.quantile = quantile
        self.window = window

    def screen(self, tickers: List[str], data: Dict[str, pd.DataFrame]) -> List[str]:
        """Filters tickers on their realised volatility at the end of the window."""
        volatilities = {}
        for ticker in tickers:
            frame = data.get(ticker)
            if frame is None or frame.empty or "Close" not in frame.columns:
                continue
            value = realised_volatility(frame["Close"], self.window)
            if value is not None and np.isfinite(value):
                volatilities[ticker] = value

        if not volatilities:
            return []

        vol_series = pd.Series(volatilities)
        cutoff = vol_series.quantile(self.quantile)
        return vol_series[vol_series <= cutoff].index.tolist()

    def get_analysis_metric(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """The realised volatility, for display in an analysis table."""
        column_name = f"Volatility ({self.window}d)"
        if price_data.empty or "Close" not in price_data.columns:
            return {column_name: "N/A"}
        value = realised_volatility(price_data["Close"], self.window)
        return {column_name: f"{value:.2%}" if value is not None else "N/A"}
