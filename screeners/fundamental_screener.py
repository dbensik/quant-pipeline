import pandas as pd
from typing import List, Dict, Any
from .base_screener import BaseScreener

class FundamentalScreener(BaseScreener):
    """
    Screens tickers based on fundamental price and volume criteria.
    Although named 'Fundamental', in this MVP it focuses on liquidity and price constraints
    which are often prerequisites for fundamental analysis.
    """

    def __init__(self, min_price: float = None, min_avg_volume: float = None, avg_volume_days: int = 20):
        """
        Args:
            min_price: Minimum closing price (latest).
            min_avg_volume: Minimum average daily volume.
            avg_volume_days: Number of days to calculate average volume over (default 20).
        """
        self.min_price = min_price
        self.min_avg_volume = min_avg_volume
        self.avg_volume_days = avg_volume_days

    def screen(self, tickers: List[str], data: Dict[str, pd.DataFrame]) -> List[str]:
        """
        Filters tickers based on price and volume criteria.
        """
        passed_tickers = []

        for ticker in tickers:
            df = data.get(ticker)
            
            # Skip if no data
            if df is None or df.empty:
                continue
                
            # Get latest data
            latest = df.iloc[-1]
            latest_price = latest["Close"]
            
            # Check Min Price
            if self.min_price is not None:
                if latest_price < self.min_price:
                    continue
            
            # Check Min Volume
            if self.min_avg_volume is not None:
                # Calculate average volume
                # If there isn't enough data, take what's available
                start_idx = max(0, len(df) - self.avg_volume_days)
                avg_vol = df["Volume"].iloc[start_idx:].mean()
                
                if avg_vol < self.min_avg_volume:
                    continue
            
            passed_tickers.append(ticker)

        return passed_tickers

    def get_analysis_metric(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Returns metric for display.
        """
        if price_data.empty:
            return {"Price": "N/A", "Avg Vol": "N/A"}
            
        latest_price = price_data["Close"].iloc[-1]
        
        start_idx = max(0, len(price_data) - self.avg_volume_days)
        avg_vol = price_data["Volume"].iloc[start_idx:].mean()
        
        return {
            "Price": f"${latest_price:.2f}",
            "Avg Vol": f"{int(avg_vol):,}"
        }
