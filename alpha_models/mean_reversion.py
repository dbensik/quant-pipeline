# mean_reversion.py
from .base_strategy import BaseStrategy
import pandas as pd

class MeanReversionStrategy(BaseStrategy):
    def __init__(self, window=20, threshold=0.05):
        """
        Initialize the Mean Reversion Strategy.
        
        Parameters:
            window (int): The rolling window size for computing the moving average.
            threshold (float): The percentage deviation from the moving average to trigger signals.
        """
        super().__init__()
        self.window = window
        self.threshold = threshold

    def generate_signals(self, data):
        """
        Generate trading signals based on mean reversion logic.
        
        The strategy computes a rolling moving average of the 'Close' price and calculates
        the deviation of the current price from this moving average. If the price deviates
        by more than the specified threshold, a trading signal is generated:
          - 1 (Buy) when the price is below the moving average by more than the threshold.
          - -1 (Sell) when the price is above the moving average by more than the threshold.
          - 0 (Hold) otherwise.
        
        Parameters:
            data (pd.DataFrame): DataFrame containing at least a 'Close' column.
            
        Returns:
            pd.Series: A Series of signals indexed by the data's index.
        """
        if 'Close' not in data.columns:
            raise KeyError("Data must contain a 'Close' column.")

        # Calculate the moving average
        moving_avg = data['Close'].rolling(window=self.window, min_periods=1).mean()
        # Calculate the deviation (as a percentage)
        deviation = (data['Close'] - moving_avg) / moving_avg

        # Initialize signals to hold (0)
        signals = pd.Series(0, index=data.index)

        # Generate signals based on deviation thresholds
        # Buy signal: price is significantly below moving average
        signals[deviation < -self.threshold] = 1
        # Sell signal: price is significantly above moving average
        signals[deviation > self.threshold] = -1

        return signals

    def backtest(self, data):
        """
        Placeholder backtest method.
        Replace this with your backtesting logic.
        """
        print("Backtesting not implemented yet for MeanReversionStrategy.")
        return None

# For testing/debugging purposes
if __name__ == "__main__":
    # Create a simple DataFrame with example data
    dates = pd.date_range(start="2020-01-01", periods=50)
    close_prices = pd.Series([100 + i * 0.5 + (-1) ** i * 2 for i in range(50)], index=dates)
    test_data = pd.DataFrame({"Close": close_prices})
    
    # Initialize the strategy
    strategy = MeanReversionStrategy(window=10, threshold=0.03)
    signals = strategy.generate_signals(test_data)
    
    print("Generated Signals:")
    print(signals.head(15))

    # Print the number of Buy and Sell signals
    num_buy = (signals == 1).sum()
    num_sell = (signals == -1).sum()
    print(f"Number of Buy Signals: {num_buy}")
    print(f"Number of Sell Signals: {num_sell}")

    # Test backtest method
    strategy.backtest(data)