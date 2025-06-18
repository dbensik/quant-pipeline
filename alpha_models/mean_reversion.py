from abc import ABC, abstractmethod
import pandas as pd
from backtesting.backtester import Backtester
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

class BaseStrategy(ABC):
    @abstractmethod
    def generate_signals(self, data):
        """Generate trading signals based on input data."""
        pass

# 🧠 Mean Reversion: Because sometimes the market just wants to go home to mama average
class MeanReversionStrategy(BaseStrategy):
    def __init__(self, window=20, threshold=0.05):
        """
        Initialize the Mean Reversion Strategy.

        Parameters:
            window (int): Rolling window for moving average. The longer the window, the smoother the delusion.
            threshold (float): Deviation to trigger a signal. It’s our tolerance for market nonsense.
        """
        self.window = window
        self.threshold = threshold

    def generate_signals(self, data):
        """
        Generate signals based on mean reversion.

        Buy when the price is dramatically below average—because clearly it's a bargain.
        Sell when the price is annoyingly above average—because gravity is a thing.

        Parameters:
            data (pd.DataFrame): Must have a 'Close' column. If it doesn’t, try again but with effort.

        Returns:
            pd.Series: Trading signals (-1 for Sell, 1 for Buy, 0 for "let’s just watch Netflix instead").
        """
        if 'Close' not in data.columns:
            raise KeyError("Data must contain a 'Close' column. You had one job.")

        # Compute the rolling average like it's 1999
        moving_avg = data['Close'].rolling(window=self.window, min_periods=1).mean()
        deviation = (data['Close'] - moving_avg) / moving_avg
        signals = pd.Series(0, index=data.index)

        # Behold, our grand logic:
        signals.loc[deviation < -self.threshold] = 1  # It's on sale, buy it before someone else does!
        signals.loc[deviation > self.threshold] = -1  # Too pricey. Dump it like your ex’s mixtape.

        return signals

# 🔬 Experimental Sandbox: Because one parameter sweep isn’t enough

def run_experiment(data, window_values, threshold_values, initial_capital=100000):
    """
    Run a parameter sweep to see how much guesswork we can automate.

    Parameters:
      data (pd.DataFrame): Clean price data. Hopefully.
      window_values (list): Windows to try. Like Goldilocks, but for rolling averages.
      threshold_values (list): Deviation thresholds. Who knew 0.05 could be so controversial?
      initial_capital (float): Your fantasy starting balance. Let’s pretend it’s real.

    Returns:
      pd.DataFrame: Performance metrics for each wild guess of parameters.
    """
    results = []
    for window in window_values:
        for threshold in threshold_values:
            strategy = MeanReversionStrategy(window=window, threshold=threshold)
            signals = strategy.generate_signals(data)

            backtester = Backtester(data, signals, initial_capital=initial_capital)
            backtester.run_backtest()
            metrics = backtester.get_performance_metrics()

            results.append({
                "window": window,
                "threshold": threshold,
                "cumulative_return": metrics["cumulative_return"],
                "annualized_return": metrics["annualized_return"],
                "volatility": metrics["volatility"],
                "sharpe_ratio": metrics["sharpe_ratio"],
                "sortino_ratio": metrics["sortino_ratio"],
                "max_drawdown": metrics["max_drawdown"]
            })

    return pd.DataFrame(results)

# 🧪 Test drive for skeptics
if __name__ == "__main__":
    # Simulate some slightly erratic price data because reality is overrated
    dates = pd.date_range(start="2021-01-01", periods=60)
    prices = [100 + ((-1)**i) * (i % 7) for i in range(60)]
    data = pd.DataFrame({"Close": prices}, index=dates)

    # Summon the strategy like a caffeinated quant
    strategy = MeanReversionStrategy(window=10, threshold=0.05)
    signals = strategy.generate_signals(data)

    # Backtest using the official ritualistic module
    backtester = Backtester(data, signals)
    portfolio = backtester.run_backtest()
    backtester.print_performance()

    # 📊 Visual proof that something happened
    print("\nSample portfolio values:")
    print(portfolio.head())

    # 🧪 Bonus round: Grid search because brute force is a strategy
    window_values = [5, 10, 15, 20]
    threshold_values = [0.03, 0.05, 0.07]
    experiment_results = run_experiment(data, window_values, threshold_values)

    print("\nExperiment Results:")
    print(experiment_results)

    heatmap_data = experiment_results.pivot(index="window", columns="threshold", values="annualized_return")
    plt.figure(figsize=(8, 6))
    sns.heatmap(heatmap_data, annot=True, fmt=".2%", cmap="coolwarm")
    plt.title("Annualized Return by Window and Threshold")
    plt.xlabel("Threshold")
    plt.ylabel("Window")
    plt.show()
