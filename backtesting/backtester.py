# backtester.py
import pandas as pd
import numpy as np
import sqlite3

class Backtester:
    def __init__(self, data, signals, initial_capital=100000):
        """
        Initialize the backtester.

        Parameters:
          data (pd.DataFrame): DataFrame containing market data with at least a 'Close' column.
          signals (pd.Series): Series of trading signals (1 for Buy, -1 for Sell, 0 for Hold).
          initial_capital (float): Starting capital for the backtest.
        """
        self.data = data.copy()
        self.signals = signals.copy()
        self.initial_capital = initial_capital
        self.portfolio = None

    def run_backtest(self):
        """
        Run the backtest simulation.

        This method calculates:
          - Daily asset returns from the 'Close' price.
          - The strategy's return by applying the position based on signals.
          - The cumulative portfolio value over time.
          
        Returns:
          pd.Series: Portfolio value over time.
        """
        df = self.data.copy()
        df['Signal'] = self.signals
        
        # For simplicity, we'll assume:
        # When Signal == 1, we are fully invested (position = 1)
        # When Signal == -1, we exit to cash (position = 0)
        # We hold the position until a signal change occurs.
        # Replace 0 signals with NaN and forward fill.
        df['Position'] = df['Signal'].replace(0, np.nan).ffill().fillna(0)
        
        # Compute daily returns from the Close price
        df['Asset_Return'] = df['Close'].pct_change()
        
        # Strategy returns: use previous day's position
        df['Strategy_Return'] = df['Position'].shift(1) * df['Asset_Return']
        
        # Calculate portfolio value over time
        df['Portfolio_Value'] = self.initial_capital * (1 + df['Strategy_Return']).cumprod()
        
        self.portfolio = df['Portfolio_Value']
        return self.portfolio

    def print_performance(self):
        """
        Print basic performance metrics of the backtest:
          - Cumulative Return
          - Annualized Return (assuming 252 trading days per year)
          - Maximum Drawdown
        """
        if self.portfolio is None:
            print("Run the backtest first using run_backtest().")
            return
        
        # Cumulative return
        cumulative_return = self.portfolio.iloc[-1] / self.initial_capital - 1
        # Annualized return
        annualized_return = (1 + cumulative_return) ** (252 / len(self.portfolio)) - 1
        
        # Maximum Drawdown calculation
        rolling_max = self.portfolio.cummax()
        drawdown = (self.portfolio - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        print(f"Cumulative Return: {cumulative_return:.2%}")
        print(f"Annualized Return: {annualized_return:.2%}")
        print(f"Maximum Drawdown: {max_drawdown:.2%}")


    def get_performance_metrics(self):
        """
        Returns key performance metrics as a dictionary:
          - Cumulative Return
          - Annualized Return
          - Volatility (annualized standard deviation)
          - Sharpe Ratio (assuming risk-free rate is 0)
          - Sortino Ratio (assuming risk-free rate is 0)
          - Maximum Drawdown
        """
        if self.portfolio is None:
            raise ValueError("Run the backtest first using run_backtest().")
        
        # Cumulative return
        cumulative_return = self.portfolio.iloc[-1] / self.initial_capital - 1
        # Annualized return (assuming daily data and 252 trading days)
        annualized_return = (1 + cumulative_return) ** (252 / len(self.portfolio)) - 1
        
        # Calculate daily returns from portfolio value
        daily_returns = self.portfolio.pct_change().dropna()
        # Annualized volatility (standard deviation of daily returns multiplied by sqrt(252))
        volatility = daily_returns.std() * (252 ** 0.5)
        
        # Sharpe Ratio: (annualized return / annualized volatility), risk-free rate assumed 0
        sharpe_ratio = (daily_returns.mean() * 252) / volatility if volatility != 0 else None
        
        # Downside volatility for Sortino Ratio: standard deviation of negative returns
        downside_returns = daily_returns[daily_returns < 0]
        downside_vol = downside_returns.std() * (252 ** 0.5)
        sortino_ratio = (daily_returns.mean() * 252) / downside_vol if downside_vol != 0 else None
        
        # Maximum Drawdown
        rolling_max = self.portfolio.cummax()
        drawdown = (self.portfolio - rolling_max) / rolling_max
        max_drawdown = drawdown.min()
        
        return {
            "cumulative_return": cumulative_return,
            "annualized_return": annualized_return,
            "volatility": volatility,
            "sharpe_ratio": sharpe_ratio,
            "sortino_ratio": sortino_ratio,
            "max_drawdown": max_drawdown
        }

# Testing code: run when executing backtester.py directly
if __name__ == "__main__":
    # Generate dummy data for testing
    dates = pd.date_range("2020-01-01", periods=100)
    np.random.seed(42)
    # Simulate a random walk for Close prices
    prices = 100 + np.cumsum(np.random.randn(100))
    data = pd.DataFrame({"Close": prices}, index=dates)
    
    # Generate dummy signals:
    # For example, a simple pattern: Buy on the first day of each 20-day block, Sell on the 11th day
    signals = pd.Series(0, index=dates)
    signals.iloc[::20] = 1
    signals.iloc[10::20] = -1
    signals = signals.replace(0, np.nan).ffill().fillna(0)
    
    # Instantiate and run the backtester
    backtester = Backtester(data, signals)
    portfolio = backtester.run_backtest()
    
    print("Sample Portfolio Values:")
    print(portfolio.head(10))
    
    print("\nPerformance Metrics:")
    backtester.print_performance()