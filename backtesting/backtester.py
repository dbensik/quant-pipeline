import numpy as np
import pandas as pd

from alpha_models.base_model import BaseAlphaModel
from analysis.performance_analyzer import PerformanceAnalyzer
from backtesting.events import OrderEvent
from execution.simulated_handler import SimulatedExecutionHandler


class Backtester:
    """
    A realistic backtester that simulates trading based on capital allocation,
    enforcing whole-share trades and using a robust event-driven loop.
    Its single responsibility is to generate a portfolio history.
    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        transaction_cost: float = 0.001,
        execution_handler: SimulatedExecutionHandler = None,
    ):
        """
        Initializes the Backtester with portfolio settings.
        """
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.results = None
        self.trade_log = []
        self.execution_handler = execution_handler or SimulatedExecutionHandler()

    def run(self, price_data: pd.DataFrame, model: BaseAlphaModel) -> pd.DataFrame:
        """
        Runs a backtest using a more robust, event-driven loop that delegates
        execution to a dedicated handler.
        """
        # 1. Generate signals from the model
        signals = model.generate_signals(price_data=price_data)
        self.trade_log = []

        # 2. Prepare the portfolio DataFrame for state tracking
        portfolio = price_data[["Close"]].copy()
        portfolio["signal"] = signals["signal"].ffill().fillna(0)
        portfolio["holdings"] = 0.0
        portfolio["cash"] = self.initial_capital
        portfolio["position"] = 0.0  # Number of shares held

        # 3. Use a stateful loop to process trades realistically
        position = 0.0
        cash = self.initial_capital
        symbol = price_data.name if hasattr(price_data, "name") else "Asset"

        for i in range(len(portfolio)):
            date = portfolio.index[i]
            price = portfolio["Close"].iloc[i]
            signal = portfolio["signal"].iloc[i]

            # --- REFACTOR: Trading logic now uses the event-driven system ---
            if signal == 1 and np.isclose(position, 0):  # Buy signal
                if price > 0:
                    # Determine max shares possible based on current cash
                    shares_to_buy = np.floor(cash / price)
                    if shares_to_buy > 0:
                        order = OrderEvent(date, symbol, "MKT", shares_to_buy, "BUY")
                        fill = self.execution_handler.execute_order(order, price)

                        # Ensure we can afford the filled order
                        if cash >= fill.total_cost:
                            position += fill.quantity
                            cash -= fill.total_cost
                            self.trade_log.append(fill)

            elif signal == 0 and position > 0:  # Sell signal (liquidate position)
                order = OrderEvent(date, symbol, "MKT", position, "SELL")
                fill = self.execution_handler.execute_order(order, price)

                position -= fill.quantity  # Should go to zero
                cash += fill.total_cost
                self.trade_log.append(fill)

            # --- Update Portfolio State for the current day ---
            portfolio.iloc[i, portfolio.columns.get_loc("position")] = position
            portfolio.iloc[i, portfolio.columns.get_loc("cash")] = cash
            portfolio.iloc[i, portfolio.columns.get_loc("holdings")] = position * price

        # 4. Calculate final portfolio values
        portfolio["total"] = portfolio["holdings"] + portfolio["cash"]
        portfolio["returns"] = portfolio["total"].pct_change().fillna(0)

        self.results = portfolio
        return portfolio

    def get_trade_log(self) -> pd.DataFrame:
        """
        Returns the log of all trades executed during the backtest as a DataFrame.
        """
        if not self.trade_log:
            return pd.DataFrame()
        return pd.DataFrame([vars(fill) for fill in self.trade_log])

    def get_performance_metrics(self) -> dict:
        """
        Calculates performance metrics by delegating to the PerformanceAnalyzer.
        """
        if self.results is None or self.results.empty:
            return {}  # Return empty dict if no results exist

        analyzer = PerformanceAnalyzer(self.results, self.initial_capital)
        return analyzer.calculate_all_metrics()

    def run_and_get_metrics(
        self, price_data: pd.DataFrame, model: BaseAlphaModel
    ) -> dict:
        """
        A convenience method that runs a backtest and immediately returns the
        performance metrics. Useful for optimizations.
        """
        self.run(price_data=price_data, model=model)
        return self.get_performance_metrics()
