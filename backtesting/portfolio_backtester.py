import numpy as np
import pandas as pd

from analysis.performance_analyzer import PerformanceAnalyzer
from execution.simulated_handler import SimulatedExecutionHandler
from .events import OrderEvent
from .risk_manager import PortfolioRiskManager


class PortfolioBacktester:
    """
    A realistic, event-driven portfolio backtester that orchestrates signals,
    risk, and execution at a portfolio level.
    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        execution_handler: SimulatedExecutionHandler = None,
        risk_manager: PortfolioRiskManager = None,
    ):
        self.initial_capital = initial_capital
        self.execution_handler = execution_handler or SimulatedExecutionHandler()
        self.risk_manager = risk_manager or PortfolioRiskManager()
        self.results = None

    def run(
        self, price_data: dict, signals_data: dict, target_weights: dict
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Runs the portfolio backtest using an efficient event-driven loop.

        Args:
            price_data: A dictionary mapping tickers to their OHLCV DataFrame.
            signals_data: A dictionary mapping tickers to their signals DataFrame.
            target_weights: A dictionary mapping tickers to their target weights.
        """
        # --- 1. ARCHITECTURAL REFACTOR: Create an event-driven loop ---
        # Get all unique timestamps from all signal sources. This is more efficient
        # than iterating over every calendar day.
        all_signal_dates = set()
        for signal_df in signals_data.values():
            all_signal_dates.update(signal_df.index)

        # Combine all price data indices to create the master timeline for the portfolio
        all_price_dates = set()
        for price_df in price_data.values():
            all_price_dates.update(price_df.index)

        # The full timeline is the union of all price and signal dates, sorted.
        full_timeline = sorted(list(all_price_dates.union(all_signal_dates)))

        if not full_timeline:
            # If there are no dates at all, return an empty result.
            return pd.DataFrame(), pd.DataFrame()

        # --- 2. Initialize Portfolio State ---
        all_tickers = list(price_data.keys())
        portfolio = pd.DataFrame(index=full_timeline)
        portfolio["cash"] = self.initial_capital
        portfolio["holdings_value"] = 0.0
        portfolio["total"] = self.initial_capital
        for ticker in all_tickers:
            portfolio[f"{ticker}_pos"] = 0.0

        trade_log = []

        # --- 3. Main Event Loop ---
        # Iterate through each day in our master timeline
        for i, timestamp in enumerate(full_timeline):
            # Carry forward the previous day's state
            if i > 0:
                prev_timestamp = full_timeline[i - 1]
                portfolio.loc[timestamp] = portfolio.loc[prev_timestamp]

            # Update holdings value with current prices
            current_holdings_value = 0.0
            current_prices = {}
            for ticker in all_tickers:
                # Use asof to get the most recent price if the current timestamp is a holiday
                price_series = price_data[ticker]["Close"]
                price_idx = price_series.index.asof_loc(timestamp)
                if price_idx != -1:  # asof_loc returns -1 if no valid location
                    current_price = price_series.iloc[price_idx]
                    current_prices[ticker] = current_price
                    current_holdings_value += (
                        portfolio.loc[timestamp, f"{ticker}_pos"] * current_price
                    )

            portfolio.loc[timestamp, "holdings_value"] = current_holdings_value
            portfolio.loc[timestamp, "total"] = (
                portfolio.loc[timestamp, "cash"] + current_holdings_value
            )

            # If the current day is not a signal day, we just update values and continue
            if timestamp not in all_signal_dates:
                continue

            # --- 4. Process Signals for the Day ---
            # This logic now correctly handles both portfolio-level and ticker-level signals.
            portfolio_signal_df = signals_data.get("Portfolio")
            portfolio_signal = (
                portfolio_signal_df.loc[timestamp, "signal"]
                if portfolio_signal_df is not None
                and timestamp in portfolio_signal_df.index
                else 0
            )

            for ticker in all_tickers:
                ticker_signal_df = signals_data.get(ticker)
                ticker_signal = (
                    ticker_signal_df.loc[timestamp, "signal"]
                    if ticker_signal_df is not None
                    and timestamp in ticker_signal_df.index
                    else 0
                )

                # Combine signals (portfolio signal takes precedence if it exists)
                signal = portfolio_signal if portfolio_signal != 0 else ticker_signal

                if signal == 0:
                    continue

                current_position = portfolio.loc[timestamp, f"{ticker}_pos"]
                current_price = current_prices.get(ticker)
                order = None

                if not current_price or pd.isna(current_price):
                    continue

                # --- Signal Execution Logic ---
                if signal == 1 and np.isclose(current_position, 0):  # Directional Buy
                    target_value = portfolio.loc[
                        timestamp, "total"
                    ] * target_weights.get(ticker, 0)
                    quantity = int(target_value / current_price)
                    if quantity > 0:
                        order = OrderEvent(timestamp, ticker, "MKT", quantity, "BUY")

                elif signal == -1 and current_position > 0:  # Directional Sell
                    order = OrderEvent(
                        timestamp, ticker, "MKT", int(current_position), "SELL"
                    )

                elif signal == 2:  # Rebalance Signal
                    target_value = portfolio.loc[
                        timestamp, "total"
                    ] * target_weights.get(ticker, 0)
                    target_quantity = int(target_value / current_price)
                    trade_quantity = target_quantity - current_position

                    if trade_quantity > 0:
                        order = OrderEvent(
                            timestamp, ticker, "MKT", int(trade_quantity), "BUY"
                        )
                    elif trade_quantity < 0:
                        order = OrderEvent(
                            timestamp, ticker, "MKT", int(abs(trade_quantity)), "SELL"
                        )

                if not order:
                    continue

                # --- 5. Risk and Execution ---
                portfolio.loc[timestamp, f"{order.symbol}_price"] = current_prices[
                    ticker
                ]
                approved_order = self.risk_manager.assess_order(
                    order, portfolio.loc[timestamp]
                )
                if not approved_order:
                    continue

                fill_event = self.execution_handler.execute_order(
                    approved_order, current_prices[ticker]
                )

                if fill_event:
                    if fill_event.direction == "BUY":
                        portfolio.loc[timestamp, "cash"] -= fill_event.total_cost
                        portfolio.loc[timestamp, f"{ticker}_pos"] += fill_event.quantity
                    elif fill_event.direction == "SELL":
                        portfolio.loc[timestamp, "cash"] += fill_event.total_cost
                        portfolio.loc[timestamp, f"{ticker}_pos"] -= fill_event.quantity
                    trade_log.append(vars(fill_event))

        # --- Final Calculations ---
        portfolio["returns"] = portfolio["total"].pct_change().fillna(0)
        self.results = portfolio

        return portfolio, pd.DataFrame(trade_log)

    def get_performance_metrics(self) -> dict:
        """Calculates performance metrics by delegating to the PerformanceAnalyzer."""
        if self.results is None or self.results.empty:
            return {}
        analyzer = PerformanceAnalyzer(self.results, self.initial_capital)
        return analyzer.calculate_all_metrics()
