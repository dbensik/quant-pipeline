import pandas as pd
import streamlit as st

from backtesting.backtester import Backtester
from backtesting.parameter_generator import (
    MACrossoverParameterGenerator,
    MeanReversionParameterGenerator,
)
from backtesting.parameter_optimizer import ParameterOptimizer
from dashboard_app.price_data_handler import PriceDataHandler
from portfolio.portfolio_optimizer import PortfolioOptimizer


class OptimizationController:
    """
    Handles logic for running parameter optimization and portfolio optimization.
    """

    def __init__(self, price_handler: PriceDataHandler):
        self.price_handler = price_handler

    def run_parameter_optimization(self, selections: dict):
        """Runs a parameter optimization for a single ticker."""
        selected_symbols = selections.get("selected_symbols", [])
        if len(selected_symbols) != 1:
            st.warning("Parameter optimization currently supports one ticker at a time.")
            return

        ticker = selected_symbols[0]
        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")

        with st.spinner(f"Fetching data for {ticker}..."):
            price_data = self.price_handler.get_full_data_for_tickers(
                [ticker], start_date, end_date
            )
            if ticker not in price_data:
                st.error("No data available.")
                return

        strategy_type = selections.get("strategy_type")
        metric = selections.get("optimize_metric", "Sharpe Ratio")
        param_grid = []

        if strategy_type == "Mean Reversion":
            w_range = selections.get("mr_window_range", (5, 20))
            t_range = selections.get("mr_threshold_range", (0.5, 1.5))
            param_grid = MeanReversionParameterGenerator.generate_grid(
                window_range=range(w_range[0], w_range[1] + 1),
                threshold_range=[
                    x / 10.0 for x in range(int(t_range[0] * 10), int(t_range[1] * 10) + 1)
                ],
            )
        elif strategy_type == "Moving Average Crossover":
            s_range = selections.get("mac_short_range", (10, 30))
            l_range = selections.get("mac_long_range", (40, 60))
            param_grid = MACrossoverParameterGenerator.generate_grid(
                short_window_range=range(s_range[0], s_range[1] + 1),
                long_window_range=range(l_range[0], l_range[1] + 1),
            )
        else:
            st.warning(f"Optimization not supported for strategy: {strategy_type}")
            return

        with st.spinner(f"Running optimization ({len(param_grid)} combinations)..."):
            optimizer = ParameterOptimizer(
                price_data[ticker], strategy_type, param_grid, metric
            )
            results_df = optimizer.run_optimization()
            best_params = optimizer.get_best_parameters()

            st.session_state.optimization_run = {
                "results": results_df,
                "best_params": best_params,
                "symbol": ticker,
                "strategy": strategy_type,
                "metric": metric,
            }

    @staticmethod
    def update_progress(progress_value):
        """A simple function to update the Streamlit progress bar."""
        if "progress_bar" in st.session_state:
            st.session_state.progress_bar.progress(progress_value)

    def run_portfolio_optimization(self, selections: dict):
        """Runs a Monte Carlo simulation to find optimal weights for a Buy & Hold strategy."""
        selected_symbols = selections.get("selected_symbols", [])
        if len(selected_symbols) < 2:
            st.warning("Portfolio optimization requires at least two assets.")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")

        with st.spinner("Fetching data..."):
            price_df_dict = self.price_handler.get_prices(
                selected_symbols, start_date, end_date
            )
            if not price_df_dict:
                st.error("No data found.")
                return
            prices_df = pd.DataFrame(price_df_dict).dropna()

        num_simulations = 5000  # Could be exposed to UI
        st.session_state.progress_bar = st.progress(0)

        with st.spinner(f"Running {num_simulations} Monte Carlo simulations..."):
            optimizer = PortfolioOptimizer(prices_df)
            results_df, optimal_weights_sharpe, optimal_weights_vol = (
                optimizer.simulate_random_portfolios(
                    num_portfolios=num_simulations,
                    callback=self.update_progress,
                )
            )
            st.session_state.progress_bar.empty()
            del st.session_state.progress_bar

            st.session_state.portfolio_opt_run = {
                "results_df": results_df,
                "max_sharpe_weights": optimal_weights_sharpe,
                "min_vol_weights": optimal_weights_vol,
                "efficient_frontier": None,  # Placeholder if we implement efficient frontier calc
            }
