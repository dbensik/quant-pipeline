from typing import Optional

import pandas as pd
import streamlit as st

from alpha_models.base_model import BaseAlphaModel
from alpha_models.basket_trading import BasketTradingStrategy
from alpha_models.buy_and_hold import BuyAndHoldStrategy
from alpha_models.cointegrated_mean_reversion import CointegratedMeanReversionStrategy
from alpha_models.index_rebalancing import IndexRebalancingStrategy
from alpha_models.mean_reversion import MeanReversionStrategy
from alpha_models.moving_average_crossover import MovingAverageCrossoverStrategy
from alpha_models.pairs_trading import PairsTradingStrategy

from alpha_models.push_response_strategy import PushResponseStrategy
from alpha_models.trend_following import TrendFollowingStrategy
# New Strategy Imports
from alpha_models.rsi_strategy import RSIStrategy
from alpha_models.atr_breakout import ATRBreakoutStrategy
from alpha_models.ml_random_forest import RandomForestStrategy
from backtesting.backtester import Backtester
from backtesting.portfolio_backtester import PortfolioBacktester
from services.execution_service.portfolio_manager import PortfolioManager
from dashboard_app.price_data_handler import PriceDataHandler
from portfolio.risk_manager import RiskManager


class AnalysisController:
    """
    Handles logic for running backtests (individual and portfolio).
    """

    def __init__(
        self, price_handler: PriceDataHandler, portfolio_manager: PortfolioManager
    ):
        self.price_handler = price_handler
        self.portfolio_manager = portfolio_manager

    def create_strategy_model(self, params: dict) -> Optional[BaseAlphaModel]:
        """Factory method to create strategy model instances."""
        strategy_type = params.get("strategy_type")
        if strategy_type == "Buy and Hold":
            return BuyAndHoldStrategy()
        elif strategy_type == "Mean Reversion":
            return MeanReversionStrategy(
                window=params.get("mr_window", 20),
                threshold=params.get("mr_threshold", 1.5),
            )
        elif strategy_type == "Moving Average Crossover":
            short_ma, long_ma = params.get("mac_short_window", 40), params.get(
                "mac_long_window", 100
            )
            if short_ma >= long_ma:
                st.error(
                    f"Short MA ({short_ma}) must be less than Long MA ({long_ma})."
                )
                return None
            return MovingAverageCrossoverStrategy(
                short_window=short_ma, long_window=long_ma
            )
        elif strategy_type == "Trend Following":
            return TrendFollowingStrategy(window=params.get("tf_window", 50))
        elif strategy_type == "Push-Response":
            return PushResponseStrategy(
                tau=params.get("pr_tau", 21),
                training_window=params.get("pr_training_window", 252),
                threshold=params.get("pr_threshold", 0.0),
            )
        elif strategy_type == "Pairs Trading":
            return PairsTradingStrategy(
                window=params.get("mr_window", 20),
                threshold=params.get("mr_threshold", 2.0),
            )
        elif strategy_type == "Basket Trading":
            return BasketTradingStrategy(rebalance_frequency="M")
        elif strategy_type == "Index Rebalancing":
            return IndexRebalancingStrategy(
                rebalance_frequency=params.get("rebalance_freq", "M")
            )
        elif strategy_type == "Cointegrated Mean Reversion":
            portfolio_name = params.get("source_name")
            portfolio_data = self.portfolio_manager.portfolios.get(portfolio_name, {})
            weights = portfolio_data.get("weights")
            if not weights:
                st.error(
                    "Cointegrated Mean Reversion requires a portfolio with weights from a Johansen test."
                )
                return None
            return CointegratedMeanReversionStrategy(
                weights=weights,
                window=params.get("mr_window", 20),
                threshold=params.get("mr_threshold", 2.0),
            )
        elif strategy_type == "RSI Oscillator":
            return RSIStrategy(
                window=params.get("rsi_window", 14),
                buy_threshold=params.get("rsi_buy_threshold", 30),
                sell_threshold=params.get("rsi_sell_threshold", 70),
            )
        elif strategy_type == "ATR Breakout":
            return ATRBreakoutStrategy(
                window=params.get("atr_window", 20),
                multiplier=params.get("atr_multiplier", 2.0),
            )
        elif strategy_type == "Random Forest":
            return RandomForestStrategy(
                n_estimators=params.get("ml_n_estimators", 100),
                lookback_window=params.get("ml_lookback_window", 5),
            )
        st.error(f"Unknown strategy type: {strategy_type}")
        return None

    def run_individual_backtest(self, selections: dict):
        """Runs a standard backtest on a set of symbols."""
        selected_symbols = selections.get("selected_symbols", [])
        if not selected_symbols:
            st.warning(f"Please select at least one ticker for analysis. (Debug: Received {selected_symbols}, Selections keys: {list(selections.keys())})")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")
        with st.spinner(
            f"Fetching data and running backtests for {len(selected_symbols)} ticker(s)..."
        ):
            backtest_data_dict = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            spy_df = self.price_handler.get_prices(["SPY"], start_date, end_date)
            benchmarks = (
                {"SPY": pd.DataFrame(spy_df["SPY"]).rename(columns={"SPY": "total"})}
                if not spy_df.empty
                else {}
            )

            results_data = {}
            backtester = Backtester(
                initial_capital=selections.get("initial_capital", 100000.0)
            )
            model = self.create_strategy_model(selections)
            if not model:
                return

            for symbol, data in backtest_data_dict.items():
                portfolio = backtester.run(price_data=data, model=model, symbol_name=symbol)
                stats = backtester.get_performance_metrics()
                risk_metrics = {}
                if not portfolio["returns"].empty:
                    risk_manager = RiskManager(portfolio_returns=portfolio["returns"])
                    risk_metrics = risk_manager.get_all_risk_metrics()

                results_data[symbol] = {
                    "portfolio": portfolio,
                    "stats": stats,
                    "risk_metrics": risk_metrics,
                    "trade_log": backtester.get_trade_log(),
                }
                benchmarks[f"Buy & Hold {symbol}"] = pd.DataFrame(data["Close"]).rename(
                    columns={"Close": "total"}
                )

            if not results_data:
                st.error(
                    "Could not generate backtest results for any selected tickers."
                )
                return
            st.session_state.backtest_run = {
                "results": results_data,
                "benchmarks": benchmarks,
            }

    def run_portfolio_backtest(self, selections: dict):
        """Runs a single backtest on a portfolio of assets."""
        selected_symbols = selections.get("selected_symbols", [])
        if (
            selections.get("strategy_type") == "Pairs Trading"
            and len(selected_symbols) != 2
        ):
            st.warning("Pairs Trading requires exactly two tickers to be selected.")
            return

        if len(selected_symbols) < 2 and selections.get("strategy_type") != "Pairs Trading":
            st.warning("Portfolio analysis requires at least two tickers.")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")
        with st.spinner(f"Fetching data for portfolio and benchmarks..."):
            price_data = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            if not price_data:
                st.error(
                    "No data available for the selected tickers in the given date range."
                )
                return

            spy_df = self.price_handler.get_prices(["SPY"], start_date, end_date)
            benchmarks = (
                {"SPY": pd.DataFrame(spy_df["SPY"]).rename(columns={"SPY": "total"})}
                if not spy_df.empty
                else {}
            )

        with st.spinner("Generating trading signals and running backtest..."):
            model = self.create_strategy_model(selections)
            if not model:
                return

            backtester = PortfolioBacktester(
                initial_capital=selections.get("initial_capital", 100000.0),
                enable_vol_targeting=selections.get("enable_vol_targeting", False),
                target_volatility=selections.get("target_volatility", 0.15)
            )
            portfolio_weights = {
                symbol: 1.0 / len(price_data) for symbol in price_data.keys()
            }
            if selections.get("source_type") == "portfolio":
                portfolio_data = self.portfolio_manager.portfolios.get(
                    selections.get("source_name"), {}
                )
                if portfolio_data.get("weights"):
                    portfolio_weights = portfolio_data["weights"]

            signals_data = {}
            if isinstance(model, PairsTradingStrategy):
                price_df = pd.DataFrame(
                    {symbol: data["Close"] for symbol, data in price_data.items()}
                ).dropna()
                signals_df = model.generate_signals(price_df)
                signals_data = {
                    col: signals_df[[col]].rename(columns={col: "signal"})
                    for col in signals_df.columns
                }
            elif isinstance(model, BasketTradingStrategy):
                any_ticker_data = next(iter(price_data.values()))
                rebalance_signals = model.generate_signals(any_ticker_data)
                signals_data = {
                    symbol: rebalance_signals for symbol in price_data.keys()
                }
            elif isinstance(model, IndexRebalancingStrategy):
                any_ticker_data = next(iter(price_data.values()))
                rebalance_signals = model.generate_signals(any_ticker_data)
                signals_data = {
                    symbol: rebalance_signals for symbol in price_data.keys()
                }
            elif isinstance(model, CointegratedMeanReversionStrategy):
                price_df = pd.DataFrame(
                    {symbol: data["Close"] for symbol, data in price_data.items()}
                ).dropna()
                signals_df = model.generate_signals(price_df)
                signals_data = {"Portfolio": signals_df}
            else:
                signals_data = {
                    symbol: model.generate_signals(data)
                    for symbol, data in price_data.items()
                }

            portfolio_df, trade_log_df = backtester.run(
                price_data, signals_data, portfolio_weights
            )

            risk_metrics = {}
            if not portfolio_df["returns"].empty:
                risk_manager = RiskManager(portfolio_returns=portfolio_df["returns"])
                risk_metrics = risk_manager.get_all_risk_metrics()

            st.session_state.backtest_run = {
                "results": {
                    "Portfolio": {
                        "portfolio": portfolio_df,
                        "trade_log": trade_log_df,
                        "stats": backtester.get_performance_metrics(),
                        "risk_metrics": risk_metrics,
                    }
                },
                "benchmarks": benchmarks,
            }

    def run_strategy_comparison(self, selections: dict):
        """
        Runs multiple strategies on the same selected ticker(s) and aggregates results
        for side-by-side comparison.
        """
        selected_strategies = selections.get("comparison_strategies", [])
        if len(selected_strategies) < 2:
            st.warning("Please select at least two strategies to compare.")
            return

        selected_symbols = selections.get("selected_symbols", [])
        if not selected_symbols:
            st.warning("Please select a ticker for comparison.")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")

        with st.spinner(f"Fetching data for comparison..."):
            # Fetch data once
            data = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            
            bench_symbol = selections.get("selected_benchmark")
            comparison_results = {}
            benchmarks = {}
            
            if bench_symbol and bench_symbol != "None":
                 bench_df = self.price_handler.get_prices([bench_symbol], start_date, end_date)
                 
                 # FALLBACK: If DB has no data for benchmark, try fetching live
                 if bench_df.empty:
                    try:
                        import yfinance as yf
                        ticker_obj = yf.Ticker(bench_symbol)
                        # Fetch history
                        hist = ticker_obj.history(start=start_date, end=end_date)
                        if not hist.empty:
                            bench_df = pd.DataFrame({bench_symbol: hist["Close"]})
                    except Exception as e:
                        st.warning(f"Could not fetch benchmark data for {bench_symbol}: {e}")

                 if not bench_df.empty:
                    # Store for plotting
                    benchmarks[bench_symbol] = pd.DataFrame(bench_df[bench_symbol]).rename(columns={bench_symbol: "total"})
                    
                    # Calculate metrics for the benchmark to show in table
                    # We need a 'returns' column for the analyzer
                    bench_data = benchmarks[bench_symbol].copy()
                    bench_data["returns"] = bench_data["total"].pct_change().fillna(0)
                    
                    # NORMALIZE: Scale benchmark to start at 100k so "Total Return" calc is correct
                    # (Analyzer expects 'total' to be portfolio value, not raw price)
                    initial_price = bench_data["total"].iloc[0]
                    if initial_price > 0:
                        bench_data["total"] = (bench_data["total"] / initial_price) * 100000.0

                    # Use analyzer to get stats
                    from analysis.performance_analyzer import PerformanceAnalyzer
                    analyzer = PerformanceAnalyzer(bench_data, 100000.0)
                    bench_stats = analyzer.calculate_all_metrics()
                    
                    # Add to comparison results so it appears in the table
                    # We treat it like a "strategy" for the table display
                    comparison_results[f"Benchmark ({bench_symbol})"] = {
                        "portfolio": None, # No portfolio object needed for table, just stats
                        "stats": bench_stats
                    }

        progress_bar = st.progress(0)
        
        with st.spinner("Running strategies..."):
            for i, strategy_name in enumerate(selected_strategies):
                params = selections.copy()
                params["strategy_type"] = strategy_name
                
                # --- AUTO-OPTIMIZATION ---
                # Attempt to find best parameters to ensure fair comparison
                best_params = self._find_optimal_params(strategy_name, data.get(selected_symbols[0]), params)
                if best_params:
                    params.update(best_params)
                
                model = self.create_strategy_model(params)
                if not model:
                    continue
                
                target_symbol = selected_symbols[0]
                ticker_data = data.get(target_symbol)
                
                if ticker_data is None or ticker_data.empty:
                    continue

                backtester = Backtester()
                portfolio = backtester.run(price_data=ticker_data, model=model)
                stats = backtester.get_performance_metrics()
                
                # Inject the parameters used into stats for display
                if best_params:
                    stats.update(best_params)
                
                comparison_results[strategy_name] = {
                    "portfolio": portfolio,
                    "stats": stats
                }
                
                progress_bar.progress((i + 1) / len(selected_strategies))

        if not comparison_results:
            st.error("Could not generate results for comparison.")
            return

        st.session_state.comparison_results = {
            "symbol": selected_symbols[0],
            "strategies": comparison_results,
            "benchmarks": benchmarks
        }

    def _find_optimal_params(self, strategy_name: str, price_data: pd.DataFrame, base_params: dict) -> dict:
        """
        Performs a quick grid search to find optimal parameters for a given strategy
        to Maximize Sharpe Ratio.
        """
        best_sharpe = -float('inf')
        best_params = {}
        
        if strategy_name == "Mean Reversion":
            # Grid search for Window and Threshold
            windows = range(10, 60, 10)
            thresholds = [1.0, 1.5, 2.0, 2.5]
            
            for w in windows:
                for t in thresholds:
                    # Create temporary model
                    model = MeanReversionStrategy(window=w, threshold=t)
                    # Fast backtest (just get metrics)
                    backtester = Backtester()
                    metrics = backtester.run_and_get_metrics(price_data, model)
                    sharpe = metrics.get("Sharpe Ratio", -100)
                    
                    if sharpe > best_sharpe:
                        best_sharpe = sharpe
                        best_params = {"mr_window": w, "mr_threshold": t}
                        
        elif strategy_name == "Moving Average Crossover":
            # Grid search for Short and Long Windows
            short_windows = [10, 20, 30]
            long_windows = [50, 100, 200]
            
            for s in short_windows:
                for l in long_windows:
                    if s >= l: continue
                    
                    model = MovingAverageCrossoverStrategy(short_window=s, long_window=l)
                    backtester = Backtester()
                    metrics = backtester.run_and_get_metrics(price_data, model)
                    sharpe = metrics.get("Sharpe Ratio", -100)
                    
                    if sharpe > best_sharpe:
                        best_sharpe = sharpe
                        best_params = {"mac_short_window": s, "mac_long_window": l}
        
        return best_params
