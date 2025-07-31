import subprocess
import sys
from typing import Optional

import pandas as pd
import streamlit as st

# --- Project Imports ---
try:
    # Service and Manager Imports
    from dashboard_app.database_manager import DatabaseManager
    from dashboard_app.results_manager import ResultsManager
    from dashboard_app.portfolio_manager import PortfolioManager
    from dashboard_app.watchlist_manager import WatchlistManager
    from dashboard_app.price_data_handler import PriceDataHandler

    # UI Component Imports
    from dashboard_app.ui_components.sidebar import Sidebar
    from dashboard_app.ui_components.analysis_tab import AnalysisTab
    from dashboard_app.ui_components.optimization_tab import OptimizationTab
    from dashboard_app.ui_components.portfolio_tab import PortfolioTab
    from dashboard_app.ui_components.stats_tab import StatsTab
    from dashboard_app.ui_components.statistical_analysis_tab import (
        StatisticalAnalysisTab,
    )
    from dashboard_app.ui_components.asset_deep_dive_tab import AssetDeepDiveTab

    # Core Logic Imports
    from data_pipeline.universe_fetcher import UniverseFetcher
    from backtesting.backtester import Backtester
    from backtesting.portfolio_backtester import PortfolioBacktester
    from optimization.portfolio_optimizer import PortfolioOptimizer
    from backtesting.parameter_generator import (
        MACrossoverParameterGenerator,
        MeanReversionParameterGenerator,
    )
    from alpha_models.base_model import BaseAlphaModel
    from alpha_models.buy_and_hold import BuyAndHoldStrategy
    from alpha_models.mean_reversion import MeanReversionStrategy
    from alpha_models.moving_average_crossover import MovingAverageCrossoverStrategy
    from alpha_models.trend_following import TrendFollowingStrategy
    from alpha_models.pairs_trading import PairsTradingStrategy
    from alpha_models.basket_trading import BasketTradingStrategy
    from alpha_models.cointegrated_mean_reversion import (
        CointegratedMeanReversionStrategy,
    )

    # --- FIX: Corrected the import path from 'push_response' to 'push_response_strategy' ---
    from alpha_models.push_response_strategy import PushResponseStrategy
    from screeners.screener_pipeline import ScreenerPipeline

    # --- FEATURE: Import the new PCA module ---
    from analysis.principal_component_analyzer import PrincipalComponentAnalyzer
    from analysis.statistical_analyzer import StatisticalAnalyzer
    from portfolio.risk_manager import RiskManager


except ImportError as e:
    # This prevents NameError exceptions and allows for a graceful failure message.
    # --- FIX: Corrected the number of variables to match the number of imports (30) ---
    (  # --- FEATURE: Expanded tuple for new import ---
        DatabaseManager,
        ResultsManager,
        PortfolioManager,
        WatchlistManager,
        PriceDataHandler,
        Sidebar,
        AnalysisTab,
        OptimizationTab,
        PortfolioTab,
        StatsTab,
        StatisticalAnalysisTab,
        AssetDeepDiveTab,
        UniverseFetcher,
        Backtester,
        PortfolioBacktester,
        PortfolioOptimizer,
        MACrossoverParameterGenerator,
        MeanReversionParameterGenerator,
        BaseAlphaModel,
        BuyAndHoldStrategy,
        MeanReversionStrategy,
        MovingAverageCrossoverStrategy,
        TrendFollowingStrategy,
        PairsTradingStrategy,
        BasketTradingStrategy,
        CointegratedMeanReversionStrategy,
        PushResponseStrategy,
        ScreenerPipeline,
        PrincipalComponentAnalyzer,
        StatisticalAnalyzer,
        RiskManager,
    ) = (None,) * 31
    st.error(
        f"🚨 FAILED TO IMPORT A MODULE. Please ensure all project components are in place. Error: {e}"
    )
    st.stop()


# --- Main Application Class --
class DashboardApp:
    """The main class that orchestrates the entire Streamlit application."""

    def __init__(self):
        """Initialize the application's state and managers."""
        st.set_page_config(
            page_title="Quantitative Analysis Dashboard",
            layout="wide",
            initial_sidebar_state="expanded",
        )
        self._initialize_session_state()

        # Instantiate service managers - the single source of truth for operations
        self.db_manager = DatabaseManager()
        self.price_handler = PriceDataHandler()
        self.results_manager = ResultsManager()
        self.watchlist_manager = WatchlistManager()
        self.portfolio_manager = PortfolioManager()
        self.statistical_analyzer = StatisticalAnalyzer()
        self.universe_fetcher = UniverseFetcher()

        self.all_db_tickers = self._get_cached_tickers()
        if not self.all_db_tickers:
            st.warning(
                "No tickers found in the database. Please run the data pipeline or add tickers via the sidebar."
            )

        # Initialize UI components with the correct, specialized handlers
        self.sidebar = Sidebar(
            self.db_manager,
            self.results_manager,
            self.watchlist_manager,
            self.portfolio_manager,
            self.all_db_tickers,
        )
        self.analysis_tab = AnalysisTab(self.price_handler)
        self.optimization_tab = OptimizationTab()
        self.stats_tab = StatsTab()
        self.portfolio_tab = PortfolioTab(self.price_handler, self.portfolio_manager)
        self.statistical_analysis_tab = StatisticalAnalysisTab()
        self.asset_deep_dive_tab = AssetDeepDiveTab(self.db_manager)
        self.selections = {}

    @st.cache_data(ttl=3600)
    def _get_cached_tickers(_self):
        """Caches the list of available tickers from the database."""
        return _self.db_manager.get_universe_tickers()

    def _initialize_session_state(self):
        """A centralized place to initialize all session state keys."""
        keys_to_init = {
            "screened_tickers": None,
            "screener_analysis_df": None,
            "backtest_run": None,
            "optimization_run": None,
            "stat_test_run": None,
            "app_error": None,
        }
        for key, default_value in keys_to_init.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    def run(self):
        """The main execution loop for the application."""
        self.selections = self.sidebar.render()
        st.title("Quantitative Analysis Dashboard")

        # --- FIX: Display any persistent error messages from action handlers ---
        if st.session_state.get("app_error"):
            st.error(st.session_state["app_error"], icon="🚨")
            st.session_state.pop("app_error", None)

        self._handle_actions()

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
            [
                "🔎 Analysis",
                "⚙️ Optimization",
                "📊 Statistics",
                "🔬 Statistical Analysis",
                "💼 Portfolio",
                "💡 Deep Dive",
            ]
        )
        with tab1:
            self.analysis_tab.render(self.selections)
        with tab2:
            self.optimization_tab.render(self.selections)
        with tab3:
            self.stats_tab.render(self.selections)
        with tab4:
            self.statistical_analysis_tab.render(self.selections)
        with tab5:
            self.portfolio_tab.render(self.selections)
        with tab6:
            self.asset_deep_dive_tab.render()

    def _handle_actions(self):
        """Controller method to dispatch actions based on st.session_state flags."""
        action_map = {
            "run_analysis_request": self._run_backtest_or_optimization,
            "apply_screener_request": self._run_screener,
            "clear_screener_request": self._clear_screener,
            "run_stat_test_request": self._run_statistical_test,
            "fetch_universe_request": lambda: self._run_universe_fetch(
                st.session_state.get("fetch_universe_request")
            ),
            "run_pipeline_request": lambda: self._run_main_pipeline(
                self.selections.get("full_backfill", False)
            ),
            "add_ticker_request": lambda: self._add_ticker_and_run_pipeline(
                st.session_state.get("add_ticker_request")
            ),
            "create_portfolio_request": lambda: self._create_portfolio(
                st.session_state.get("create_portfolio_request")
            ),
            "delete_portfolio_request": lambda: self._delete_portfolio(
                st.session_state.get("delete_portfolio_request")
            ),
            "save_watchlist_request": lambda: self._save_watchlist(
                st.session_state.get("save_watchlist_request")
            ),
            "delete_watchlist_request": lambda: self._delete_watchlist(
                st.session_state.get("delete_watchlist_request")
            ),
            "save_results_request": lambda: self._save_results(
                st.session_state.get("save_results_request")
            ),
            "load_results_request": lambda: self._load_results(
                st.session_state.get("load_results_request")
            ),
            "save_johansen_portfolio_request": lambda: self._save_johansen_portfolio(
                st.session_state.get("save_johansen_portfolio_request")
            ),
        }

        for flag, action in action_map.items():
            if st.session_state.get(flag):
                action()
                st.session_state.pop(flag, None)
                st.rerun()
                return

    def _run_backtest_or_optimization(self):
        """Routes the request to the correct handler with upfront validation."""
        backtest_mode = self.selections.get("backtest_mode", "Individual Ticker")
        is_optimization = self.selections.get("optimization_mode", False)

        if backtest_mode == "Individual Ticker":
            if is_optimization:
                self._run_parameter_optimization()
            else:
                self._run_individual_backtest()
        elif backtest_mode == "Portfolio":
            if is_optimization:
                self._run_portfolio_optimization()
            else:
                self._run_portfolio_backtest()

    def _create_strategy_model(self, params: dict) -> Optional[BaseAlphaModel]:
        """Factory method to create strategy model instances from sidebar selections."""
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
        st.error(f"Unknown strategy type: {strategy_type}")
        return None

    def _run_screener(self):
        """Runs the screening pipeline using data fetched by the PriceDataHandler."""
        screener_objects = self.selections.get("screener_objects", [])
        if not screener_objects:
            st.warning("Please select and configure at least one screener.")
            return

        with st.spinner("Running screener pipeline..."):
            source_type, source_name = self.selections.get(
                "source_type"
            ), self.selections.get("source_name")
            if source_type == "asset_type":
                initial_universe = self.db_manager.get_tickers_by_asset_type(
                    source_name
                )
            elif source_type == "watchlist":
                initial_universe = self.watchlist_manager.watchlists.get(
                    source_name, []
                )
            else:
                st.error(f"Invalid source for screener: {source_name}")
                return

            price_data_dict = self.price_handler.get_full_data_for_tickers(
                initial_universe,
                self.selections["start_date"].strftime("%Y-%m-%d"),
                self.selections["end_date"].strftime("%Y-%m-%d"),
            )
            pipeline = ScreenerPipeline(*screener_objects)
            final_tickers = pipeline.run(initial_universe, price_data_dict)
            analysis_df = self._build_screener_analysis_df(
                final_tickers, screener_objects, price_data_dict
            )

            st.session_state.update(
                {
                    "screened_tickers": final_tickers,
                    "screener_analysis_df": analysis_df,
                    "backtest_run": None,
                }
            )

    def _run_individual_backtest(self):
        """Runs a standard backtest on a set of symbols."""
        selected_symbols = self.selections.get("selected_symbols", [])
        if not selected_symbols:
            st.warning("Please select at least one ticker for analysis.")
            return

        start_date, end_date = self.selections["start_date"].strftime(
            "%Y-%m-%d"
        ), self.selections["end_date"].strftime("%Y-%m-%d")
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
            backtester = Backtester()
            model = self._create_strategy_model(self.selections)
            if not model:
                return

            for symbol, data in backtest_data_dict.items():
                # --- REFACTOR: Cleaned up redundant code block ---
                portfolio = backtester.run(price_data=data, model=model)
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

    def _run_portfolio_backtest(self):
        """Runs a single backtest on a portfolio of assets."""
        selected_symbols = self.selections.get("selected_symbols", [])
        if (
            self.selections.get("strategy_type") == "Pairs Trading"
            and len(selected_symbols) != 2
        ):
            st.warning("Pairs Trading requires exactly two tickers to be selected.")
            return

        if (
            len(selected_symbols) < 2
            and self.selections.get("strategy_type") != "Pairs Trading"
        ):
            st.warning("Portfolio analysis requires at least two tickers.")
            return

        start_date, end_date = self.selections["start_date"].strftime(
            "%Y-%m-%d"
        ), self.selections["end_date"].strftime("%Y-%m-%d")
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
            model = self._create_strategy_model(self.selections)
            if not model:
                return

            backtester = PortfolioBacktester()
            portfolio_weights = {
                symbol: 1.0 / len(price_data) for symbol in price_data.keys()
            }
            if self.selections.get("source_type") == "portfolio":
                portfolio_data = self.portfolio_manager.portfolios.get(
                    self.selections.get("source_name"), {}
                )
                if portfolio_data.get("weights"):
                    portfolio_weights = portfolio_data["weights"]

            # --- REFACTOR: Centralized signal generation and backtest run ---
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
            elif isinstance(model, CointegratedMeanReversionStrategy):
                price_df = pd.DataFrame(
                    {symbol: data["Close"] for symbol, data in price_data.items()}
                ).dropna()
                signals_df = model.generate_signals(price_df)
                signals_data = {"Portfolio": signals_df}
            else:
                # Default case for strategies that operate on individual tickers
                signals_data = {
                    symbol: model.generate_signals(data)
                    for symbol, data in price_data.items()
                }

            # --- FIX: Removed the redundant, bug-causing second call to backtester.run ---
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

    def _run_parameter_optimization(self):
        """Runs a parameter optimization for a single ticker."""
        strategy_type = self.selections.get("strategy_type")
        if strategy_type == "Buy and Hold":
            st.error("Parameter Optimization is not applicable to 'Buy and Hold'.")
            return

        symbol = self.selections.get("selected_symbols", [])[0]
        with st.spinner(f"Running optimization for {symbol}... This may take a while."):
            start_date, end_date = self.selections["start_date"].strftime(
                "%Y-%m-%d"
            ), self.selections["end_date"].strftime("%Y-%m-%d")
            backtest_data_dict = self.price_handler.get_full_data_for_tickers(
                [symbol], start_date, end_date
            )
            if not backtest_data_dict:
                st.error(f"No data for {symbol} in the selected range.")
                return
            backtest_data = backtest_data_dict[symbol]

            if strategy_type == "Mean Reversion":
                param_generator = MeanReversionParameterGenerator(self.selections)
            elif strategy_type == "Moving Average Crossover":
                param_generator = MACrossoverParameterGenerator(self.selections)
            else:
                st.error(f"Optimization is not supported for '{strategy_type}'.")
                return

            param_results = []
            backtester = Backtester()
            for (
                model_params,
                display_params,
            ) in param_generator.generate_combinations():
                model = self._create_strategy_model(model_params)
                if not model:
                    continue
                stats = backtester.run_and_get_metrics(
                    price_data=backtest_data, model=model
                )
                if stats:
                    stats.update(display_params)
                    param_results.append(stats)
            st.session_state.optimization_run = {
                "results": pd.DataFrame(param_results),
                "metric": self.selections["optimize_metric"],
                "strategy": strategy_type,
                "symbol": symbol,
            }

    def _run_portfolio_optimization(self):
        """Runs a Monte Carlo simulation to find optimal weights for a Buy & Hold strategy."""
        selected_symbols = self.selections.get("selected_symbols", [])
        if len(selected_symbols) < 2:
            st.warning("Please select at least two tickers for portfolio optimization.")
            return

        start_date, end_date = self.selections["start_date"].strftime(
            "%Y-%m-%d"
        ), self.selections["end_date"].strftime("%Y-%m-%d")
        with st.spinner(f"Fetching data for {len(selected_symbols)} tickers..."):
            price_data = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            if not price_data:
                st.error("Could not fetch sufficient data for portfolio optimization.")
                return

        optimizer = PortfolioOptimizer(
            symbols=list(price_data.keys()),
            price_data=price_data,
            strategy_model=BuyAndHoldStrategy(),
        )
        progress_bar = st.progress(0, text="Running Monte Carlo Simulation...")

        def update_progress(progress_value):
            """A simple function to update the Streamlit progress bar."""
            progress_bar.progress(
                progress_value,
                text=f"Running Trial {int(progress_value * 500)}/500",
            )

        results_df = optimizer.run_monte_carlo(
            num_trials=500, progress_callback=update_progress
        )
        progress_bar.empty()

        if results_df is None or results_df.empty:
            st.error("Portfolio optimization failed to produce results.")
            return

        st.session_state.optimization_run = {
            "results": results_df,
            "metric": self.selections.get("optimize_metric"),
            "strategy": "Buy & Hold Weight Optimization",
            "symbol": f"{len(selected_symbols)} Tickers",
        }

    def _run_statistical_test(self):
        """
        Runs the selected statistical test by dispatching to a specific runner method.
        This is a more scalable and maintainable approach.
        """
        test_type = self.selections.get("stat_test_type")

        # --- REFACTOR: Use a dispatch dictionary to map test types to methods ---
        test_runners = {
            "Augmented Dickey-Fuller Test": self._run_adf_test,
            "OLS Regression (Alpha/Beta)": self._run_ols_regression,
            "Engle-Granger Cointegration Test": self._run_engle_granger_test,
            "Johansen Cointegration Test": self._run_johansen_test,
            "Kalman Filter Smoother": self._run_kalman_filter,
            "Principal Component Analysis (PCA)": self._run_pca,
        }

        runner = test_runners.get(test_type)
        if not runner:
            st.error(f"Unknown statistical test type: {test_type}")
            return

        with st.spinner(f"Fetching data and running {test_type}..."):
            try:
                # Each runner method is now responsible for its own logic and error handling
                results, benchmark = runner()
                st.session_state.stat_test_run = {
                    "results": results,
                    "benchmark": benchmark,
                    "test_type": test_type,
                }
            except Exception as e:
                st.session_state.stat_test_run = None
                st.session_state.app_error = f"An error occurred during analysis: {e}"

    def _get_test_data(self, num_assets: int = None, require_benchmark: bool = False):
        """Helper to fetch and validate data for statistical tests."""
        selected_symbols = self.selections.get("selected_symbols", [])
        if num_assets is not None and len(selected_symbols) != num_assets:
            raise ValueError(f"This test requires exactly {num_assets} assets.")
        if not selected_symbols:
            raise ValueError("Please select at least one ticker for analysis.")

        # --- FIX: Adjust end_date to be inclusive in queries ---
        # The price handler might use a strict '<' comparison on dates. Adding
        # a day to the end_date ensures that all data for the selected end day
        # is included in the query result, resolving off-by-one issues.
        start_date = self.selections["start_date"]
        end_date = self.selections["end_date"] + pd.Timedelta(days=1)

        tickers_to_fetch = selected_symbols[:]
        benchmark_symbol = None
        if require_benchmark:
            benchmark_symbol = self.selections.get("selected_benchmark")
            if not benchmark_symbol or benchmark_symbol == "None":
                raise ValueError("This test requires a benchmark to be selected.")
            tickers_to_fetch.append(benchmark_symbol)

        price_df = self.price_handler.get_prices(
            tickers_to_fetch,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
        if price_df.empty:
            # --- FIX: Add diagnostic check to provide a more helpful error message ---
            missing_from_db = []
            conn = self.db_manager._get_connection()
            for ticker in tickers_to_fetch:
                try:
                    # Check if at least one row exists for the ticker in the price table
                    check_df = pd.read_sql(
                        "SELECT 1 FROM price_data_daily WHERE Ticker = ? LIMIT 1",
                        conn,
                        params=(ticker,),
                    )
                    if check_df.empty:
                        missing_from_db.append(ticker)
                except Exception:
                    # In case of a broader DB issue, add to list to be safe
                    missing_from_db.append(ticker)

            if missing_from_db:
                # This error means the data pipeline has likely not been run for these tickers.
                raise ValueError(
                    f"No price data found in the database for: {', '.join(missing_from_db)}. "
                    "Please ensure the data ingestion pipeline has been run for these assets."
                )
            else:
                # This error means data exists, but just not in the selected time frame.
                raise ValueError(
                    "No data available for the selected assets in the given date range. "
                    "Please try adjusting the start or end dates."
                )
        return price_df, selected_symbols, benchmark_symbol

    def _run_adf_test(self):
        """Runs the Augmented Dickey-Fuller test for selected assets."""
        price_df, selected_symbols, _ = self._get_test_data()
        results = {
            symbol: self.statistical_analyzer.run_adf_test(price_df[symbol])
            for symbol in selected_symbols
        }
        return results, None

    def _run_ols_regression(self):
        """Runs OLS regression for selected assets against a benchmark."""
        price_df, selected_symbols, benchmark_symbol = self._get_test_data(
            require_benchmark=True
        )
        # Cointegration and regression tests require complete, aligned data.
        price_df.dropna(inplace=True)
        if price_df.empty:
            raise ValueError(
                "No overlapping data found for the selected assets and benchmark."
            )
        asset_returns = price_df[selected_symbols].pct_change()
        benchmark_returns = price_df[benchmark_symbol].pct_change()
        results = {
            symbol: self.statistical_analyzer.run_ols_regression(
                asset_returns[symbol], benchmark_returns
            )
            for symbol in selected_symbols
        }
        return results, benchmark_symbol

    def _run_engle_granger_test(self):
        """Runs the Engle-Granger cointegration test."""
        price_df, selected_symbols, _ = self._get_test_data(num_assets=2)
        # Cointegration and regression tests require complete, aligned data.
        price_df.dropna(inplace=True)
        if price_df.empty:
            raise ValueError("No overlapping data found for the selected asset pair.")
        results = {
            f"{selected_symbols[0]} & {selected_symbols[1]}": self.statistical_analyzer.run_engle_granger_test(
                price_df[selected_symbols[0]], price_df[selected_symbols[1]]
            )
        }
        return results, None

    def _run_johansen_test(self):
        """Runs the Johansen cointegration test."""
        price_df, selected_symbols, _ = self._get_test_data()
        if len(selected_symbols) < 2:
            raise ValueError("Johansen test requires at least two assets.")
        # Cointegration and regression tests require complete, aligned data.
        price_df.dropna(inplace=True)
        if price_df.empty:
            raise ValueError("No overlapping data found for the selected assets.")
        results = {
            f"Johansen Test on {len(selected_symbols)} Tickers": self.statistical_analyzer.run_johansen_test(
                price_df[selected_symbols]
            )
        }
        return results, None

    def _run_kalman_filter(self):
        """Runs the Kalman Filter smoother on selected assets."""
        price_df, selected_symbols, _ = self._get_test_data()
        results = {
            symbol: self.statistical_analyzer.run_kalman_filter_smoother(
                price_df[symbol]
            )
            for symbol in selected_symbols
        }
        return results, None

    def _run_pca(self):
        """Runs Principal Component Analysis on the selected assets' returns."""
        price_df, selected_symbols, _ = self._get_test_data()
        # PCA requires a complete data matrix with no missing values.
        price_df.dropna(inplace=True)
        if price_df.empty:
            raise ValueError(
                "Could not fetch sufficient overlapping data for the selected assets."
            )
        if len(selected_symbols) < 2:
            raise ValueError("PCA requires at least two assets.")
        returns_df = price_df[selected_symbols].pct_change().dropna()

        # --- FIX: Add more robust pre-analysis checks for PCA ---
        # 1. Check for sufficient data points (more observations than assets)
        if returns_df.shape[0] < returns_df.shape[1]:
            raise ValueError(
                f"Not enough data for PCA ({returns_df.shape[0]} data points for "
                f"{returns_df.shape[1]} assets). Please select a longer date range "
                "or fewer assets."
            )

        # 2. Check for columns with zero variance, which are invalid for PCA
        # Use a tolerance for robust floating point comparison
        zero_variance_cols = returns_df.columns[returns_df.var() < 1e-10].tolist()
        if zero_variance_cols:
            raise ValueError(
                "PCA cannot be computed on data with zero variance. The following "
                f"assets had no price changes in the selected period: {', '.join(zero_variance_cols)}. "
                "Please select a different date range or remove these assets."
            )

        analyzer = PrincipalComponentAnalyzer(returns_df)
        results = analyzer.run()
        return results, None

    def _add_ticker_and_run_pipeline(self, data: dict):
        """
        Handles the request to manually add a ticker to the database and
        then trigger a pipeline run to fetch its data.
        """
        if not data:
            return

        ticker = data.get("ticker")
        asset_type = data.get("asset_type")

        if not ticker or not asset_type:
            st.error("Ticker and Asset Type are required.")
            return

        # Use the DatabaseManager to add the ticker to the canonical universe
        success, message = self.db_manager.add_ticker_to_universe(ticker, asset_type)

        if success:
            st.toast(message, icon="✅")
            # Clear the ticker cache to reflect the new addition in the UI
            self._get_cached_tickers.clear()
            # Now, trigger the pipeline run to backfill data for the new ticker
            self._run_main_pipeline(full_backfill=True)
        else:
            # If the ticker already exists, we don't need to run the pipeline
            # unless the user explicitly asks.
            st.warning(message, icon="⚠️")

    def _run_universe_fetch(self, source: str):
        """Fetches a ticker universe, normalizes it, and updates the database."""
        with st.spinner(f"Fetching and normalizing '{source}' universe..."):
            try:
                tickers, metadata = self.universe_fetcher.run(source=source)
                if not tickers:
                    st.error(f"Failed to fetch any tickers from the {source} source.")
                    return
                rows_affected = self.db_manager.update_universe(
                    source, tickers, metadata
                )
                self._get_cached_tickers.clear()
                st.success(
                    f"Successfully updated database with {rows_affected} tickers from {source}."
                )
                st.balloons()
                st.info(
                    "💡 Next step: Run the Data Ingestion Pipeline to download price history for the new tickers.",
                    icon="➡️",
                )
            except Exception as e:
                st.error(f"An error occurred during universe fetch: {e}")
                st.exception(e)

    def _run_main_pipeline(self, full_backfill: bool):
        """Handles the execution of the main data pipeline as a subprocess."""
        command = [sys.executable, "-m", "cli.run_pipeline"]
        if full_backfill:
            command.append("--full-backfill")
        st.info(
            "🚀 Starting data pipeline... This may take a few minutes. Please wait."
        )
        with st.expander("Show Pipeline Logs", expanded=True):
            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                )
                log_container = st.empty()
                log_output = ""
                for line in iter(process.stdout.readline, ""):
                    log_output += line
                    log_container.code(log_output, language="log")
                process.wait()
                if process.returncode == 0:
                    st.success("✅ Pipeline completed successfully!")
                    # --- FIX: Clear all relevant caches after a successful run ---
                    # This ensures the app state is synchronized with the updated database.
                    self._get_cached_tickers.clear()
                    # This is the crucial fix: clear the price data cache
                    self.price_handler.get_prices.clear()
                    self.price_handler.get_full_data_for_tickers.clear()
                    self.asset_deep_dive_tab._get_ticker_info.clear()
                    self.asset_deep_dive_tab._get_history.clear()
                    self.asset_deep_dive_tab._get_financials.clear()
                    self.asset_deep_dive_tab._get_news.clear()
                    st.balloons()
                else:
                    st.error(f"❌ Pipeline failed with exit code {process.returncode}.")
            except Exception as e:
                st.exception(e)

    def _build_screener_analysis_df(
        self, tickers: list, screeners: list, data: dict
    ) -> pd.DataFrame:
        """
        Builds a DataFrame with relevant metrics for the tickers that passed the screen.
        This version is refactored to be data-driven and easily extensible.
        """
        if not tickers:
            return pd.DataFrame()

        # --- REFACTOR: Use a config dict for cleaner, extensible formatting ---
        metric_format_config = {
            # "source_col_name": ("Display Name", "format_string")
            "beta": ("Beta", "{:.2f}"),
            "rsi_14d": ("RSI (14d)", "{:.0f}"),
            "volatility_90d": ("Volatility (90d)", "{:.2%}"),
        }

        analysis_rows = []
        for ticker in tickers:
            row = {"Ticker": ticker}
            if ticker not in data or data[ticker].empty:
                continue

            ticker_data = data[ticker]
            for screener in screeners:
                # Add metrics used by the screener itself
                row.update(screener.get_analysis_metric(ticker_data))

            latest_data = ticker_data.iloc[-1]
            # Loop through the config to format and add metrics
            for col, (display_name, fmt) in metric_format_config.items():
                if col in latest_data and pd.notna(latest_data[col]):
                    row[display_name] = fmt.format(latest_data[col])

            analysis_rows.append(row)

        return pd.DataFrame(analysis_rows).set_index("Ticker")

    def _clear_screener(self):
        st.session_state.screened_tickers = None
        st.session_state.screener_analysis_df = None

    def _create_portfolio(self, name: str):
        self.portfolio_manager.save_portfolio(name, constituents=[], weights={})
        st.toast(f"Portfolio '{name}' created.")

    def _delete_portfolio(self, name: str):
        """Deletes a portfolio from the portfolio manager."""
        self.portfolio_manager.delete(name)
        st.toast(f"Portfolio '{name}' deleted.")

    def _save_watchlist(self, data: dict):
        """Saves a watchlist using the watchlist manager."""
        name = data.get("name")
        tickers = data.get("tickers")
        if name and tickers:
            self.watchlist_manager.add_or_update(name, tickers)
            st.toast(f"Watchlist '{name}' saved.")
        else:
            st.sidebar.warning("Please provide a name and at least one ticker.")

    def _delete_watchlist(self, name: str):
        """Deletes a watchlist from the watchlist manager."""
        self.watchlist_manager.delete(name)
        st.toast(f"Watchlist '{name}' deleted.")

    def _save_results(self, filename: str):
        """Saves the current backtest or optimization results to a file."""
        if st.session_state.get("backtest_run"):
            self.results_manager.save(filename, st.session_state.backtest_run)
            st.sidebar.success(f"Saved backtest results to '{filename}.pkl'.")
        elif st.session_state.get("optimization_run"):
            self.results_manager.save(filename, st.session_state.optimization_run)
            st.sidebar.success(f"Saved optimization results to '{filename}.pkl'.")
        else:
            st.sidebar.warning("No analysis results to save.")

    def _load_results(self, filename: str):
        """Handles loading saved results using the ResultsManager."""
        if not filename:
            return
        loaded_data = self.results_manager.load(filename)
        if loaded_data:
            # Check the structure to determine if it's a backtest or optimization result
            if "results" in loaded_data and "benchmarks" in loaded_data:
                st.session_state.backtest_run = loaded_data
                st.session_state.optimization_run = None
                st.success(f"Loaded backtest results from '{filename}'.")
            elif "results" in loaded_data and "metric" in loaded_data:
                st.session_state.optimization_run = loaded_data
                st.session_state.backtest_run = None
                st.success(f"Loaded optimization results from '{filename}'.")
            else:
                st.error("The selected file is not a valid result file.")
        else:
            st.error(f"Could not load results from '{filename}'.")

    def _save_johansen_portfolio(self, portfolio_data: dict):
        """Saves the discovered cointegrated portfolio using the PortfolioManager."""
        name = portfolio_data.get("name")
        tickers = portfolio_data.get("tickers")
        weights = portfolio_data.get("weights")

        if not name or not tickers or not weights:
            st.error("Invalid portfolio data. Could not save.")
            return

        self.portfolio_manager.save_portfolio(
            name, constituents=tickers, weights=weights
        )
        st.sidebar.success(f"✅ Saved portfolio '{name}'.")


if __name__ == "__main__":
    app = DashboardApp()
    app.run()
