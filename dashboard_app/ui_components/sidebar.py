import pandas as pd
import streamlit as st

from dashboard_app.config_manager import ConfigManager
from dashboard_app.database_manager import DatabaseManager
from services.execution_service.portfolio_manager import PortfolioManager
from dashboard_app.results_manager import ResultsManager
from dashboard_app.watchlist_manager import WatchlistManager
from screeners.low_volatility_screener import LowVolatilityScreener
from screeners.momentum_screener import MomentumScreener
from screeners.fundamental_screener import FundamentalScreener


class Sidebar:
    """
    The Sidebar class is a "View" component. Its sole responsibility is to
    render the sidebar UI. It does not perform any actions itself but sets
    persistent request flags in st.session_state for the controller to act upon.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        results_manager: ResultsManager,
        watchlist_manager: WatchlistManager,
        portfolio_manager: PortfolioManager,
        all_db_tickers: list,
    ):
        """
        Initializes the Sidebar with service managers.
        """
        self.db_manager = db_manager
        self.results_manager = results_manager
        self.watchlist_manager = watchlist_manager
        self.portfolio_manager = portfolio_manager
        self.all_db_tickers = all_db_tickers
        self.config_manager = ConfigManager()

        self.watchlists = self.watchlist_manager.load()
        self.portfolios = self.portfolio_manager.get_all_portfolios().keys()

    def render(self) -> dict:
        """
        Renders all sidebar widgets and returns user inputs.
        Action triggers are handled via st.session_state.
        """
        selections = {}
        st.sidebar.title("⚙️ Quant Pipeline")

        # Create Tabs for better organization
        # 1. Dashboard: High level view
        # 2. Trading: Active paper trading
        # 3. Analysis: Charting and fundamental research
        # 4. Quant Lab: Backtesting, Screening, Signals
        # 5. Settings: App config
        tab_dash, tab_trade, tab_analysis, tab_quant, tab_settings = st.sidebar.tabs(
            ["📊 Dash", "⚡ Trade", "📈 Charts", "🧪 Lab", "⚙️ Config"]
        )

        # --- Tab 1: Dashboard ---
        with tab_dash:
            self._render_dashboard_summary(selections)

        # --- Tab 2: Trading ---
        with tab_trade:
            st.markdown("### ⚡ Paper Trading")
            self._render_trading_panel(selections)

        # --- Tab 3: Analysis (Charts) ---
        with tab_analysis:
            st.markdown("### 📈 Technical Analysis")
            # Reuse the ticker selection logic for charting
            self._render_ticker_selection(selections, key_suffix="chart")
            st.divider()
            st.caption("Select tickers to view interactive charts in the main area.")

        # --- Tab 4: Quant Lab (Backtest/Signals) ---
        with tab_quant:
            self._render_quant_lab(selections)

        # --- Tab 5: Configuration & Settings ---
        with tab_settings:
            st.markdown("### ⚙️ Settings")
            self._render_settings_tab(selections)

        return selections

        return selections

    # _render_analysis_panel removed in refactor


    def _render_strategy_comparison_configs(self, selections: dict):
        """Renders configuration for comparing multiple strategies."""
        with st.container(border=True):
            st.markdown("**Select Strategies to Compare**")
            strategy_options = [
                "Buy and Hold",
                "Mean Reversion",
                "Moving Average Crossover",
                "Trend Following",
            ]
            selections["comparison_strategies"] = st.multiselect(
                "Strategies",
                options=strategy_options,
                default=["Buy and Hold", "Mean Reversion"],
                help="Select 2 or more strategies to compare using default parameters."
            )
            
            benchmark_options = [
                "None", 
                "S&P 500 (SPY)", 
                "Nasdaq 100 (QQQ)", 
                "Russell 2000 (IWM)"
            ] + [t for t in self.all_db_tickers if t not in ["SPY", "QQQ", "IWM"]]
            
            default_benchmark_index = 1 if "S&P 500 (SPY)" in benchmark_options else 0
            
            selections["selected_benchmark"] = st.selectbox(
                "Benchmark", 
                options=benchmark_options, 
                index=default_benchmark_index,
                key="comparison_benchmark"
            )

            # Map display name back to ticker symbol
            if "SPY" in selections["selected_benchmark"]: selections["selected_benchmark"] = "SPY"
            elif "QQQ" in selections["selected_benchmark"]: selections["selected_benchmark"] = "QQQ"
            elif "IWM" in selections["selected_benchmark"]: selections["selected_benchmark"] = "IWM"

            if st.button(
                "▶️ Run Comparison", use_container_width=True, type="primary"
            ):
                st.session_state["run_analysis_request"] = True
                # Use a specific flag/mode to distinguish or just rely on 'analysis_type' in selections

    def _render_backtest_screener_configs(self, selections: dict):
        """Renders configuration options for backtesting and screening."""
        with st.container(border=True):
            st.markdown("**Screener**")
            selections["screener_objects"] = []
            if st.checkbox("Enable Low Volatility Screener"):
                percentile = st.slider(
                    "Volatility Percentile", 0.0, 1.0, 0.2, 0.05, key="screener_vol_pct"
                )
                selections["screener_objects"].append(LowVolatilityScreener(percentile))
            if st.checkbox("Enable Momentum Screener"):
                window = st.slider(
                    "Momentum Window (Days)", 10, 252, 90, key="screener_mom_win"
                )
                selections["screener_objects"].append(MomentumScreener(window))
            
            if st.checkbox("Enable Fundamental Screener (Price/Vol)"):
                c1, c2 = st.columns(2)
                min_price = c1.number_input("Min Price ($)", value=5.0, min_value=0.0, step=1.0)
                min_vol = c2.number_input("Min Avg Vol", value=100000, min_value=0, step=100000)
                selections["screener_objects"].append(FundamentalScreener(min_price=min_price, min_avg_volume=min_vol))
            col1, col2 = st.columns(2)
            if col1.button("Apply Screener", use_container_width=True):
                st.session_state["apply_screener_request"] = True
            if col2.button("Clear Screener", use_container_width=True):
                st.session_state["clear_screener_request"] = True

            st.divider()
            st.markdown("**Backtest / Optimization**")
            
            # --- General Backtest Settings ---
            c1, c2 = st.columns(2)
            selections["initial_capital"] = c1.number_input("Initial Capital", value=100000.0, step=1000.0)
            selections["commission"] = c2.number_input("Commission", value=0.0, step=0.01)

            st.markdown("**Position Sizing**")
            if st.checkbox("Enable Volatility Targeting", help="Dynamically sizes positions based on asset volatility."):
                selections["enable_vol_targeting"] = True
                selections["target_volatility"] = st.slider("Target Ann. Volatility", 0.05, 0.40, 0.15, 0.01)
            else:
                selections["enable_vol_targeting"] = False
                selections["target_volatility"] = 0.15
            st.divider()


            selections["backtest_mode"] = st.radio(
                "Backtest Mode",
                ["Individual Ticker", "Portfolio"],
                horizontal=True,
                help=(
                    "**Individual Ticker:** Run a separate backtest for each selected ticker.\n\n"
                    "**Portfolio:** Run a single backtest on all selected tickers as a combined portfolio."
                ),
            )
            strategy_options = [
                "Buy and Hold",
                "Mean Reversion",
                "Moving Average Crossover",
                "RSI Oscillator",
                "ATR Breakout",
                "Random Forest",
            ]
            if selections["backtest_mode"] == "Portfolio":
                strategy_options.append("Cointegrated Mean Reversion")
                strategy_options.append("Index Rebalancing")
            selections["strategy_type"] = st.selectbox(
                "Strategy Type", strategy_options
            )

            selections["optimization_mode"] = st.checkbox(
                "Enable Parameter Optimization"
            )
            if selections["strategy_type"] == "Mean Reversion":
                if selections["optimization_mode"]:
                    selections["mr_window_range"] = st.slider(
                        "Window Range", 1, 100, (5, 20)
                    )
                    selections["mr_threshold_range"] = st.slider(
                        "Threshold Range", 0.1, 3.0, (0.5, 1.5), 0.1
                    )
                else:
                    selections["mr_window"] = st.slider(
                        "Z-Score Window", 5, 100, 20
                    )
                    selections["mr_threshold"] = st.slider(
                        "Z-Score Threshold", 0.5, 3.0, 1.0, 0.1
                    )
            elif selections["strategy_type"] == "Index Rebalancing":
                selections["rebalance_freq"] = st.selectbox(
                    "Rebalance Frequency",
                    options=["M", "W", "Q"],
                    format_func=lambda x: {"M": "Monthly", "W": "Weekly", "Q": "Quarterly"}[x],
                    index=0
                )
            elif selections["strategy_type"] == "Moving Average Crossover":
                if selections["optimization_mode"]:
                    selections["mac_short_range"] = st.slider(
                        "Short MA Range", 5, 100, (10, 30)
                    )
                    selections["mac_long_range"] = st.slider(
                        "Long MA Range", 20, 250, (40, 60)
                    )
                else:
                    selections["mac_short_window"] = st.slider(
                        "Short MA Window", 5, 100, 20
                    )
                    selections["mac_long_window"] = st.slider(
                        "Long MA Window", 20, 250, 50
                    )
            elif selections["strategy_type"] == "RSI Oscillator":
                selections["rsi_window"] = st.slider("RSI Window", 2, 50, 14)
                col1, col2 = st.columns(2)
                selections["rsi_buy_threshold"] = col1.slider("Buy Below (Oversold)", 10, 45, 30)
                selections["rsi_sell_threshold"] = col2.slider("Sell Above (Overbought)", 55, 90, 70)
            elif selections["strategy_type"] == "ATR Breakout":
                selections["atr_window"] = st.slider("ATR Window", 5, 50, 20)
                selections["atr_multiplier"] = st.slider("Multiplier", 0.5, 5.0, 2.0, 0.1)
            elif selections["strategy_type"] == "Random Forest":
                selections["ml_n_estimators"] = st.slider("Number of Estimators", 10, 500, 100, 10)
                selections["ml_lookback_window"] = st.slider("Lookback Window (Days)", 1, 30, 5)
            if selections["optimization_mode"]:
                selections["optimize_metric"] = st.selectbox(
                    "Metric to Optimize",
                    (
                        "Sharpe Ratio",
                        "Sortino Ratio",
                        "Calmar Ratio",
                        "Total Return",
                        "Annualized Return",
                        "Annualized Volatility",
                        "Max Drawdown"
                    ),
                )

            benchmark_options = ["None"] + self.all_db_tickers
            default_benchmark_index = (
                benchmark_options.index("SPY") if "SPY" in benchmark_options else 0
            )
            selections["selected_benchmark"] = st.selectbox(
                "Benchmark", options=benchmark_options, index=default_benchmark_index
            )

            if st.button(
                "▶️ Run Backtest", use_container_width=True, type="primary"
            ):
                st.session_state["run_analysis_request"] = True

    def _render_statistical_test_configs(self, selections: dict):
        """Renders configuration for statistical tests."""
        with st.container(border=True):
            test_options = [
                "Augmented Dickey-Fuller Test",
                "OLS Regression (Alpha/Beta)",
                "Engle-Granger Cointegration Test",
                "Johansen Cointegration Test",
                "Kalman Filter Smoother",
                "Principal Component Analysis (PCA)",
                "Monte Carlo Simulation",
                "Cluster Analysis (K-Means)",
            ]
            selections["stat_test_type"] = st.selectbox(
                "Test Type", options=test_options, key="stat_test_type"
            )

            if selections["stat_test_type"] == "OLS Regression (Alpha/Beta)":
                benchmark_options = ["None"] + self.all_db_tickers
                default_benchmark_index = (
                    benchmark_options.index("SPY") if "SPY" in benchmark_options else 0
                )
                selections["selected_benchmark"] = st.selectbox(
                    "Benchmark",
                    options=benchmark_options,
                    index=default_benchmark_index,
                    help="Select a benchmark to regress against for Alpha/Beta calculation.",
                    key="selected_benchmark_stat_test",  # Use a unique key
                )
            elif selections["stat_test_type"] == "Monte Carlo Simulation":
                selections["mc_simulations"] = st.slider(
                    "Number of Simulations", 100, 5000, 1000, 100, help="Number of random paths to generate."
                )
                selections["mc_horizon"] = st.slider(
                    "Time Horizon (Days)", 30, 365, 252, 10, help="Days into the future to simulate."
                )
            elif selections["stat_test_type"] == "Cluster Analysis (K-Means)":
                selections["cluster_k"] = st.slider(
                    "Number of Clusters (k)", 2, 10, 4, 1, help="Number of groups to form."
                )
            
            if st.button(
                "▶️ Run Statistical Test", use_container_width=True, type="primary"
            ):
                st.session_state["run_stat_test_request"] = True

    def _render_portfolio_management(self, selections: dict):
        """Renders controls to select, create, or delete a portfolio definition."""
        portfolio_options = [""] + list(self.portfolios)
        selections["selected_portfolio_to_manage"] = st.selectbox(
            "Select Portfolio to View/Edit",
            options=portfolio_options,
            help="Choose a portfolio to view and edit its trade history in the 'Portfolio' tab.",
        )

        with st.form("create_portfolio_form", clear_on_submit=True):
            new_portfolio_name = st.text_input(
                "Or, Create New Portfolio", placeholder="e.g., Dividend Growth"
            )
            submitted = st.form_submit_button("Create Portfolio")
            if submitted and new_portfolio_name:
                st.session_state["create_portfolio_request"] = (
                    new_portfolio_name.strip()
                )

        if selections["selected_portfolio_to_manage"]:
            if st.button(
                f"❌ Delete '{selections['selected_portfolio_to_manage']}'",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state["delete_portfolio_request"] = selections[
                    "selected_portfolio_to_manage"
                ]

    def _render_watchlist_management(self, selections: dict):
        """Renders a full CRUD interface for managing watchlists."""
        st.subheader("Watchlists")

        def on_watchlist_select():
            """Callback to populate the form when a watchlist is selected."""
            selected = st.session_state.get(
                "watchlist_selector", "Create New Watchlist"
            )
            if selected != "Create New Watchlist":
                st.session_state.watchlist_form_name = selected
                st.session_state.watchlist_form_tickers = self.watchlists.get(
                    selected, []
                )
            else:
                st.session_state.watchlist_form_name = ""
                st.session_state.watchlist_form_tickers = []

        watchlist_options = ["Create New Watchlist"] + list(self.watchlists.keys())
        st.selectbox(
            "Select Watchlist to Edit",
            options=watchlist_options,
            key="watchlist_selector",
            on_change=on_watchlist_select,
        )

        with st.form("watchlist_form", clear_on_submit=False):
            st.text_input("Watchlist Name", key="watchlist_form_name")
            st.multiselect(
                "Tickers", options=self.all_db_tickers, key="watchlist_form_tickers"
            )
            submitted_save = st.form_submit_button("💾 Save Watchlist")

            if submitted_save:
                name = st.session_state.get("watchlist_form_name", "").strip()
                tickers = st.session_state.get("watchlist_form_tickers", [])
                if name and tickers:
                    st.session_state["save_watchlist_request"] = {
                        "name": name,
                        "tickers": tickers,
                    }
                else:
                    st.warning("Please provide a name and at least one ticker.")

        selected_to_delete = st.session_state.get(
            "watchlist_selector", "Create New Watchlist"
        )
        if selected_to_delete != "Create New Watchlist":
            if st.button(
                f"❌ Delete '{selected_to_delete}'",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state["delete_watchlist_request"] = selected_to_delete

    def _render_data_management_panel(self, selections: dict):
        """Renders the admin panel for running the data pipeline and managing the universe."""
        st.markdown("**Fetch Ticker Universes**")
        st.info(
            "This updates the master list of tickers in the database.", icon="ℹ️"
        )

        col1, col2 = st.columns(2)
        if col1.button("Fetch S&P 500", use_container_width=True):
            st.session_state["fetch_universe_request"] = "S&P 500"
        if col2.button("Fetch Nasdaq 100", use_container_width=True):
            st.session_state["fetch_universe_request"] = "Nasdaq 100"
        if col1.button("Fetch Dow Jones", use_container_width=True):
            st.session_state["fetch_universe_request"] = "Dow Jones"
        if col2.button("Fetch Top Crypto", use_container_width=True):
            st.session_state["fetch_universe_request"] = "Top Crypto"

        st.divider()

        st.markdown("**Manually Add Ticker**")
        with st.form("sidebar_add_ticker_form", clear_on_submit=True):
            new_ticker = st.text_input("Ticker Symbol", placeholder="e.g., NVDA")
            asset_type = st.selectbox("Asset Type", ["Equity", "Crypto"])
            submitted = st.form_submit_button("Add Ticker & Run Pipeline")
            if submitted and new_ticker:
                st.session_state["add_ticker_request"] = {
                    "ticker": new_ticker.upper().strip(),
                    "asset_type": asset_type,
                }

        st.divider()

        st.markdown("**Ingest Price Data**")
        selections["full_backfill"] = st.checkbox(
            "Full Backfill (slower)",
            value=False,
            help="If checked, re-downloads all historical data for all tickers in the database. If unchecked, only downloads recent data.",
        )
        if st.button("Run Data Ingestion Pipeline", use_container_width=True):
            st.session_state["run_pipeline_request"] = True

    def _render_load_save_results(self, selections: dict):
        """Renders widgets for loading and saving backtest/optimization results."""
        st.markdown("**Data Source**")
        data_source = st.radio(
            "Select Source", 
            ["Local File System", "Remote API"], 
            horizontal=True,
            label_visibility="collapsed"
        )
        selections["data_source"] = data_source

        saved_files = []
        if data_source == "Local File System":
            saved_files = self.results_manager.get_saved_files()
        else:
            # API Mode
            from dashboard_app.api_client import ApiClient
            client = ApiClient() # Default to local info, or make this configurable
            saved_files = client.get_result_files()
            selections["api_client"] = client # Store for controller to use

        selections["file_to_load"] = st.selectbox(
            "Load Saved Analysis", options=[""] + saved_files
        )
        if st.button("Load Selected", use_container_width=True):
            if selections["file_to_load"]:
                st.session_state["load_results_request"] = {
                    "filename": selections["file_to_load"],
                    "source": "api" if data_source == "Remote API" else "local"
                }

        with st.form("save_results_form", clear_on_submit=True):
            filename = st.text_input("Filename to Save As")
            submitted = st.form_submit_button("Save Current Analysis")
            if submitted and filename:
                st.session_state["save_results_request"] = filename

    def _render_signals_panel(self, selections: dict):
        """
        Renders controls for generating verifiable signals.
        
        This panel is simplified for the MVP:
        1. Accepts a Ticker Symbol.
        2. Triggers a 'generate_signal_request' in session state.
        3. The Controller handles the actual API call on rerun.
        """
        with st.container(border=True):
            st.info("Request a cryptographically signed signal from the core gRPC service.")
            
            ticker = st.text_input("Ticker Symbol", value="AAPL", key="signal_ticker_input")
            
            if st.button("Generate Signed Signal", type="primary", use_container_width=True):
                if ticker:
                    # Signal the Controller to act
                    st.session_state["generate_signal_request"] = ticker.upper().strip()
                else:
                    st.warning("Please enter a ticker symbol.")
    def _render_dashboard_summary(self, selections: dict):
        """Renders high-level portfolio metrics."""
        # Use the portfolio selected in the Settings tab, or default to the first one/default
        # Note: Sidebar renders happen top-to-bottom. If user changes selection in Settings (Tab 5),
        # this Summary (Tab 1) might use the old value until next rerun, or we should lift the state.
        # For better UX, we can just allow selecting the "Active Dashboard Portfolio" right here or use the global one.
        
        # We will try to read the selection from session state or default
        selected_portfolio = selections.get("selected_portfolio_to_manage")
        
        port_state = self.portfolio_manager.get_portfolio_state(selected_portfolio)
        cash = port_state.get("cash", 0.0)
        positions = port_state.get("positions", {})
        
        # Display which portfolio is active
        if selected_portfolio:
            st.caption(f"Showing: **{selected_portfolio}**")

        # Calculate approximate equity (using last known avg price as proxy if no live feed here)
        # Ideally, this should come from a controller that has live prices
        equity = cash
        for _, pos in positions.items():
            equity += pos["quantity"] * pos["average_price"]

        st.markdown("### 🏦 **Summary**")
        col1, col2 = st.columns(2)
        col1.metric("Cash", f"${cash:,.0f}")
        col2.metric("Equity", f"${equity:,.0f}")
        
        st.divider()
        st.caption("Active Positions")
        if positions:
            for sym, pos in positions.items():
                st.markdown(f"**{sym}**: {pos['quantity']} @ ${pos['average_price']:.2f}")
        else:
            st.info("No active positions.")
            
    def _render_trading_panel(self, selections: dict):
        """Renders the paper trading execution form."""
        
        # Add Portfolio Selector
        portfolio_options = list(self.portfolios)
        target_portfolio = st.selectbox("Target Portfolio", portfolio_options, key="trade_target_portfolio")
        
        with st.form("trade_execution_form", clear_on_submit=True):
            symbol = st.text_input("Symbol", placeholder="AAPL").upper().strip()
            
            c1, c2 = st.columns(2)
            action = c1.selectbox("Action", ["BUY", "SELL", "SHORT"])
            quantity = c2.number_input("Quantity", min_value=0.01, value=10.0)
            
            price = st.number_input("Exec Price (Simulated)", value=100.0)
            
            submitted = st.form_submit_button("🚀 Execute Trade", type="primary")
            
            if submitted:
                if symbol and quantity > 0:
                    st.session_state["execute_trade_request"] = {
                        "symbol": symbol,
                        "action": action,
                        "quantity": quantity,
                        "price": price,
                        "portfolio_name": target_portfolio
                    }
                else:
                    st.error("Invalid Trade Parameters")

    def _render_ticker_selection(self, selections: dict, key_suffix: str = ""):
        """Reusable ticker selection component."""
        st.markdown("**1. Select Source**")
        source_map = {"-- Select a Source --": ("placeholder", None)}
        source_map.update(
            {
                "All Equities": ("asset_type", "Equity"),
                "All Crypto": ("asset_type", "Crypto"),
                "Filter by Sector": ("sector", None),  # New Option
                "Load from Portfolio": ("portfolio", None),  # New Option
            }
        )
        if self.watchlists:
            for name in self.watchlists:
                source_map[f"Watchlist: {name}"] = ("watchlist", name)
        
        # Use a unique key for widget to avoid collisions if used multiple times
        k = f"ticker_source_{key_suffix}"
        selected_source_name = st.selectbox(
            "Ticker Source",
            options=list(source_map.keys()),
            key=k
        )
        source_type, source_name = source_map.get(
            selected_source_name, ("placeholder", None)
        )
        
        # --- Handle Dynamic Sub-Selections ---
        if source_type == "sector":
            sectors = self.db_manager.get_sectors()
            if not sectors:
                st.warning("No sector data found in database.")
                source_name = None
            else:
                source_name = st.selectbox(
                    "Select Sector",
                    options=sectors,
                    key=f"sector_select_{key_suffix}"
                )

        elif source_type == "portfolio":
            if not self.portfolios:
                st.warning("No portfolios found.")
                source_name = None
            else:
                source_name = st.selectbox(
                    "Select Portfolio",
                    options=list(self.portfolios),
                    key=f"portfolio_load_{key_suffix}"
                )

        selections[f"source_type_{key_suffix}"] = source_type
        selections[f"source_name_{key_suffix}"] = source_name
        
        # Refine Selection
        available_tickers = []
        if source_type == "asset_type":
            available_tickers = self.db_manager.get_tickers_by_asset_type(source_name)
        elif source_type == "watchlist":
            available_tickers = self.watchlists.get(source_name, [])
        elif source_type == "sector":
            if source_name:
                available_tickers = self.db_manager.get_tickers_by_sector(source_name)
        elif source_type == "portfolio":
            if source_name:
                # Load tickers from the selected portfolio's current holdings or trade history
                # Ideally, we want unique tickers that have ever been traded or currently held.
                # For simplicity, let's pull all unique tickers from the portfolio's trade log.
                port_data = self.portfolio_manager.get_portfolio_state(source_name)
                trades = port_data.get("trades", [])
                available_tickers = list(set([t["ticker"] for t in trades]))
                if not available_tickers:
                    st.info("Selected portfolio has no trades.")
            
        selections[f"selected_symbols_{key_suffix}"] = st.multiselect(
            "Select Tickers",
            options=available_tickers,
            default=available_tickers,
            key=f"ticker_select_{key_suffix}"
        )

    def _render_quant_lab(self, selections: dict):
        """Consolidated Quant Lab: Screener, Signals, Backtest, Stats."""
        mode = st.radio("Lab Mode", ["Screener", "Backtest", "Stats", "Compare", "Signals"], horizontal=True)
        selections["quant_mode"] = mode
        
        if mode == "Screener":
            self._render_ticker_selection(selections, key_suffix="screener")
            self._render_backtest_screener_configs(selections)
        elif mode == "Backtest":
             self._render_ticker_selection(selections, key_suffix="backtest")
             
             # Setup columns for manual input and dates
             c_man, c_d1 = st.columns([2, 1]) 
             manual_str = c_man.text_input("Or Type Tickers", placeholder="AAPL, MSFT", key="manual_backtest_input")
             if manual_str:
                 extras = [t.strip().upper() for t in manual_str.split(",") if t.strip()]
                 current = selections.get("selected_symbols_backtest", [])
                 selections["selected_symbols_backtest"] = list(set(current + extras))
             
             c1, c2 = st.columns(2)
             selections["start_date_backtest"] = c1.date_input("Start", pd.to_datetime("2020-01-01"))
             selections["end_date_backtest"] = c2.date_input("End", pd.to_datetime("today"))
             selections["backtest_mode"] = "Individual Ticker"
             self._render_backtest_screener_configs(selections)
        elif mode == "Stats":
             self._render_ticker_selection(selections, key_suffix="stats")
             self._render_statistical_test_configs(selections)
        elif mode == "Compare":
             self._render_ticker_selection(selections, key_suffix="compare")
             self._render_strategy_comparison_configs(selections)
        elif mode == "Signals":
            self._render_signals_panel(selections)

    def _render_settings_tab(self, selections: dict):
        """Consolidated settings."""
        self._render_load_save_results(selections)
        st.divider()
        st.markdown("**Portfolio & Watchlists**")
        self._render_portfolio_management(selections)
        st.divider()
        self._render_watchlist_management(selections)
        st.divider()
        st.markdown("**Data Pipeline**")
        self._render_data_management_panel(selections)
        
        st.divider()
        st.markdown("**Config Management**")
        # Logic from old render method for config load/save
        save_config_name = st.text_input("Save Config As:", key="save_config_name_tab")
        if st.button("Save", key="btn_save_cfg"):
             if save_config_name:
                self.config_manager.save_config(save_config_name, st.session_state.selections)
                st.success("Saved!")
        
