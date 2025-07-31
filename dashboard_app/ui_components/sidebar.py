import pandas as pd
import streamlit as st

from dashboard_app.config_manager import ConfigManager
from dashboard_app.database_manager import DatabaseManager
from dashboard_app.portfolio_manager import PortfolioManager
from dashboard_app.results_manager import ResultsManager
from dashboard_app.watchlist_manager import WatchlistManager
from screeners.low_volatility_screener import LowVolatilityScreener
from screeners.momentum_screener import MomentumScreener


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
        st.sidebar.title("⚙️ Controls")

        # --- Main UI Flow ---
        self._render_analysis_panel(selections)

        # --- Management Panels (collapsible) ---
        with st.sidebar.expander("📂 Portfolio & Watchlist Management"):
            self._render_portfolio_management(selections)
            st.sidebar.divider()
            self._render_watchlist_management(selections)

        with st.sidebar.expander("💾 Load/Save Analysis Results"):
            self._render_load_save_results(selections)

        st.sidebar.divider()
        st.sidebar.subheader("⚙️ Configuration Management")

        # --- Save Configuration ---
        save_config_name = st.sidebar.text_input(
            "Save Config As:", key="save_config_name"
        )
        if st.sidebar.button("Save Configuration"):
            if save_config_name:
                # We get the selections dict which contains all current UI settings
                success, message = self.config_manager.save_config(
                    save_config_name, st.session_state.selections
                )
                if success:
                    st.sidebar.success(message)
                else:
                    st.sidebar.error(message)
            else:
                st.sidebar.warning("Please enter a name for the configuration.")

        # --- Load Configuration ---
        available_configs = self.config_manager.list_configs()
        if available_configs:
            selected_config = st.sidebar.selectbox(
                "Load Configuration:",
                [""] + available_configs,
                key="load_config_select",
            )
            if st.sidebar.button("Load Configuration"):
                if selected_config:
                    loaded_settings = self.config_manager.load_config(selected_config)
                    if loaded_settings:
                        # This is a simplified load; a full implementation would update all
                        # relevant session_state keys from the loaded_settings dict.
                        st.session_state.selections = loaded_settings
                        st.sidebar.success(
                            f"Loaded '{selected_config}'. Please re-run analysis."
                        )
                        st.rerun()  # Rerun to reflect the new settings in the UI

        with st.sidebar.expander("🔧 Data Pipeline & Universe"):
            self._render_data_management_panel(selections)

        return selections

    def _render_analysis_panel(self, selections: dict):
        """A single, unified panel for all analysis and screening tasks."""
        st.sidebar.header("🔬 Analysis Workflow")

        # --- Step 1: Select Ticker Source ---
        st.sidebar.subheader("1. Select Ticker Source")
        source_map = {"-- Select a Source --": ("placeholder", None)}
        source_map.update(
            {
                "All Equities": ("asset_type", "Equity"),
                "All Crypto": ("asset_type", "Crypto"),
            }
        )
        if self.watchlists:
            for name in self.watchlists:
                source_map[f"Watchlist: {name}"] = ("watchlist", name)
        if self.portfolios:
            for name in self.portfolios:
                source_map[f"Portfolio: {name}"] = ("portfolio", name)

        if st.session_state.get("screened_tickers") is not None:
            source_map["⭐ Screened Tickers"] = (
                "screener_results",
                "⭐ Screened Tickers",
            )
            default_source_index = list(source_map.keys()).index("⭐ Screened Tickers")
        else:
            default_source_index = 0

        selected_source_name = st.sidebar.selectbox(
            "Ticker Source",
            options=list(source_map.keys()),
            index=default_source_index,
            help="Choose the starting universe for your analysis or screener.",
        )
        source_type, source_name = source_map.get(
            selected_source_name, ("placeholder", None)
        )
        selections["source_type"] = source_type
        selections["source_name"] = source_name

        # --- Step 2: Refine Tickers & Date Range ---
        st.sidebar.subheader("2. Refine Selection")
        available_tickers = []
        if source_type == "asset_type":
            available_tickers = self.db_manager.get_tickers_by_asset_type(source_name)
        elif source_type == "watchlist":
            available_tickers = self.watchlists.get(source_name, [])
        elif source_type == "portfolio":
            portfolio_data = self.portfolio_manager.get_all_portfolios().get(
                source_name, {}
            )
            available_tickers = portfolio_data.get("constituents", [])
        elif source_type == "screener_results":
            available_tickers = st.session_state.get("screened_tickers", [])

        selections["selected_symbols"] = st.sidebar.multiselect(
            "Tickers for Action",
            options=available_tickers,
            default=available_tickers,
            help="These tickers will be used for the analysis. You can de-select any you wish to exclude.",
        )
        col1, col2 = st.sidebar.columns(2)
        selections["start_date"] = col1.date_input(
            "Start Date", pd.to_datetime("2020-01-01")
        )
        selections["end_date"] = col2.date_input("End Date", pd.to_datetime("today"))

        # --- Step 3: Configure & Run Analysis ---
        st.sidebar.subheader("3. Configure & Run")
        analysis_type = st.sidebar.radio(
            "Analysis Type",
            ["Backtest / Screener", "Statistical Tests"],
            horizontal=True,
            key="analysis_type",
        )
        selections["analysis_type"] = analysis_type

        if analysis_type == "Backtest / Screener":
            self._render_backtest_screener_configs(selections)
        elif analysis_type == "Statistical Tests":
            self._render_statistical_test_configs(selections)

    def _render_backtest_screener_configs(self, selections: dict):
        """Renders configuration options for backtesting and screening."""
        with st.sidebar.container(border=True):
            st.sidebar.markdown("**Screener**")
            selections["screener_objects"] = []
            if st.sidebar.checkbox("Enable Low Volatility Screener"):
                percentile = st.sidebar.slider(
                    "Volatility Percentile", 0.0, 1.0, 0.2, 0.05, key="screener_vol_pct"
                )
                selections["screener_objects"].append(LowVolatilityScreener(percentile))
            if st.sidebar.checkbox("Enable Momentum Screener"):
                window = st.sidebar.slider(
                    "Momentum Window (Days)", 10, 252, 90, key="screener_mom_win"
                )
                selections["screener_objects"].append(MomentumScreener(window))
            col1, col2 = st.sidebar.columns(2)
            if col1.button("Apply Screener", use_container_width=True):
                st.session_state["apply_screener_request"] = True
            if col2.button("Clear Screener", use_container_width=True):
                st.session_state["clear_screener_request"] = True

            st.sidebar.divider()
            st.sidebar.markdown("**Backtest / Optimization**")
            selections["backtest_mode"] = st.sidebar.radio(
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
            ]
            if selections["backtest_mode"] == "Portfolio":
                strategy_options.append("Cointegrated Mean Reversion")
            selections["strategy_type"] = st.sidebar.selectbox(
                "Strategy Type", strategy_options
            )

            selections["optimization_mode"] = st.sidebar.checkbox(
                "Enable Parameter Optimization"
            )
            if selections["strategy_type"] == "Mean Reversion":
                if selections["optimization_mode"]:
                    selections["mr_window_range"] = st.sidebar.slider(
                        "Window Range", 1, 100, (5, 20)
                    )
                    selections["mr_threshold_range"] = st.sidebar.slider(
                        "Threshold Range", 0.1, 3.0, (0.5, 1.5), 0.1
                    )
                else:
                    selections["mr_window"] = st.sidebar.slider(
                        "Z-Score Window", 5, 100, 20
                    )
                    selections["mr_threshold"] = st.sidebar.slider(
                        "Z-Score Threshold", 0.5, 3.0, 1.0, 0.1
                    )
            elif selections["strategy_type"] == "Moving Average Crossover":
                if selections["optimization_mode"]:
                    selections["mac_short_range"] = st.sidebar.slider(
                        "Short MA Range", 5, 100, (10, 30)
                    )
                    selections["mac_long_range"] = st.sidebar.slider(
                        "Long MA Range", 20, 250, (40, 60)
                    )
                else:
                    selections["mac_short_window"] = st.sidebar.slider(
                        "Short MA Window", 5, 100, 20
                    )
                    selections["mac_long_window"] = st.sidebar.slider(
                        "Long MA Window", 20, 250, 50
                    )
            if selections["optimization_mode"]:
                selections["optimize_metric"] = st.sidebar.selectbox(
                    "Metric to Optimize",
                    ("Sharpe Ratio", "Total Return", "Max Drawdown"),
                )

            benchmark_options = ["None"] + self.all_db_tickers
            default_benchmark_index = (
                benchmark_options.index("SPY") if "SPY" in benchmark_options else 0
            )
            selections["selected_benchmark"] = st.sidebar.selectbox(
                "Benchmark", options=benchmark_options, index=default_benchmark_index
            )

            if st.sidebar.button(
                "▶️ Run Backtest", use_container_width=True, type="primary"
            ):
                st.session_state["run_analysis_request"] = True

    def _render_statistical_test_configs(self, selections: dict):
        """Renders configuration for statistical tests."""
        with st.sidebar.container(border=True):
            test_options = [
                "Augmented Dickey-Fuller Test",
                "OLS Regression (Alpha/Beta)",
                "Engle-Granger Cointegration Test",
                "Johansen Cointegration Test",
                "Kalman Filter Smoother",
                "Principal Component Analysis (PCA)",
            ]
            selections["stat_test_type"] = st.sidebar.selectbox(
                "Test Type", options=test_options, key="stat_test_type"
            )

            if selections["stat_test_type"] == "OLS Regression (Alpha/Beta)":
                benchmark_options = ["None"] + self.all_db_tickers
                default_benchmark_index = (
                    benchmark_options.index("SPY") if "SPY" in benchmark_options else 0
                )
                selections["selected_benchmark"] = st.sidebar.selectbox(
                    "Benchmark",
                    options=benchmark_options,
                    index=default_benchmark_index,
                    help="Select a benchmark to regress against for Alpha/Beta calculation.",
                    key="selected_benchmark_stat_test",  # Use a unique key
                )

            if st.sidebar.button(
                "▶️ Run Statistical Test", use_container_width=True, type="primary"
            ):
                st.session_state["run_stat_test_request"] = True

    def _render_portfolio_management(self, selections: dict):
        """Renders controls to select, create, or delete a portfolio definition."""
        st.sidebar.subheader("Portfolios")
        portfolio_options = [""] + list(self.portfolios)
        selections["selected_portfolio_to_manage"] = st.sidebar.selectbox(
            "Select Portfolio to View/Edit",
            options=portfolio_options,
            help="Choose a portfolio to view and edit its trade history in the 'Portfolio' tab.",
        )

        with st.sidebar.form("create_portfolio_form", clear_on_submit=True):
            new_portfolio_name = st.text_input(
                "Or, Create New Portfolio", placeholder="e.g., Dividend Growth"
            )
            submitted = st.form_submit_button("Create Portfolio")
            if submitted and new_portfolio_name:
                st.session_state["create_portfolio_request"] = (
                    new_portfolio_name.strip()
                )

        if selections["selected_portfolio_to_manage"]:
            if st.sidebar.button(
                f"❌ Delete '{selections['selected_portfolio_to_manage']}'",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state["delete_portfolio_request"] = selections[
                    "selected_portfolio_to_manage"
                ]

    def _render_watchlist_management(self, selections: dict):
        """Renders a full CRUD interface for managing watchlists."""
        st.sidebar.subheader("Watchlists")

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
        st.sidebar.selectbox(
            "Select Watchlist to Edit",
            options=watchlist_options,
            key="watchlist_selector",
            on_change=on_watchlist_select,
        )

        with st.sidebar.form("watchlist_form", clear_on_submit=False):
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
                    st.sidebar.warning("Please provide a name and at least one ticker.")

        selected_to_delete = st.session_state.get(
            "watchlist_selector", "Create New Watchlist"
        )
        if selected_to_delete != "Create New Watchlist":
            if st.sidebar.button(
                f"❌ Delete '{selected_to_delete}'",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state["delete_watchlist_request"] = selected_to_delete

    def _render_data_management_panel(self, selections: dict):
        """Renders the admin panel for running the data pipeline and managing the universe."""
        st.sidebar.subheader("Data & Universe")

        st.sidebar.markdown("**Fetch Ticker Universes**")
        st.sidebar.info(
            "This updates the master list of tickers in the database.", icon="ℹ️"
        )

        col1, col2 = st.sidebar.columns(2)
        if col1.button("Fetch S&P 500", use_container_width=True):
            st.session_state["fetch_universe_request"] = "S&P 500"
        if col2.button("Fetch Nasdaq 100", use_container_width=True):
            st.session_state["fetch_universe_request"] = "Nasdaq 100"
        if col1.button("Fetch Dow Jones", use_container_width=True):
            st.session_state["fetch_universe_request"] = "Dow Jones"
        if col2.button("Fetch Top Crypto", use_container_width=True):
            st.session_state["fetch_universe_request"] = "Top Crypto"

        st.sidebar.divider()

        st.sidebar.markdown("**Manually Add Ticker**")
        with st.sidebar.form("sidebar_add_ticker_form", clear_on_submit=True):
            new_ticker = st.text_input("Ticker Symbol", placeholder="e.g., NVDA")
            asset_type = st.selectbox("Asset Type", ["Equity", "Crypto"])
            submitted = st.form_submit_button("Add Ticker & Run Pipeline")
            if submitted and new_ticker:
                st.session_state["add_ticker_request"] = {
                    "ticker": new_ticker.upper().strip(),
                    "asset_type": asset_type,
                }

        st.sidebar.divider()

        st.sidebar.markdown("**Ingest Price Data**")
        selections["full_backfill"] = st.sidebar.checkbox(
            "Full Backfill (slower)",
            value=False,
            help="If checked, re-downloads all historical data for all tickers in the database. If unchecked, only downloads recent data.",
        )
        if st.sidebar.button("Run Data Ingestion Pipeline", use_container_width=True):
            st.session_state["run_pipeline_request"] = True

    def _render_load_save_results(self, selections: dict):
        """Renders widgets for loading and saving backtest/optimization results."""
        st.sidebar.subheader("Load/Save Analysis")
        saved_files = self.results_manager.get_saved_files()
        selections["file_to_load"] = st.sidebar.selectbox(
            "Load Saved Analysis", options=[""] + saved_files
        )
        if st.sidebar.button("Load Selected", use_container_width=True):
            if selections["file_to_load"]:
                st.session_state["load_results_request"] = selections["file_to_load"]

        with st.sidebar.form("save_results_form", clear_on_submit=True):
            filename = st.text_input("Filename to Save As")
            submitted = st.form_submit_button("Save Current Analysis")
            if submitted and filename:
                st.session_state["save_results_request"] = filename
