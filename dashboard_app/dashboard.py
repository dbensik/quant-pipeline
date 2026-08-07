import subprocess
import sys
from typing import Optional

import pandas as pd
import streamlit as st

# --- Project Imports ---
try:
    # Service and Manager Imports
    # Service and Manager Imports
    from dashboard_app.database_manager import DatabaseManager
    from dashboard_app.results_manager import ResultsManager
    from services.execution_service.portfolio_manager import PortfolioManager
    from dashboard_app.watchlist_manager import WatchlistManager
    from dashboard_app.price_data_handler import PriceDataHandler
    from dashboard_app.api_price_data_handler import ApiPriceDataHandler
    from dashboard_app.config_manager import ConfigManager
    from config.settings import QUANT_USE_API

    # UI Components
    from dashboard_app.ui_components.sidebar import Sidebar
    from dashboard_app.ui_components.results_viewer import ResultsViewer

    # Controllers
    from dashboard_app.controllers.analysis_controller import AnalysisController
    from dashboard_app.controllers.statistics_controller import StatisticsController
    from dashboard_app.controllers.optimization_controller import OptimizationController
    from dashboard_app.controllers.signals_controller import SignalsController

    # UI Components
    from dashboard_app.ui_components.sidebar import Sidebar
    from dashboard_app.ui_components.results_viewer import ResultsViewer
    from dashboard_app.ui_components.signals_viewer import SignalsViewer
    from dashboard_app.ui_components.paper_portfolio import PaperPortfolioViewer
    from dashboard_app.ui_components.asset_deep_dive_tab import AssetDeepDiveTab
    from dashboard_app.ui_components.dashboard_home import DashboardHome

    # Pipeline
    from data_pipeline.pipeline_orchestrator import PipelineOrchestrator
    
    # Models and Analysis (Still needed for imports check, though usage moved to controllers)
    from backtesting.backtester import Backtester  # unused here but keeps import check valid
    from analysis.statistical_analyzer import StatisticalAnalyzer

except ImportError as e:
    st.error(
        f"🚨 FAILED TO IMPORT A MODULE. Please ensure all project components are in place. Error: {e}"
    )
    st.stop()


# --- Main Application Class ---
class DashboardApp:
    """
    The main class that orchestrates the entire Streamlit application.
    It now delegates core business logic to specific controllers.
    """

    def __init__(self):
        """Initialize the application's state, managers, and controllers."""
        st.set_page_config(
            page_title="Quant Research Dashboard",
            page_icon="📈",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        # 1. Initialize Managers (Data Layer)
        self.db_manager = DatabaseManager()
        self.results_manager = ResultsManager()
        self.portfolio_manager = PortfolioManager()
        self.watchlist_manager = WatchlistManager()
        # Phase 3 migration seam: QUANT_USE_API=1 routes every price read through
        # the FastAPI service instead of SQLite. Both handlers expose the same
        # interface, so nothing downstream of this line changes. Default stays
        # SQLite until the API path is proven across all consumers; the Phase 3
        # exit gate is reached when this branch collapses to the API handler.
        if QUANT_USE_API:
            self.price_handler = ApiPriceDataHandler()
        else:
            self.price_handler = PriceDataHandler(self.db_manager.db_path)
        self.config_manager = ConfigManager()

        # 2. Initialize Controllers (Logic Layer)
        self.analysis_controller = AnalysisController(
            self.price_handler, self.portfolio_manager
        )
        self.statistics_controller = StatisticsController(self.price_handler)
        self.optimization_controller = OptimizationController(self.price_handler)
        self.signals_controller = SignalsController()

        # UI Components
        self.asset_deep_dive = AssetDeepDiveTab(self.db_manager)
        self.dashboard_home = DashboardHome(self.portfolio_manager, self.watchlist_manager)

        self._initialize_session_state()

        # 3. Initialize Shared Data
        if "all_tickers" not in st.session_state:
            st.session_state.all_tickers = self._get_cached_tickers()

    def _get_cached_tickers(self):
        """Caches the list of available tickers from the database."""
        return self.db_manager.get_universe_tickers()

    def _initialize_session_state(self):
        """A centralized place to initialize all session state keys."""
        defaults = {
            "selections": {},
            "backtest_run": None,
            "stat_test_results": None,
            "optimization_run": None,
            "portfolio_opt_run": None,
            "screened_tickers": None,
            "signal_result": None,
            # Request Flags
            "run_analysis_request": False,
            "run_stat_test_request": False,
            "apply_screener_request": False,
            "clear_screener_request": False,
            "save_results_request": None,
            "load_results_request": None,
            "create_portfolio_request": None,
            "delete_portfolio_request": None,
            "save_watchlist_request": None,
            "delete_watchlist_request": None,
            "fetch_universe_request": None,
            "add_ticker_request": None,
            "add_ticker_request": None,
            "run_pipeline_request": False,
            "comparison_results": None,
            "generate_signal_request": None,
        }
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def run(self):
        """The main execution loop for the application."""
        # --- UI: Sidebar ---
        sidebar = Sidebar(
            self.db_manager,
            self.results_manager,
            self.watchlist_manager,
            self.portfolio_manager,
            st.session_state.all_tickers,
        )
        user_selections = sidebar.render()
        
        # Persist selections to session state so they survive reruns
        if user_selections:
             st.session_state.selections.update(user_selections)

        # --- Logic: Handle Actions ---
        self._handle_actions()

        # --- UI: Main Content Area ---
        st.title("🚀 Quant Research Dashboard")

        # Render Signals if active
        if st.session_state.get("signal_result"):
             signals_viewer = SignalsViewer(controller=self.signals_controller)
             portfolio_viewer = PaperPortfolioViewer(controller=self.signals_controller)
             
             # Get symbol from session state or attempt to parse from payload
             symbol = st.session_state.get("signal_ticker")
             
             # Layout: Signal Card on top, Portfolio below
             signals_viewer.render(st.session_state["signal_result"], symbol=symbol)
             
             portfolio_viewer.render()
             
             st.divider()

        # Check for Chart Selection (Deep Dive)
        # Render if no specific result view is active, effectively serving as the "Charts" tab view
        chart_ticker = st.session_state.selections.get("selected_symbols_chart")
        # Ensure chart_ticker is a string (it might be a list from multiselect if the component was reused incorrectly, but _render_ticker_selection default is single?)
        # Actually _render_ticker_selection adapts based on context. Sidebar defaults to Single Ticker for Charts?
        # Let's assume user flow in sidebar: Analysis tab -> Single Ticker.
        
        has_active_result = (
            st.session_state.get("signal_result") or 
            st.session_state.get("backtest_run") or 
            st.session_state.get("optimization_run") or 
            st.session_state.get("stat_test_results") or
            st.session_state.get("screened_tickers") or
            st.session_state.get("comparison_results")
        )

        if chart_ticker and not has_active_result:
             # Handle case where it might be a list (though Deep Dive expects str)
             if isinstance(chart_ticker, list):
                 if chart_ticker:
                     chart_ticker = chart_ticker[0]
                 else:
                     chart_ticker = None
             
             if chart_ticker:
                 self.asset_deep_dive.render(chart_ticker)

        # 2. Results View (Backtest, Stats, Opt)
        if has_active_result:
            results_viewer = ResultsViewer(db_manager=self.db_manager)
            results_viewer.render(
                st.session_state.get("selections", {}),
                st.session_state.get("backtest_run"),
                st.session_state.get("stat_test_results"),
                st.session_state.get("optimization_run"),
                st.session_state.get("portfolio_opt_run"),
            )
        else:
            # 3. Default Dashboard Home Logic (News + Portfolio)
            self.dashboard_home.render(st.session_state.selections)

    def _handle_actions(self):
        """Controller method to dispatch actions based on st.session_state flags."""
        
        # 1. Run Analysis (Backtest)
        if st.session_state.get("run_analysis_request"):
            with st.spinner("Running Analysis..."):
                self._run_backtest_or_optimization()
            st.session_state["run_analysis_request"] = False
            st.session_state["signal_result"] = None # Clear signal view

        # 2. Run Statistical Tests
        if st.session_state.get("run_stat_test_request"):
            if st.session_state.selections:
                with st.spinner("Running Statistical Test..."):
                    self.statistics_controller.run_statistical_test(st.session_state.selections)
            st.session_state["run_stat_test_request"] = False
            st.session_state["signal_result"] = None

        # 3. Apply Screener
        if st.session_state.get("apply_screener_request"):
            self._run_screener()
            st.session_state["apply_screener_request"] = False
        
        if st.session_state.get("clear_screener_request"):
            st.session_state.screened_tickers = None
            st.session_state["clear_screener_request"] = False

        # 4. Save/Load Results
        if st.session_state.get("save_results_request"):
            self._save_results(st.session_state["save_results_request"])
            st.session_state["save_results_request"] = None

        if st.session_state.get("load_results_request"):
            self._load_results(st.session_state["load_results_request"])
            st.session_state["load_results_request"] = None

        # 5. Portfolio Management
        if st.session_state.get("create_portfolio_request"):
            name = st.session_state["create_portfolio_request"]
            self.portfolio_manager.create_portfolio(name)
            st.success(f"Created portfolio: {name}")
            st.session_state["create_portfolio_request"] = None
            st.rerun()

        if st.session_state.get("delete_portfolio_request"):
            name = st.session_state["delete_portfolio_request"]
            self.portfolio_manager.delete_portfolio(name)
            st.success(f"Deleted portfolio: {name}")
            st.session_state["delete_portfolio_request"] = None
            st.rerun()

        # 6. Watchlist Management
        if st.session_state.get("save_watchlist_request"):
            req = st.session_state["save_watchlist_request"]
            self.watchlist_manager.save_watchlist(req["name"], req["tickers"])
            st.success(f"Saved watchlist: {req['name']}")
            st.session_state["save_watchlist_request"] = None
            st.rerun()

        if st.session_state.get("delete_watchlist_request"):
            name = st.session_state["delete_watchlist_request"]
            self.watchlist_manager.delete_watchlist(name)
            st.success(f"Deleted watchlist: {name}")
            st.session_state["delete_watchlist_request"] = None
            st.rerun()

        # 7. Data Pipeline & Admin
        if st.session_state.get("add_ticker_request"):
            req = st.session_state["add_ticker_request"]
            self.db_manager.add_ticker(req["ticker"], req["asset_type"])
            st.success(f"Added {req['ticker']}")
            st.session_state["add_ticker_request"] = None
            
        if st.session_state.get("run_pipeline_request"):
             cmd = ["/opt/anaconda3/envs/quant-pipeline-env/bin/python", "cli/run_pipeline.py"]
             if st.session_state.selections.get("full_backfill"):
                 cmd.append("--full-backfill")
             self._run_external_command(cmd, "Data Pipeline Completed!")
             st.session_state["run_pipeline_request"] = False

        # 8. Signal Generation
        if st.session_state.get("generate_signal_request"):
             # Reset state immediately to avoid showing stale data during fetch
             st.session_state["signal_result"] = None
             st.session_state["signal_ticker"] = None
             
             ticker = st.session_state["generate_signal_request"]
             
             with st.spinner(f"Processing request for {ticker}..."):
                 result = self.signals_controller.fetch_signal(ticker)
             
             if result:
                 st.toast(f"✅ Received signal for {ticker}")
                 st.session_state["signal_result"] = result
                 st.session_state["signal_ticker"] = ticker  # Store ticker for Viewer context
                 st.session_state["generate_signal_request"] = None
                 # Clean up other views to focus on signal
                 st.session_state["backtest_run"] = None
                 st.session_state["optimization_run"] = None
                 # No rerun needed; fall-through will render the new result
             else:
                 st.error(f"❌ Failed to fetch signal for {ticker}. See logs for details.")
                 st.session_state["generate_signal_request"] = None


        # 9. Trade Execution
        if st.session_state.get("execute_trade_request"):
            req = st.session_state["execute_trade_request"]
            try:
                msg = self.portfolio_manager.execute_trade(
                    req["symbol"], req["action"], req["quantity"], req["price"],
                    portfolio_name=req.get("portfolio_name")
                )
                st.toast(f"✅ {msg}")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Trade Failed: {e}")
            st.session_state["execute_trade_request"] = None

    def _run_backtest_or_optimization(self):
        """Routes the analysis request to the appropriate controller."""
        selections = st.session_state.selections
        
        # Normalize selections for Backtest/Screener Mode (resolve suffixed keys)
        q_mode = selections.get("quant_mode")
        if q_mode in ["Backtest", "Screener"]:
             suffix = "backtest" if q_mode == "Backtest" else "screener"
        
             selections["start_date"] = selections.get(f"start_date_{suffix}", selections.get("start_date"))
             selections["end_date"] = selections.get(f"end_date_{suffix}", selections.get("end_date"))
             
             # Default dates if missing (e.g. Screener mode)
             if not selections["start_date"]:
                 selections["start_date"] = pd.to_datetime("2020-01-01")
             if not selections["end_date"]:
                 selections["end_date"] = pd.to_datetime("today")
                 
             # Map source
             selections["source_type"] = selections.get(f"source_type_{suffix}", selections.get("source_type"))
             selections["source_name"] = selections.get(f"source_name_{suffix}", selections.get("source_name"))
             
             # Primary: Lab selection (depends on mode)
             # Fallback: Chart selection
             lab_syms = selections.get(f"selected_symbols_{suffix}")
             chart_syms = selections.get("selected_symbols_chart")
             
             if lab_syms:
                 selections["selected_symbols"] = lab_syms
             elif chart_syms:
                 # chart_syms might be list or str
                 if isinstance(chart_syms, str):
                     selections["selected_symbols"] = [chart_syms]
                 else:
                     selections["selected_symbols"] = chart_syms
                 st.toast(f"Using selected tickers from Charts tab: {selections['selected_symbols']}")
             else:
                 selections["selected_symbols"] = []

             if not selections["selected_symbols"]:
                 st.warning(f"DEBUG: No symbols found. Lab Selection: {selections.get('selected_symbols_backtest')}, Chart Selection: {selections.get('selected_symbols_chart')}")
                 
        if selections.get("selected_benchmark") == "-- Select a Benchmark --":
             selections["selected_benchmark"] = None # sanitize input
        
        # 1. Parameter Optimization
        if selections.get("optimization_mode") and selections.get("backtest_mode") == "Individual Ticker":
             self.optimization_controller.run_parameter_optimization(selections)
             return

        # 2. Portfolio Optimization (Monte Carlo) - Special Case logic from Sidebar?
        # Typically handled by a specific tool selection. Assuming logic based on analysis type inferred.
        # But looking at sidebar, it seems Portfolio Optimization is not a specific mode yet, 
        # let's assume if user selects specific settings (not fully implemented in Sidebar V1 yet but controller is ready).
        
        st.session_state["run_analysis_request"] = False  # Reset flag

        # 3. Standard Backtests
        if selections["backtest_mode"] == "Individual Ticker":
            # st.info(f"DEBUG: Running Backtest for: {selections.get('selected_symbols')}")
            self.analysis_controller.run_individual_backtest(selections)
        elif selections["backtest_mode"] == "Portfolio":
            self.analysis_controller.run_portfolio_backtest(selections)

        # 4. Strategy Comparison
        if selections.get("analysis_type") == "Strategy Comparison":
            self.analysis_controller.run_strategy_comparison(selections)

    def _run_screener(self):
        """Runs the screening pipeline using data fetched by the PriceDataHandler."""
        selections = st.session_state.selections
        screener_objects = selections.get("screener_objects", [])
        if not screener_objects:
            st.warning("No screeners selected.")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")

        tickers_to_screen = []
        
        # Normalize selections for Screener Mode
        if selections.get("quant_mode") == "Screener":
             source_type = selections.get("source_type_screener", selections.get("source_type"))
             source_name = selections.get("source_name_screener", selections.get("source_name"))
        else:
             source_type = selections.get("source_type")
             source_name = selections.get("source_name")

        # Resolve Ticker Universe for Screening
        if source_type == "asset_type":
             tickers_to_screen = self.db_manager.get_tickers_by_asset_type(source_name)
        elif source_type == "watchlist":
             tickers_to_screen = self.watchlist_manager.load().get(source_name, [])
        elif source_type == "portfolio":
             # Screening a portfolio is valid
             port = self.portfolio_manager.get_all_portfolios().get(source_name, {})
             tickers_to_screen = port.get("constituents", [])
        
        if not tickers_to_screen:
            st.warning("No tickers found in the selected source to screen.")
            return

        with st.spinner(
            f"Screening {len(tickers_to_screen)} tickers from {start_date} to {end_date}..."
        ):
            # Fetch data efficiently
            price_data = self.price_handler.get_full_data_for_tickers(
                tickers_to_screen, start_date, end_date
            )
            
            passed_tickers = []
            for ticker in tickers_to_screen:
                data = price_data.get(ticker)
                if data is None or data.empty:
                    continue
                
                # A ticker must pass ALL selected screeners
                all_passed = True
                for screener in screener_objects:
                    if not screener.run(data):
                        all_passed = False
                        break
                
                if all_passed:
                    passed_tickers.append(ticker)

            st.session_state.screened_tickers = passed_tickers
            if not passed_tickers:
                st.warning("No tickers passed the screening criteria.")
            else:
                st.success(f"{len(passed_tickers)} tickers passed!")

    def _save_results(self, filename: str):
        """Handles saving results via ResultsManager."""
        # Determine what to save based on what's active
        data_to_save = None
        if st.session_state.backtest_run:
            data_to_save = st.session_state.backtest_run
        elif st.session_state.optimization_run:
            data_to_save = st.session_state.optimization_run
        
        if data_to_save:
            success = self.results_manager.save(filename, data_to_save)
            if success:
                st.success(f"Saved analysis to {filename}.pkl")
            else:
                st.error("Failed to save analysis.")
        else:
            st.warning("No results to save.")

    def _load_results(self, request_data: dict):
        """
        Handles loading saved results from either local FS or Remote API.
        
        Args:
            request_data: dict with "filename" and "source" keys. 
                          (Backwards compat: if str, treats as local filename)
        """
        filename = request_data if isinstance(request_data, str) else request_data["filename"]
        source = request_data.get("source", "local") if isinstance(request_data, dict) else "local"

        data = None
        if source == "api":
            from dashboard_app.api_client import ApiClient
            client = ApiClient() # Could also pass this from sidebar via session state if config needed
            with st.spinner(f"Fetching '{filename}' from API..."):
                data = client.get_result_data(filename)
        else:
            data = self.results_manager.load(filename)

        if data:
            if "benchmarks" in data: # Heuristic to identify backtest results
                 st.session_state.backtest_run = data
                 st.session_state.optimization_run = None
            else:
                 st.session_state.optimization_run = data
                 st.session_state.backtest_run = None
            st.success(f"Loaded {filename} from {source}")
        else:
            st.error(f"Could not load {filename} from {source}")

    def _run_external_command(self, cmd: list, success_message: str):
        """Helper to run external scripts/subprocesses."""
        with st.spinner("Running external process..."):
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, check=True
                )
                st.success(success_message)
                if result.stdout:
                    with st.expander("Process Output"):
                        st.code(result.stdout)
            except subprocess.CalledProcessError as e:
                st.error(f"Process failed: {e}")
                if e.stderr:
                    with st.expander("Error Details"):
                        st.code(e.stderr)

    def _save_johansen_portfolio(self):
         """Legacy logic for Johansen specific save, can be integrated into controller later."""
         # See AnalysisController logic for usage (CointegratedMeanReversionStrategy)
         # Logic for *saving* the weights usually happens in the UI after a test run.
         # For now, this is removed or can be re-added if specific button action exists.
         pass


if __name__ == "__main__":
    app = DashboardApp()
    app.run()
