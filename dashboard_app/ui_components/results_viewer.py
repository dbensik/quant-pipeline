import streamlit as st

from dashboard_app.ui_components.analysis_tab import AnalysisTab
from dashboard_app.ui_components.optimization_tab import OptimizationTab
from dashboard_app.ui_components.statistical_analysis_tab import StatisticalAnalysisTab
from dashboard_app.ui_components.stats_tab import StatsTab
from dashboard_app.ui_components.comparison_tab import ComparisonTab


class ResultsViewer:
    """
    A unified component to render the results area of the dashboard.
    It acts as a dispatcher, choosing the right visualization (Backtest, Optimization,
    or Statistical Test) based on the current state.
    """

    def __init__(self, db_manager):
        self.db_manager = db_manager
        # Initialize sub-components
        self.analysis_tab = AnalysisTab(self.db_manager)
        self.stats_tab = StatsTab()
        self.optimization_tab = OptimizationTab()
        self.statistical_tab = StatisticalAnalysisTab()
        self.comparison_tab = ComparisonTab()

    def render(
        self,
        selections: dict,
        backtest_run: dict,
        stat_test_results: dict,
        optimization_run: dict,
        portfolio_opt_run: dict,
    ):
        """
        Renders the appropriate result view.
        """
        # 1. Statistical Analysis View
        # Check if a statistical test was requested or run
        if st.session_state.get("run_stat_test_request") or st.session_state.get("stat_test_run"):
            self.statistical_tab.render(selections)
            return

        # 2. Optimization View
        # Check if either parameter optimization or portfolio optimization ran
        if optimization_run or portfolio_opt_run:
            # Note: OptimizationTab currently reads from 'optimization_run' in session state.
            # If we have portfolio_opt_run, we might need to unify them or ensure
            # the tab handles both. For now, we assume the controller sets 'optimization_run'
            # appropriately or the tab Logic covers it.
            self.optimization_tab.render(selections)
            return

        # 3. Strategy Comparison View
        if st.session_state.get("comparison_results"):
            self.comparison_tab.render()
            return

        # 4. Backtest Analysis View (Default)
        # If none of the above special modes are active, we show the standard backtest results.
        # We split this into tabs for better organization.
        tab1, tab2 = st.tabs(["📈 Analysis & Charts", "📊 Detailed Statistics"])

        with tab1:
            self.analysis_tab.render(selections)

        with tab2:
            self.stats_tab.render(selections)
