from typing import Dict

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


class AnalysisTab:
    """
    Renders the Analysis tab, which displays backtest results including
    performance charts, metrics, and comparisons against multiple benchmarks.
    """

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def render(self, selections: dict):
        """Main render method for the tab."""
        st.header("🔎 Analysis & Backtesting")

        if st.session_state.get("backtest_run"):
            self._render_backtest_results(selections)
        elif st.session_state.get("screener_analysis_df") is not None:
            self._render_screener_results()
        else:
            st.info(
                "Run a backtest, optimization, or screener from the sidebar to see results here."
            )

    def _render_screener_results(self):
        """Displays the results from the screener pipeline."""
        st.subheader("Screener Results")
        analysis_df = st.session_state.get("screener_analysis_df")
        if analysis_df is not None and not analysis_df.empty:
            st.dataframe(analysis_df, use_container_width=True)
        else:
            st.success(
                "The screener ran successfully and filtered out all tickers based on your criteria."
            )

    def _render_backtest_results(self, selections: dict):
        """Orchestrates the display of all backtest result components."""
        backtest_run = st.session_state.get("backtest_run", {})
        all_results = backtest_run.get("results", {})
        all_benchmarks = backtest_run.get("benchmarks", {})

        if not all_results:
            st.warning("Backtest ran, but no results were generated.")
            return

        result_options = list(all_results.keys())
        if len(result_options) > 1:
            selected_symbol = st.selectbox("View Results For:", result_options)
        else:
            selected_symbol = result_options[0]

        if selected_symbol:
            result_data = all_results[selected_symbol]
            self._render_individual_result(selected_symbol, result_data, all_benchmarks)

    def _render_individual_result(self, symbol: str, result: dict, benchmarks: dict):
        """Displays the chart and metrics for a single backtest result."""
        st.subheader(f"Results for: {symbol}")
        portfolio = result.get("portfolio")

        if portfolio is None or portfolio.empty:
            st.error(f"No portfolio data available for {symbol}.")
            return

        self._render_comparative_chart(portfolio, benchmarks)

    def _render_comparative_chart(
        self, portfolio: pd.DataFrame, benchmarks: Dict[str, pd.DataFrame]
    ):
        """
        Renders a performance chart comparing the strategy against multiple benchmarks.
        """
        st.info(
            """
            The chart below normalizes the strategy and all benchmarks to a starting value of 100
            for a fair visual comparison of performance over time.
            """
        )
        fig = go.Figure()

        # 1. Add Strategy Trace
        if "total" in portfolio.columns:
            normalized_portfolio = (
                portfolio["total"] / portfolio["total"].iloc[0]
            ) * 100
            fig.add_trace(
                go.Scatter(
                    x=normalized_portfolio.index,
                    y=normalized_portfolio,
                    mode="lines",
                    name="Strategy",
                    line=dict(color="firebrick", width=3),
                )
            )

        # 2. Add Benchmark Traces
        benchmark_colors = [
            "#636EFA",
            "#00CC96",
            "#FECB52",
            "#FFA15A",
        ]  # Blue, Green, Yellow, Orange
        for i, (name, b_df) in enumerate(benchmarks.items()):
            if b_df is None or b_df.empty:
                continue

            # Determine the correct price column ('total' for backtested B&H, 'Close' for raw SPY)
            price_series = None
            if "total" in b_df.columns:
                price_series = b_df["total"]
            elif "Close" in b_df.columns:
                price_series = b_df["Close"]
            else:
                continue

            # Align benchmark to portfolio start date for fair comparison
            aligned_series = price_series.loc[portfolio.index[0] :]
            if aligned_series.empty:
                continue

            normalized_benchmark = (aligned_series / aligned_series.iloc[0]) * 100
            fig.add_trace(
                go.Scatter(
                    x=normalized_benchmark.index,
                    y=normalized_benchmark,
                    mode="lines",
                    name=name,
                    line=dict(
                        color=benchmark_colors[i % len(benchmark_colors)],
                        width=1.5,
                        dash="dash",
                    ),
                )
            )

        fig.update_layout(
            title_text="Strategy vs. Benchmark Performance",
            xaxis_title="Date",
            yaxis_title="Normalized Performance (Starts at 100)",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            margin=dict(l=40, r=40, t=80, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)
