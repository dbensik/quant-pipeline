import pandas as pd
import plotly.graph_objects as go
import streamlit as st


class StatsTab:
    """
    Renders the Statistics tab, displaying detailed performance metrics,
    risk analysis, distributions, and event logs from a backtest run.
    """

    def render(self, selections: dict):
        """Main render method for the tab."""
        st.header("📊 Performance & Risk Statistics")
        backtest_run = st.session_state.get("backtest_run")

        if not backtest_run:
            st.info("Run a backtest from the sidebar to see detailed statistics here.")
            return

        results = backtest_run.get("results")
        if not results:
            st.warning("Backtest ran, but no results were generated.")
            return

        # Create a select box to switch between different results in the run
        result_options = list(results.keys())
        selected_symbol = st.selectbox("View Statistics For:", result_options)

        if selected_symbol:
            result_data = results[selected_symbol]
            self._render_stats_details(selected_symbol, result_data)

    def _render_stats_details(self, symbol: str, result: dict):
        """Renders all the statistical components for a single backtest result."""
        stats = result.get("stats")
        portfolio = result.get("portfolio")
        trade_log = result.get("trade_log")
        risk_metrics = result.get("risk_metrics")
        diagnostics = result.get("diagnostics")  # For advanced strategies

        if not stats or portfolio is None:
            st.error(f"Incomplete data for {symbol}. Cannot display stats.")
            return

        st.subheader(f"Metrics for: {symbol}")

        # --- REFACTOR: Combine performance and risk metrics for a cleaner layout ---
        self._render_key_metrics(stats, risk_metrics)

        st.divider()

        # --- NEW: Conditionally render strategy diagnostics if available ---
        if diagnostics is not None and not diagnostics.empty:
            self._render_strategy_diagnostics(diagnostics)
            st.divider()

        col1, col2 = st.columns(2)
        with col1:
            self._render_returns_histogram(portfolio)
        with col2:
            self._render_drawdown_series(portfolio)

        st.divider()
        if trade_log is not None and not trade_log.empty:
            self._render_portfolio_event_log(trade_log)
        else:
            st.info("No trades were executed in this backtest.")

    def _render_key_metrics(self, stats: dict, risk_metrics: dict):
        """Displays the main performance and risk metrics in columns."""
        cols = st.columns(5)
        cols[0].metric("Total Return", f"{stats.get('Total Return', 0):.2%}")
        cols[1].metric("Sharpe Ratio", f"{stats.get('Sharpe Ratio', 0):.2f}")
        cols[2].metric("Max Drawdown", f"{stats.get('Max Drawdown', 0):.2%}")

        # --- NEW: Display the risk metrics that were previously ignored ---
        if risk_metrics:
            cols[3].metric(
                "Value at Risk (95%)",
                f"{risk_metrics.get('Value at Risk (95%)', 0):.2%}",
            )
            cols[4].metric(
                "Conditional VaR (95%)",
                f"{risk_metrics.get('Conditional VaR (95%)', 0):.2%}",
            )

    def _render_strategy_diagnostics(self, diagnostics: pd.DataFrame):
        """
        Displays a chart of a strategy's internal state, like a Z-score,
        to help diagnose and tune its parameters.
        """
        st.markdown("##### Strategy Diagnostics")

        # --- REFACTOR: Moved user guide from docstring into the UI ---
        with st.expander("How to Use This Chart to Improve Your Strategy"):
            st.info(
                """
                This chart shows the calculated Z-score of the portfolio spread. Trades are triggered when the Z-score crosses the threshold lines.

                - **No Trades?** If you see no trade markers (triangles), the spread never deviated enough to trigger a trade. Try lowering the 'Mean Reversion Threshold' in the sidebar.
                - **Too Many Losing Trades?** If the Z-score is very jagged and crosses the thresholds frequently, the strategy is "whipsawing". Try increasing the 'Mean Reversion Window' in the sidebar to smooth the Z-score.
                """
            )

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=diagnostics.index,
                y=diagnostics["z_score"],
                mode="lines",
                name="Z-Score",
                line=dict(color="royalblue"),
            )
        )

        # Infer the threshold from the signal data for accurate plotting
        entry_signals = diagnostics[
            diagnostics["signal"] != diagnostics["signal"].shift(1)
        ]
        entry_signals = entry_signals[entry_signals["signal"] != 0]
        threshold = (
            entry_signals["z_score"].abs().min() if not entry_signals.empty else 2.0
        )

        fig.add_hline(
            y=threshold,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Short Threshold ({threshold:.2f})",
        )
        fig.add_hline(
            y=-threshold,
            line_dash="dash",
            line_color="green",
            annotation_text=f"Long Threshold (-{threshold:.2f})",
        )

        long_trades = entry_signals[entry_signals["signal"] == 1]
        short_trades = entry_signals[entry_signals["signal"] == -1]

        fig.add_trace(
            go.Scatter(
                x=long_trades.index,
                y=long_trades["z_score"],
                mode="markers",
                name="Long Entry",
                marker=dict(color="green", size=10, symbol="triangle-up"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=short_trades.index,
                y=short_trades["z_score"],
                mode="markers",
                name="Short Entry",
                marker=dict(color="red", size=10, symbol="triangle-down"),
            )
        )

        fig.update_layout(
            title_text="Z-Score of Portfolio Spread with Trade Entries",
            xaxis_title="Date",
            yaxis_title="Z-Score (Standard Deviations)",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    def _render_returns_histogram(self, portfolio: pd.DataFrame):
        """Displays a histogram of daily returns."""
        st.markdown("##### Daily Returns Distribution")
        returns = portfolio["total"].pct_change().dropna()
        fig = go.Figure(
            data=[go.Histogram(x=returns, nbinsx=50, marker_color="#636EFA")]
        )
        fig.update_layout(
            title_text="Frequency of Daily Returns",
            xaxis_title_text="Daily Return",
            yaxis_title_text="Frequency",
            bargap=0.1,
        )
        st.plotly_chart(fig, use_container_width=True)

    def _render_drawdown_series(self, portfolio: pd.DataFrame):
        """Displays a plot of the portfolio's drawdown over time."""
        st.markdown("##### Portfolio Drawdown")
        returns = portfolio["total"].pct_change().dropna()
        cumulative_returns = (1 + returns).cumprod()
        peak = cumulative_returns.expanding(min_periods=1).max()
        drawdown = (cumulative_returns / peak) - 1

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=drawdown.index,
                y=drawdown,
                fill="tozeroy",
                mode="lines",
                line=dict(color="firebrick", width=1.5),
                name="Drawdown",
            )
        )
        fig.update_layout(
            title_text="Drawdown Over Time",
            xaxis_title_text="Date",
            yaxis_title_text="Drawdown",
            yaxis_tickformat=".1%",
        )
        st.plotly_chart(fig, use_container_width=True)

    def _render_portfolio_event_log(self, trade_log: pd.DataFrame):
        """Renders the trade log."""
        st.markdown("##### Trade Log")
        st.info("This log shows every trade executed during the backtest.")
        display_df = trade_log.copy()
        display_df["date"] = pd.to_datetime(display_df["date"]).dt.strftime("%Y-%m-%d")
        display_df["quantity"] = display_df["quantity"].map("{:,.2f}".format)
        display_df["price"] = display_df["price"].map("${:,.2f}".format)
        display_df["cost"] = display_df["cost"].map("${:,.2f}".format)
        st.dataframe(
            display_df[["date", "symbol", "action", "quantity", "price", "cost"]],
            use_container_width=True,
            hide_index=True,
        )
