import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


class ComparisonTab:
    """
    Renders the Strategy Comparison tab, enabling side-by-side performance analysis
    of multiple strategies on the same asset.
    """

    def render(self):
        """Main render method for the comparison tab."""
        st.header("⚖️ comparison Results")
        comparison_results = st.session_state.get("comparison_results")

        if not comparison_results:
            st.info("Run a 'Strategy Comparison' from the sidebar to see results here.")
            return

        symbol = comparison_results.get("symbol")
        strategies_data = comparison_results.get("strategies", {})
        benchmarks = comparison_results.get("benchmarks", {})

        st.subheader(f"Comparison for: {symbol}")

        # --- 1. Combined Equity Curve ---
        self._render_combined_equity_curve(strategies_data, benchmarks)

        st.divider()

        # --- 2. Comparative Metrics Table ---
        self._render_metrics_table(strategies_data)

    def _render_combined_equity_curve(self, strategies_data: dict, benchmarks: dict):
        """Plots the cumulative returns of all strategies and benchmarks on one chart."""
        fig = go.Figure()

        # Add trace for each strategy
        for strategy_name, data in strategies_data.items():
            portfolio = data.get("portfolio")
            if portfolio is not None and not portfolio.empty:
                # Normalize to start at 0%
                cum_returns = (1 + portfolio["returns"]).cumprod() - 1
                fig.add_trace(
                    go.Scatter(
                        x=cum_returns.index,
                        y=cum_returns,
                        mode="lines",
                        name=strategy_name,
                        line=dict(width=2),
                    )
                )

        # Add trace for benchmarks (e.g., SPY)
        for bench_name, bench_df in benchmarks.items():
            if not bench_df.empty:
                # Calculate returns if not present (benchmarks usually have 'total')
                returns = bench_df["total"].pct_change().fillna(0)
                cum_returns = (1 + returns).cumprod() - 1
                
                fig.add_trace(
                    go.Scatter(
                        x=cum_returns.index,
                        y=cum_returns,
                        mode="lines",
                        name=bench_name,
                        line=dict(width=1.5, dash="dot", color="gray"),
                    )
                )

        fig.update_layout(
            title="Cumulative Returns Comparison",
            xaxis_title="Date",
            yaxis_title="Cumulative Return",
            yaxis_tickformat=".1%",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    def _render_metrics_table(self, strategies_data: dict):
        """Renders a comparative table of key performance metrics."""
        st.subheader("Performance Metrics Comparison")
        
        metrics_list = []
        for strategy_name, data in strategies_data.items():
            stats = data.get("stats", {})
            row = {"Strategy": strategy_name}
            # key mapping: nice name -> numeric value
            row.update(stats)
            metrics_list.append(row)
            
        if not metrics_list:
            st.warning("No metrics available for display.")
            return

        df = pd.DataFrame(metrics_list)
        
        # Select key columns to display
        cols_to_show = [
            "Strategy", 
            "Total Return", 
            "Sharpe Ratio", 
            "Max Drawdown",
            "mr_window",
            "mr_threshold",
            "mac_short_window",
            "mac_long_window"
        ]
        
        # Filter for columns that actually exist in the data
        final_cols = [c for c in cols_to_show if c in df.columns]
        
        # Formatting for display
        display_df = df[final_cols].copy()
        
        # Format percentages
        pct_cols = ["Total Return", "Annualized Return", "Annualized Volatility", "Max Drawdown"]
        for col in pct_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].map("{:.2%}".format)
                
        # Format ratios
        ratio_cols = ["Sharpe Ratio", "Sortino Ratio", "Calmar Ratio"]
        for col in ratio_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].map("{:.2f}".format)
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
