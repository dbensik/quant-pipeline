import pandas as pd
import plotly.express as px
import streamlit as st


class OptimizationTab:
    """
    Renders the Optimization tab, displaying results from parameter sweeps
    or portfolio weight optimizations.
    """

    def render(self, selections: dict):
        """Main render method for the tab."""
        st.header("⚙️ Optimization Results")
        opt_run = st.session_state.get("optimization_run")

        if not opt_run:
            st.info(
                "Run a parameter or portfolio optimization from the sidebar to see results here."
            )
            return

        if "status" in opt_run:
            st.info(f"✨ {opt_run['status']}")
            return

        results_df = opt_run.get("results")
        if results_df is None or results_df.empty:
            st.warning("Optimization ran, but no valid results were generated.")
            return

        metric = opt_run.get("metric")
        strategy = opt_run.get("strategy")
        symbol = opt_run.get("symbol")

        st.subheader(f"Optimization for: {symbol} ({strategy})")

        # --- NEW: Handle Portfolio Optimization Results ---
        if strategy == "Buy & Hold Weight Optimization":
            self._render_portfolio_optimization_results(results_df, metric)
        else:  # Handle single-asset parameter optimization
            self._render_parameter_optimization_results(results_df, metric)

    def _render_parameter_optimization_results(
        self, results_df: pd.DataFrame, metric: str
    ):
        """Displays results for a single-asset parameter sweep."""
        st.metric(f"Best Metric ({metric})", f"{results_df[metric].max():.2f}")
        st.dataframe(
            results_df.sort_values(by=metric, ascending=False), use_container_width=True
        )

        param_cols = [
            col
            for col in results_df.columns
            if col
            not in [
                "Total Return",
                "Annualized Return",
                "Annualized Volatility",
                "Sharpe Ratio",
                "Max Drawdown",
                "weights",
            ]
        ]
        if len(param_cols) >= 2:
            st.subheader("Performance vs. Parameters")
            fig = px.scatter(
                results_df,
                x=param_cols[0],
                y=param_cols[1],
                color=metric,
                size=metric,
                hover_data=results_df.columns,
                color_continuous_scale=px.colors.sequential.Viridis,
                title=f"{metric} by {param_cols[0]} and {param_cols[1]}",
            )
            st.plotly_chart(fig, use_container_width=True)

    def _render_portfolio_optimization_results(
        self, results_df: pd.DataFrame, metric: str
    ):
        """
        Displays results for a portfolio weight optimization, including an
        efficient frontier plot and optimal weight allocation.
        """
        if metric not in results_df.columns:
            st.error(
                f"The selected metric '{metric}' was not found in the optimization results."
            )
            return

        # --- 1. Find and Display the Best Portfolio ---
        st.subheader("Optimal Portfolio")
        best_portfolio = results_df.loc[results_df[metric].idxmax()]

        cols = st.columns(3)
        cols[0].metric("Optimal " + metric, f"{best_portfolio[metric]:.2f}")
        cols[1].metric(
            "Annualized Return", f"{best_portfolio['Annualized Return']:.2%}"
        )
        cols[2].metric(
            "Annualized Volatility", f"{best_portfolio['Annualized Volatility']:.2%}"
        )

        # --- 2. Display Optimal Weights in a Pie Chart ---
        st.subheader("Optimal Asset Allocation")
        optimal_weights = best_portfolio["weights"]
        weights_df = pd.DataFrame(
            list(optimal_weights.items()), columns=["Ticker", "Weight"]
        )

        fig_pie = px.pie(
            weights_df,
            values="Weight",
            names="Ticker",
            title="Optimal Portfolio Weights",
            hole=0.3,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)

        # --- 3. Display the Efficient Frontier Plot ---
        st.subheader("Efficient Frontier Simulation")
        fig_scatter = px.scatter(
            results_df,
            x="Annualized Volatility",
            y="Annualized Return",
            color=metric,
            hover_data=["weights"],
            title="Monte Carlo Simulation Results (Efficient Frontier)",
            labels={
                "Annualized Volatility": "Risk (Annualized Volatility)",
                "Annualized Return": "Return (Annualized Return)",
            },
        )
        # Highlight the best portfolio
        fig_scatter.add_scatter(
            x=[best_portfolio["Annualized Volatility"]],
            y=[best_portfolio["Annualized Return"]],
            mode="markers",
            marker=dict(color="red", size=15, symbol="star"),
            name="Optimal Portfolio",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # --- 4. Show the full data for exploration ---
        with st.expander("Show All Simulation Trials"):
            st.dataframe(
                results_df.sort_values(by=metric, ascending=False),
                use_container_width=True,
            )
