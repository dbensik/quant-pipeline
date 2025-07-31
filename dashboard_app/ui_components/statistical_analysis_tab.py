import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


class StatisticalAnalysisTab:
    """
    Renders the UI for the Statistical Analysis tab, displaying results from
    various statistical tests run from the dashboard controller.
    """

    def render(self, selections: dict):
        """Main render method for the tab."""
        st.header("🔬 Statistical Analysis")

        test_run_data = st.session_state.get("stat_test_run")
        if not test_run_data:
            st.info(
                "📈 Select tickers and a statistical test from the sidebar, then click '▶️ Run Statistical Test' to see the results here."
            )
            return

        results = test_run_data.get("results")
        benchmark = test_run_data.get("benchmark")
        test_type = test_run_data.get(
            "test_type"
        )  # Uses the type from the last RUN test

        if not results:
            st.warning("No results were generated for the selected test.")
            return

        # --- FEATURE: Dispatch to the correct rendering method based on test type ---
        render_map = {
            "Principal Component Analysis (PCA)": self._render_pca_results,
            "Augmented Dickey-Fuller Test": self._render_adf_results,
            "OLS Regression (Alpha/Beta)": self._render_ols_results,
            "Engle-Granger Cointegration Test": self._render_engle_granger_results,
            "Johansen Cointegration Test": self._render_johansen_results,
            "Kalman Filter Smoother": self._render_kalman_filter_results,
        }

        render_function = render_map.get(test_type)

        if render_function:
            try:
                # Pass benchmark only if the function expects it (e.g., OLS)
                if "benchmark" in render_function.__code__.co_varnames:
                    render_function(results, benchmark=benchmark)
                else:
                    render_function(results)
            except Exception as e:
                st.error(f"Failed to render results for {test_type}. Error: {e}")
                st.json(results)
        else:
            st.warning(f"Result rendering for '{test_type}' is not yet implemented.")
            st.json(results)

    def _render_pca_results(self, results: dict):
        """Displays the results of a Principal Component Analysis."""
        st.subheader("Principal Component Analysis (PCA) Results")
        st.markdown(
            """
            PCA identifies the primary drivers of variance in a set of returns.
            The first few components often represent market-wide movements,
            while later components can represent sector-specific or idiosyncratic risk.
            """
        )

        explained_variance = results["explained_variance_ratio"]
        cumulative_variance = results["cumulative_explained_variance"]
        components = results["components"]
        eigenvalues = results["eigenvalues"]

        # --- Explained Variance Plot (Scree Plot) ---
        st.markdown("##### Explained Variance")
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=components.index,
                y=explained_variance,
                name="Explained Variance",
                marker_color="cornflowerblue",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=components.index,
                y=cumulative_variance,
                name="Cumulative Variance",
                line=dict(color="red", dash="dot"),
            ),
            secondary_y=True,
        )
        fig.update_layout(
            title_text="Explained Variance by Principal Component",
            yaxis_tickformat=".1%",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        fig.update_yaxes(title_text="Explained Variance", secondary_y=False)
        fig.update_yaxes(title_text="Cumulative Explained Variance", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

        # --- Component Loadings Table and Heatmap ---
        st.markdown("##### Principal Component Loadings (Eigenvectors)")
        st.write(
            "The table below shows the 'loadings' or weights of each asset on the principal components. "
            "A high absolute value indicates a strong influence."
        )

        col1, col2 = st.columns([1, 2])
        with col1:
            st.dataframe(
                components.style.background_gradient(cmap="vlag", axis=1).format(
                    "{:.3f}"
                ),
                use_container_width=True,
                height=min(35 * (len(components) + 1) + 3, 600),
            )
        with col2:
            heatmap_fig = px.imshow(
                components,
                text_auto=".2f",
                aspect="auto",
                color_continuous_scale="vlag",
                title="Component Loadings Heatmap",
            )
            st.plotly_chart(heatmap_fig, use_container_width=True)

        # --- Eigenvalues ---
        with st.expander("View Eigenvalues"):
            st.write(
                "Eigenvalues correspond to the magnitude (variance) captured by each component. "
                "The 'elbow' in the scree plot is often used to determine the number of significant components."
            )
            eigen_df = pd.DataFrame(
                eigenvalues, index=components.index, columns=["Eigenvalue"]
            )
            st.dataframe(eigen_df.style.format("{:.4f}"), use_container_width=True)

    def _render_adf_results(self, results: dict):
        """Displays the results of an Augmented Dickey-Fuller test."""
        st.subheader("Augmented Dickey-Fuller Test Results")
        st.markdown(
            "The ADF test determines if a time series is stationary (i.e., its statistical properties don't change over time). "
            "A **p-value below 0.05** typically suggests that we can reject the null hypothesis (that the series is non-stationary)."
        )
        summary_data = []
        for symbol, res in results.items():
            summary_data.append(
                {
                    "Ticker": symbol,
                    "Test Statistic": res["Test Statistic"],
                    "p-value": res["p-value"],
                    "Stationary (p < 0.05)": res["p-value"] < 0.05,
                }
            )
        summary_df = pd.DataFrame(summary_data).set_index("Ticker")
        st.dataframe(
            summary_df.style.apply(
                lambda row: [
                    "background-color: #d4edda" if row["Stationary (p < 0.05)"] else ""
                ]
                * len(row),
                axis=1,
            ).format({"Test Statistic": "{:.3f}", "p-value": "{:.3f}"}),
            use_container_width=True,
        )
        with st.expander("Show Detailed Critical Values"):
            st.json({k: v["Critical Values"] for k, v in results.items()})

    def _render_ols_results(self, results: dict, benchmark: str):
        """Displays the results of an OLS Regression."""
        st.subheader(f"OLS Regression Results (vs. {benchmark or 'N/A'})")
        st.markdown(
            """
            This table shows an Ordinary Least Squares (OLS) regression for each asset against the benchmark.
            - **Alpha (α):** The asset's excess return when the benchmark's return is zero.
            - **Beta (β):** The asset's sensitivity to the benchmark. A beta > 1 means more volatile than the benchmark.
            - **R-squared:** The proportion of the asset's variance predictable from the benchmark.
            """
        )
        summary_df = pd.DataFrame.from_dict(results, orient="index").drop(
            columns=["summary"]
        )
        summary_df.index.name = "Ticker"
        st.dataframe(
            summary_df.style.format(
                {"alpha": "{:.4f}", "beta": "{:.3f}", "r_squared": "{:.2%}"}
            ),
            use_container_width=True,
        )
        with st.expander("Show Full Model Summaries"):
            for symbol, res in results.items():
                st.text(f"--- {symbol} vs {benchmark} ---")
                st.text(res["summary"])

    def _render_engle_granger_results(self, results: dict):
        """Displays the results of an Engle-Granger Cointegration test."""
        st.subheader("Engle-Granger Cointegration Test Results")
        st.markdown(
            "This test determines if two time series are cointegrated (have a stationary, long-run relationship). "
            "A **p-value below 0.05** suggests the pair is cointegrated."
        )
        for pair, res in results.items():
            st.markdown(f"#### Pair: `{pair}`")
            p_value = res["p-value"]
            is_cointegrated = p_value < 0.05
            status = "✅ Cointegrated" if is_cointegrated else "❌ Not Cointegrated"
            st.metric(label="p-value", value=f"{p_value:.4f}", help=status)
            with st.expander("View Full Test Statistics"):
                st.json({k: v for k, v in res.items() if k != "summary"})
                st.text(res["summary"])

    def _render_johansen_results(self, results: dict):
        """Displays the results of a Johansen Cointegration test."""
        st.subheader("Johansen Cointegration Test Results")
        st.markdown(
            "This test checks for cointegration relationships among several time series."
        )
        for group, res in results.items():
            st.markdown(f"#### {group}")
            st.text(str(res))  # The statsmodels result object has a good __str__ method

            # --- FEATURE: Allow saving the cointegrated portfolio ---
            # --- FIX: Add guards to prevent errors if results are missing keys ---
            eigenvectors = res.get("eigenvectors")
            tickers = res.get("names")

            if (
                eigenvectors is not None
                and tickers is not None
                and eigenvectors.ndim == 2
            ):
                st.markdown("##### Discovered Cointegrated Portfolio")
                st.info(
                    "The first eigenvector can be used as weights for a mean-reverting portfolio."
                )

                weights = eigenvectors[:, 0]
                portfolio_df = pd.DataFrame({"Ticker": tickers, "Weight": weights})
                st.dataframe(portfolio_df, use_container_width=True)

                portfolio_name = st.text_input(
                    "Save portfolio as:", f"coint_{'_'.join(tickers)}"
                )
                if st.button("💾 Save Cointegrated Portfolio"):
                    st.session_state["save_johansen_portfolio_request"] = {
                        "name": portfolio_name,
                        "tickers": tickers,
                        "weights": dict(zip(tickers, weights)),
                    }
                    st.rerun()

    def _render_kalman_filter_results(self, results: dict):
        """Displays the results of a Kalman Filter Smoother."""
        st.subheader("Kalman Filter Smoother Results")
        st.markdown(
            "The Kalman Filter provides a smoothed estimate of a time series, reducing noise."
        )
        for symbol, df in results.items():
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(x=df.index, y=df.iloc[:, 0], mode="lines", name="Original")
            )
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df.iloc[:, 1],
                    mode="lines",
                    name="Smoothed",
                    line=dict(color="red", dash="dash"),
                )
            )
            fig.update_layout(
                title=f"Original vs. Kalman Smoothed Series for {symbol}",
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
            )
            st.plotly_chart(fig, use_container_width=True)
