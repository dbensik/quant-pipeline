import pandas as pd
import streamlit as st

from analysis.principal_component_analyzer import PrincipalComponentAnalyzer
from analysis.statistical_analyzer import StatisticalAnalyzer
from dashboard_app.price_data_handler import PriceDataHandler


class StatisticsController:
    """
    Handles logic for running statistical analysis and tests.
    """

    def __init__(self, price_handler: PriceDataHandler):
        self.price_handler = price_handler

    def get_test_data(
        self,
        selections: dict,
        num_assets: int = None,
        require_benchmark: bool = False,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Helper to fetch and validate data for statistical tests."""
        selected_symbols = selections.get("selected_symbols", [])

        if num_assets and len(selected_symbols) != num_assets:
            if num_assets == 2:
                st.warning("Please select exactly two tickers for this test.")
            else:
                st.warning(f"Please select exactly {num_assets} tickers.")
            return None, None

        if not selected_symbols:
            st.warning("Please select at least one ticker.")
            return None, None

        benchmark_symbol = selections.get("selected_benchmark")
        if require_benchmark:
            if not benchmark_symbol or benchmark_symbol == "None":
                st.warning("Please select a benchmark ticker for this test.")
                return None, None

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")

        with st.spinner("Fetching data..."):
            all_symbols = list(selected_symbols)
            if benchmark_symbol and benchmark_symbol != "None":
                all_symbols.append(benchmark_symbol)

            price_df_dict = self.price_handler.get_prices(
                all_symbols, start_date, end_date
            )
            if price_df_dict is None or price_df_dict.empty:
                st.error("No data found for the selected tickers.")
                return None, None

            df = pd.DataFrame(price_df_dict).dropna()
            
            # FALLBACK: If benchmark requested but missing, try yfinance
            if benchmark_symbol and benchmark_symbol != "None" and benchmark_symbol not in df.columns:
                try:
                    import yfinance as yf
                    ticker = yf.Ticker(benchmark_symbol)
                    hist = ticker.history(start=start_date, end=end_date)
                    if not hist.empty:
                        # Align dates by reindexing
                        bench_series = hist["Close"]
                        curr_index = df.index
                        # Reindex benchmark to match existing data range (fill fwd/bwd or drop)
                        bench_series = bench_series.reindex(curr_index, method='ffill')
                        df[benchmark_symbol] = bench_series
                except Exception as e:
                    # Only warn if strictly required, otherwise silent fallback to None
                    if require_benchmark:
                        st.warning(f"Could not fetch data for benchmark {benchmark_symbol}: {e}")

            if df.empty:
                st.error("Dataset is empty after dropping missing values.")
                return None, None

            asset_prices = df[selected_symbols]
            
            benchmark_prices = None
            if benchmark_symbol and benchmark_symbol in df.columns:
                benchmark_prices = df[benchmark_symbol]
            elif require_benchmark:
                 st.error(f"Benchmark {benchmark_symbol} data not available.")
                 return None, None

            return asset_prices, benchmark_prices

    def run_statistical_test(self, selections: dict):
        """Runs the selected statistical test."""
        test_type = selections.get("stat_test_type")
        if test_type == "Augmented Dickey-Fuller Test":
            self.run_adf_test(selections)
        elif test_type == "OLS Regression (Alpha/Beta)":
            self.run_ols_regression(selections)
        elif test_type == "Engle-Granger Cointegration Test":
            self.run_engle_granger_test(selections)
        elif test_type == "Johansen Cointegration Test":
            self.run_johansen_test(selections)
        elif test_type == "Kalman Filter Smoother":
            self.run_kalman_filter(selections)
        elif test_type == "Principal Component Analysis (PCA)":
            self.run_pca(selections)
        elif test_type == "Monte Carlo Simulation":
            self.run_monte_carlo(selections)
        elif test_type == "Cluster Analysis (K-Means)":
            self.run_cluster_analysis(selections)

    def run_adf_test(self, selections: dict):
        """Runs the Augmented Dickey-Fuller test for selected assets."""
        prices, _ = self.get_test_data(selections)
        if prices is None:
            return
        results = {}
        # Instantiate analyzer (it's an instance method based on file view)
        analyzer = StatisticalAnalyzer() 
        for col in prices.columns:
            results[col] = analyzer.run_adf_test(prices[col])
        st.session_state.stat_test_run = {
            "test_type": "Augmented Dickey-Fuller Test",
            "results": results,
            "description": "Tests for stationarity. p-value < 0.05 indicates stationarity.",
        }

    def run_ols_regression(self, selections: dict):
        """Runs OLS regression for selected assets against a benchmark."""
        prices, benchmark = self.get_test_data(
            selections, require_benchmark=True
        )
        if prices is None:
            return
        results = {}
        analyzer = StatisticalAnalyzer()
        for col in prices.columns:
            results[col] = analyzer.run_ols_regression(
                prices[col], benchmark
            )
        st.session_state.stat_test_run = {
            "test_type": "OLS Regression (Alpha/Beta)",
            "results": results,
            "benchmark": selections.get('selected_benchmark'),
            "description": f"Regression against {selections.get('selected_benchmark')}.",
        }

    def run_engle_granger_test(self, selections: dict):
        """Runs the Engle-Granger cointegration test."""
        prices, _ = self.get_test_data(selections, num_assets=2)
        if prices is None:
            return
        analyzer = StatisticalAnalyzer()
        
        # The analyzer method returns a full dict result, not tuple
        res = analyzer.run_engle_granger_test(
            prices.iloc[:, 0], prices.iloc[:, 1]
        )
        
        st.session_state.stat_test_run = {
            "test_type": "Engle-Granger Cointegration Test",
            "results": {
                 f"{prices.columns[0]} vs {prices.columns[1]}": res
            },
            "description": f"Cointegration test between {prices.columns[0]} and {prices.columns[1]}.",
        }

    def run_johansen_test(self, selections: dict):
        """Runs the Johansen cointegration test."""
        prices, _ = self.get_test_data(selections)
        if prices is None:
            return
        if len(prices.columns) > 12:
            st.warning("Johansen test is computationally intensive for >12 assets.")
        
        analyzer = StatisticalAnalyzer()
        res = analyzer.run_johansen_test(prices)
        
        st.session_state.stat_test_run = {
            "test_type": "Johansen Cointegration Test",
            "results": {"Selected Portfolio": res},
            "description": "Multivariate cointegration test.",
            "raw_prices": prices,  # Needed for saving cointegrated portfolios
        }

    def run_kalman_filter(self, selections: dict):
        """Runs the Kalman Filter smoother on selected assets."""
        prices, _ = self.get_test_data(selections)
        if prices is None:
            return
        results = {}
        analyzer = StatisticalAnalyzer()
        for col in prices.columns:
            df_res = analyzer.run_kalman_filter_smoother(prices[col])
            results[col] = df_res
            
        st.session_state.stat_test_run = {
            "test_type": "Kalman Filter Smoother",
            "results": results,
            "description": "Noise reduction using Kalman Filter.",
        }

    def run_pca(self, selections: dict):
        """Runs Principal Component Analysis on the selected assets' returns."""
        prices, _ = self.get_test_data(selections)
        if prices is None:
            return
        returns = prices.pct_change().dropna()
        if returns.empty:
            st.error("Not enough data to calculate returns for PCA.")
            return

        pca_analyzer = PrincipalComponentAnalyzer(n_components=None)
        pca_analyzer.fit(returns)
        explained_variance = pca_analyzer.get_explained_variance_ratio()
        components_df = pca_analyzer.get_components(feature_names=returns.columns)
        
        st.session_state.stat_test_run = {
            "test_type": "Principal Component Analysis (PCA)",
            "results": {
                "explained_variance_ratio": explained_variance,
                "cumulative_explained_variance": pca_analyzer.explained_variance_ratio_.cumsum(),
                "components": components_df,
                "eigenvalues": pca_analyzer.explained_variance_
            },
            "description": "Principal Component Analysis of asset returns.",
        }

    def run_monte_carlo(self, selections: dict):
        """
        Runs Monte Carlo Simulation using Geometric Brownian Motion (GBM).
        Simulates future portfolio value paths.
        """
        # For simplicity, we simulate the *Portfolio* value if multiple assets are selected,
        # assuming an equal-weight portfolio or just summing their values. 
        # Better: run MC on a single "Portfolio" timeseries constructed from selections.
        
        prices, _ = self.get_test_data(selections)
        if prices is None:
            return
            
        # Create a synthetic portfolio "Close" series (Equal Weighted)
        # Normalize start to 100k
        norm_prices = prices / prices.iloc[0]
        portfolio_series = norm_prices.mean(axis=1) * 100000 
        
        # Calculate drift and volatility
        returns = portfolio_series.pct_change().dropna()
        mu = returns.mean()
        sigma = returns.std()
        
        n_simulations = selections.get("mc_simulations", 1000)
        time_horizon = selections.get("mc_horizon", 252)
        
        # Simulation
        last_price = portfolio_series.iloc[-1]
        simulation_df = pd.DataFrame()
        
        # Vectorized simulation using numpy
        import numpy as np
        
        # shape: (time_horizon, n_simulations)
        # Random shocks
        Z = np.random.normal(0, 1, (time_horizon, n_simulations))
        
        # GBM Formula: S_t = S_{t-1} * exp((mu - 0.5 * sigma^2) + sigma * Z)
        # We can calculate cumulative returns
        daily_returns = np.exp((mu - 0.5 * sigma**2) + sigma * Z)
        
        # Cumulative product
        price_paths = np.vstack([np.ones((1, n_simulations)) * last_price, np.zeros((time_horizon, n_simulations))])
        
        # Iterative update or cumprod (Cumprod is faster)
        # price_path[t] = price_path[t-1] * daily_returns[t]
        # Equivalently: price_path = last_price * cumprod(daily_returns)
        price_paths[1:] = last_price * np.cumprod(daily_returns, axis=0)
        
        simulation_df = pd.DataFrame(price_paths)
        
        # Calculate stats on final outcomes
        final_values = price_paths[-1, :]
        var_95 = np.percentile(final_values, 5)
        mean_final = np.mean(final_values)
        median_final = np.median(final_values)
        
        st.session_state.stat_test_run = {
            "test_type": "Monte Carlo Simulation",
            "results": {
                "paths": simulation_df, # Be careful with size if n is huge
                "stats": {
                    "VaR (95%)": var_95,
                    "Mean Ending Value": mean_final,
                    "Median Ending Value": median_final,
                    "Start Value": last_price,
                    "Horizon": time_horizon
                }
            },
            "description": f"Monte Carlo Simulation ({n_simulations} runs, {time_horizon} days)."
        }

    def run_cluster_analysis(self, selections: dict):
        """
        Runs K-Means clustering on the correlation matrix of selected assets.
        Also runs PCA for 2D visualization of the clusters.
        """
        prices, _ = self.get_test_data(selections)
        if prices is None:
            return
        
        if len(prices.columns) < 3:
            st.error("Cluster Analysis requires at least 3 assets to be meaningful.")
            return

        returns = prices.pct_change().dropna()
        
        # Feature extraction: We cluster based on Correlation structure
        # Alternatively, we can scale returns and cluster on return patterns.
        # Transpose correlation matrix so we cluster 'Assets' not 'Dates' (though corr is mxm)
        # Using Correlation Matrix itself as features for each asset
        corr_matrix = returns.corr()
        
        k = selections.get("cluster_k", 4)
        if k >= len(prices.columns):
             st.warning(f"Number of clusters ({k}) cannot be >= number of assets ({len(prices.columns)}). Reducing k.")
             k = len(prices.columns) - 1
        
        from sklearn.cluster import KMeans
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(corr_matrix)
        
        cluster_map = pd.DataFrame({
            "Ticker": corr_matrix.index,
            "Cluster": clusters
        })
        
        # PCA for visualization (2 components)
        from sklearn.decomposition import PCA
        pca = PCA(n_components=2)
        coords = pca.fit_transform(corr_matrix)
        
        cluster_map["PC1"] = coords[:, 0]
        cluster_map["PC2"] = coords[:, 1]
        
        st.session_state.stat_test_run = {
            "test_type": "Cluster Analysis (K-Means)",
            "results": {
                "clusters": cluster_map,
                "corr_matrix": corr_matrix
            },
            "description": f"K-Means Clustering (k={k}) based on asset correlations."
        }
