import numpy as np
import pandas as pd
import statsmodels.api as sm
from pykalman import KalmanFilter
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen


class StatisticalAnalyzer:
    """
    A service class to perform various statistical tests on time-series data.
    """

    def run_adf_test(self, price_series: pd.Series) -> dict:
        """
        Performs the Augmented Dickey-Fuller test to check for stationarity.

        Returns:
            A dictionary containing the test results and an interpretation.
        """
        if price_series.empty:
            return {"error": "Input series is empty."}

        # The ADF test is performed on returns, which are more likely to be stationary
        returns = price_series.pct_change().dropna()
        if len(returns) < 20:  # Need a minimum number of observations
            return {"error": "Not enough data points to perform ADF test."}

        adf_result = adfuller(returns)

        p_value = adf_result[1]
        is_stationary = p_value < 0.05
        interpretation = (
            f"The p-value is {p_value:.4f}. "
            f"The series is likely {'stationary' if is_stationary else 'non-stationary'}."
        )

        return {
            "Test Statistic": adf_result[0],
            "p-value": p_value,
            "Lags Used": adf_result[2],
            "Number of Observations": adf_result[3],
            "Critical Values": adf_result[4],
            "is_stationary": is_stationary,
            "interpretation": interpretation,
        }

    def run_ols_regression(
        self, asset_returns: pd.Series, benchmark_returns: pd.Series
    ) -> dict:
        """
        Performs an Ordinary Least Squares (OLS) regression to find Alpha and Beta.
        """
        data = pd.concat([asset_returns, benchmark_returns], axis=1)

        if not data.index.is_unique:
            data = data[~data.index.duplicated(keep="first")]

        # --- BEST PRACTICE: Avoid inplace=True ---
        data = data.dropna()
        data.columns = ["asset", "benchmark"]

        if len(data) < 30:
            return {
                "error": "Not enough overlapping data points to perform regression."
            }

        benchmark_with_const = sm.add_constant(data["benchmark"])
        model = sm.OLS(data["asset"], benchmark_with_const).fit()

        alpha = model.params.get("const", 0) * 252
        beta = model.params.get("benchmark", 0)
        r_squared = model.rsquared

        return {
            "Alpha (Annualized)": alpha,
            "Beta": beta,
            "R-squared": r_squared,
            "summary": str(model.summary()),
        }

    def run_engle_granger_test(self, series1: pd.Series, series2: pd.Series) -> dict:
        """
        Performs the Engle-Granger two-step cointegration test.
        """
        data = pd.concat([series1, series2], axis=1, keys=[series1.name, series2.name])

        if not data.index.is_unique:
            data = data[~data.index.duplicated(keep="first")]

        # --- BEST PRACTICE: Avoid inplace=True ---
        data = data.dropna()

        if len(data) < 30:
            return {
                "error": "Not enough overlapping data points for cointegration test after cleaning."
            }

        cleaned_series1 = data[series1.name]
        cleaned_series2 = data[series2.name]

        coint_t_statistic, p_value, crit_values = coint(
            cleaned_series1, cleaned_series2
        )
        is_cointegrated = p_value < 0.05

        series2_with_const = sm.add_constant(cleaned_series2)
        model = sm.OLS(cleaned_series1, series2_with_const).fit()
        hedge_ratio = model.params.get(series2.name, 0)

        return {
            "test_name": "Engle-Granger",
            "p_value": p_value,
            "test_statistic": coint_t_statistic,
            "critical_values": {
                "1%": crit_values[0],
                "5%": crit_values[1],
                "10%": crit_values[2],
            },
            "hedge_ratio": hedge_ratio,
            "is_cointegrated": is_cointegrated,
            "interpretation": f"The series are {'likely cointegrated' if is_cointegrated else 'not cointegrated'} with a p-value of {p_value:.4f}.",
        }

    def run_johansen_test(
        self, data: pd.DataFrame, det_order: int = 0, k_ar_diff: int = 1
    ) -> dict:
        """
        Performs the Johansen test for cointegration for multiple assets.

        This enhanced version includes data sanitization, robust error handling,
        and a clear interpretation of the test results.
        """
        # 1. Data Validation and Sanitization
        if data.empty or data.shape[1] < 2:
            return {"error": "Input data must have at least two columns."}

        # Defensive check for duplicate indices
        if not data.index.is_unique:
            data = data[~data.index.duplicated(keep="first")]

        # This creates a new, clean DataFrame and avoids any potential
        # issues with modifying slices of data.
        data = data.dropna()

        if len(data) < 50:  # Check length after all cleaning
            return {"error": "Not enough data points for Johansen test after cleaning."}

        try:
            # 2. Run the Test on the fully sanitized DataFrame
            result = coint_johansen(data, det_order, k_ar_diff)

            # ... (rest of the function is unchanged) ...
            # 3. Format Trace Statistics into a readable DataFrame
            trace_stats = pd.DataFrame(
                data=result.cvt,  # Critical values
                index=[f"r <= {i}" for i in range(data.shape[1])],
                columns=["90% Crit Value", "95% Crit Value", "99% Crit Value"],
            )
            trace_stats["Trace Statistic"] = result.lr1
            # Reorder columns for better readability
            trace_stats = trace_stats[
                [
                    "Trace Statistic",
                    "90% Crit Value",
                    "95% Crit Value",
                    "99% Crit Value",
                ]
            ]

            # 4. Interpret the Results to find the number of cointegrating relationships
            num_cointegrating_relations = 0
            for i in range(data.shape[1]):
                # Compare trace statistic to the 95% critical value
                if result.lr1[i] > result.cvt[i, 1]:
                    num_cointegrating_relations = i + 1
                else:
                    break  # Stop when the stat is no longer significant

            interpretation = (
                f"The test found {num_cointegrating_relations} cointegrating relationship(s) "
                f"at the 95% significance level."
            )

            # The first column of the eigenvector matrix corresponds to the most significant relationship
            primary_cointegrating_vector = result.evec[:, 0]

            # 5. Structure the Output Dictionary
            return {
                "test_name": "Johansen",
                "trace_statistics": trace_stats,
                "eigenvectors": result.evec,
                "primary_cointegrating_vector": primary_cointegrating_vector,
                "num_cointegrating_relations": num_cointegrating_relations,
                "tickers": data.columns.tolist(),
                "interpretation": interpretation,
            }

        except np.linalg.LinAlgError as e:
            return {
                "error": f"A linear algebra error occurred, possibly due to highly correlated assets. Details: {e}"
            }
        except Exception as e:
            return {
                "error": f"An unexpected error occurred during the Johansen test: {e}"
            }

    def run_kalman_filter_smoother(self, series: pd.Series) -> pd.DataFrame:
        """
        Applies a Kalman filter to smooth a time series.

        Args:
            series: The time series to smooth.

        Returns:
            A DataFrame containing the original and smoothed series.
        """
        if not series.index.is_unique:
            series = series[~series.index.duplicated(keep="first")]
        series = series.dropna()
        if series.empty:
            return pd.DataFrame({"original": [], "smoothed": []})

        kf = KalmanFilter(
            initial_state_mean=0,
            n_dim_obs=1,
            transition_matrices=1,
            observation_matrices=1,
            transition_covariance=0.01,
            observation_covariance=1,
        )
        # Use the EM algorithm to find the best parameters for the filter
        kf = kf.em(series.values, n_iter=5)
        (smoothed_state_means, _) = kf.smooth(series.values)

        return pd.DataFrame(
            {"original": series, "smoothed": smoothed_state_means.flatten()},
            index=series.index,
        )
