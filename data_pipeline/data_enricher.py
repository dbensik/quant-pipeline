import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class DataEnricher:
    """
    A class dedicated to enriching raw time-series data with calculated
    financial metrics like volatility, beta, Sharpe ratio, and RSI.
    It operates on a per-ticker basis to ensure calculations are correct.
    """

    def __init__(
        self,
        benchmark_returns: pd.Series = None,
        volatility_window: int = 90,
        rsi_window: int = 14,
        sharpe_window: int = 90,
        beta_window: int = 90,
        risk_free_rate: float = 0.02,
        granularity: str = "1d",
    ):
        """
        Initializes the DataEnricher with configurable parameters.

        Args:
            benchmark_returns: A Series of benchmark returns (e.g., from SPY),
                               indexed by date. Required for beta calculation.
            volatility_window: The lookback window for calculating annualized volatility.
            rsi_window: The lookback window for the Relative Strength Index (RSI).
            sharpe_window: The lookback window for the Sharpe Ratio.
            beta_window: The lookback window for calculating beta.
            risk_free_rate: The annualized risk-free rate for the Sharpe Ratio.
        """
        self.benchmark_returns = benchmark_returns
        self.volatility_window = volatility_window
        self.rsi_window = rsi_window
        self.sharpe_window = sharpe_window
        self.beta_window = beta_window
        self.risk_free_rate = risk_free_rate
        self.daily_risk_free_rate = (1 + self.risk_free_rate) ** (1 / 252) - 1
        self._set_annualization_factor(granularity)

    def _set_annualization_factor(self, granularity: str):
        """Sets the correct annualization factor based on data granularity."""
        if "d" in granularity.lower():
            self.annualization_factor = 252
        elif "h" in granularity.lower():
            # Assuming 6.5 trading hours in a US market day
            self.annualization_factor = 252 * 6.5
        elif "m" in granularity.lower():
            self.annualization_factor = 252 * 6.5 * 60
        else:
            self.annualization_factor = 252  # Default to daily

    def _calculate_rsi(self, series: pd.Series) -> pd.Series:
        """
        Calculates the Relative Strength Index (RSI) for a given price series.

        Args:
            series: A pandas Series of prices (e.g., 'Close' prices).

        Returns:
            A pandas Series containing the calculated RSI values.
        """
        delta = series.diff()
        gain = delta.where(delta > 0, 0).rolling(window=self.rsi_window).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=self.rsi_window).mean()

        # Calculate Relative Strength (RS)
        # Add a small epsilon to the denominator to avoid division by zero
        relative_strength = gain / (loss + 1e-9)

        # Calculate RSI
        rsi = 100.0 - (100.0 / (1.0 + relative_strength))
        return rsi

    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes a DataFrame of raw price data and returns it with new
        columns for calculated metrics.

        Args:
            df: The input DataFrame, must have 'Ticker' and 'Close' columns
               and be indexed by 'Date'.

        Returns:
            pd.DataFrame: The enriched DataFrame.
        """
        if df.empty:
            logger.warning("Input DataFrame to DataEnricher is empty. Returning as is.")
            return df

        # Group by ticker to apply calculations per-asset
        enriched_dfs = []
        for ticker, group in df.groupby("Ticker"):
            # Ensure data is a copy and sorted by date to prevent warnings and ensure correct calculations
            group = group.copy().sort_index()

            # Use fill_method=None to adopt future pandas behavior and avoid warnings.
            group["daily_return"] = group["Close"].pct_change(fill_method=None)

            # --- Volatility (annualized) ---
            group[f"volatility_{self.volatility_window}d"] = group[
                "daily_return"
            ].rolling(window=self.volatility_window).std() * np.sqrt(
                self.annualization_factor
            )  # Use the dynamic factor

            # --- RSI ---
            group[f"rsi_{self.rsi_window}d"] = self._calculate_rsi(group["Close"])

            # --- Sharpe Ratio (annualized) ---
            excess_returns = group["daily_return"] - self.daily_risk_free_rate
            mean_excess_return = excess_returns.rolling(
                window=self.sharpe_window
            ).mean()
            # Annualize the numerator (mean excess return)
            sharpe_numerator = mean_excess_return * self.annualization_factor
            # The denominator (volatility) is already annualized
            sharpe_denominator = group[f"volatility_{self.volatility_window}d"]
            group[f"sharpe_ratio_{self.sharpe_window}d"] = (
                sharpe_numerator / sharpe_denominator
            )

            # --- Beta (requires benchmark data) ---
            if self.benchmark_returns is not None:
                # --- REFACTOR: Robust beta calculation ---
                # The previous method could cause index alignment issues. This is safer.
                if not group["daily_return"].dropna().empty:
                    benchmark = self.benchmark_returns.rename("benchmark_return")
                    # Use a left join to align benchmark returns to the asset's dates.
                    # This is safer than concat as it preserves the group's index.
                    merged_df = group.join(benchmark)

                    rolling_cov = merged_df["daily_return"].rolling(window=self.beta_window).cov(merged_df["benchmark_return"])
                    rolling_var = merged_df["benchmark_return"].rolling(window=self.beta_window).var()

                    # Pandas automatically aligns the indexes during this assignment.
                    group["beta"] = rolling_cov / rolling_var
            else:
                group["beta"] = None

            # --- FIX: Re-add the Ticker column, which is lost during groupby ---
            group["Ticker"] = ticker
            enriched_dfs.append(group)

        if not enriched_dfs:
            return pd.DataFrame()

        final_df = pd.concat(enriched_dfs)

        # --- FIX: Sort the final DataFrame to ensure a predictable order ---
        # The concatenation of ticker groups can result in a non-monotonic
        # index. Sorting by the DatetimeIndex ensures data is written to the
        # database in a clean, chronological order.
        final_df.sort_index(inplace=True)

        # Clean up intermediate column
        final_df = final_df.drop(columns=["daily_return"])
        # Replace infinite values (from division by zero) and NaNs with None for DB compatibility
        final_df = final_df.replace([np.inf, -np.inf, np.nan], None)

        return final_df
