import numpy as np
import pandas as pd

class PortfolioOptimizer:
    """
    Performs portfolio optimization using Monte Carlo simulations to find
    allocations that maximize Sharpe Ratio or minimize volatility.
    """

    def __init__(
        self,
        price_data: pd.DataFrame,
        risk_free_rate: float = 0.02,
        seed: int = 42,
    ):
        """
        Args:
            price_data: DataFrame of asset prices (Close).
            risk_free_rate: Annualized risk-free rate for Sharpe calculation.
            seed: Seed for the weight-sampling RNG. This used to draw from the
                  global numpy RNG, so the same universe and date range
                  returned different "optimal" weights on every run and a
                  saved allocation could not be reproduced or audited. Pass
                  None for unseeded behaviour.
        """
        self.price_data = price_data
        self.risk_free_rate = risk_free_rate
        self.seed = seed
        # A dedicated Generator rather than np.random.seed(): seeding the
        # global RNG here would silently change the draws of anything else
        # sharing it in the same process, including the slippage handler.
        self._rng = np.random.default_rng(seed)
        # Calculate daily returns
        self.returns = self.price_data.pct_change().dropna()
        self.mean_returns = self.returns.mean()
        self.cov_matrix = self.returns.cov()
        self.num_assets = len(self.mean_returns)
        self.assets = self.price_data.columns.tolist()

    def simulate_random_portfolios(self, num_portfolios: int = 5000, callback=None):
        """
        Simulates random portfolio allocations to generate an efficient frontier.

        Args:
            num_portfolios: Number of random portfolios to simulate.
            callback: Optional callable to report progress (0.0 to 1.0).

        Returns:
            Tuple of:
            - results_df: DataFrame containing the simulation results.
            - max_sharpe_weights: Dictionary of weights for the portfolio with max Sharpe Ratio.
            - min_vol_weights: Dictionary of weights for the portfolio with min Volatility.
        """
        results_list = []
        all_weights = np.zeros((num_portfolios, self.num_assets))

        # Restart from the seed on every call, so two calls on the same
        # instance draw the same portfolios rather than continuing the stream.
        rng = np.random.default_rng(self.seed)

        for i in range(num_portfolios):
            # Generate random weights
            weights = rng.random(self.num_assets)
            weights /= np.sum(weights)
            all_weights[i, :] = weights

            # Calculate portfolio return and volatility
            # Annualize return (assuming 252 trading days)
            portfolio_return = np.sum(self.mean_returns * weights) * 252
            portfolio_std_dev = np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights))) * np.sqrt(252)

            # Calculate Sharpe Ratio
            if portfolio_std_dev == 0:
                sharpe_ratio = 0
            else:
                sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_std_dev

            results_list.append({
                "Annualized Return": portfolio_return,
                "Annualized Volatility": portfolio_std_dev,
                "Sharpe Ratio": sharpe_ratio,
                "weights": dict(zip(self.assets, weights))
            })

            # Report progress periodically
            if callback and i % 100 == 0:
                callback(i / num_portfolios)

        if callback:
            callback(1.0)

        results_df = pd.DataFrame(results_list)
        
        # Find optimal portfolios
        max_sharpe_idx = results_df["Sharpe Ratio"].idxmax()
        min_vol_idx = results_df["Annualized Volatility"].idxmin()

        max_sharpe_port = results_df.loc[max_sharpe_idx]
        min_vol_port = results_df.loc[min_vol_idx]

        return results_df, max_sharpe_port["weights"], min_vol_port["weights"]
