import numpy as np
import pandas as pd

from analysis.performance_analyzer import cash_account_sharpe, periods_per_year

#: Weightings scored per matrix product: bounds memory (bars x chunk floats)
#: and sets how often progress is reported.
CHUNK = 250


class PortfolioOptimizer:
    """
    Performs portfolio optimization using Monte Carlo simulations to find
    allocations that maximize Sharpe Ratio or minimize volatility.

    Each weighting is scored as a daily-rebalanced, cost-free backtest of it
    would be: return is the CAGR, volatility the annualized std of daily
    returns, and Sharpe the cash-account Sharpe every backtest reports
    (analysis.performance_analyzer.cash_account_sharpe). Until 2026-10-12 it
    used mean daily return x 252 and subtracted the window's mean rate, so the
    same basket showed a different Sharpe here than in a backtest
    (SPY/AGG/GLD 2023-2025: 1.82 here, 1.94 there) and a "return" that was not
    compounded (26.3% for a basket that grew 29.0% a year).
    """

    def __init__(
        self,
        price_data: pd.DataFrame,
        risk_free_accrual: np.ndarray,
        seed: int = 42,
    ):
        """
        Args:
            price_data: DataFrame of asset prices (Close), one column per
                  asset, no missing values.
            risk_free_accrual: Interest one unit of cash earns on each bar,
                  aligned to price_data (api/risk_free.py builds it from ^IRX;
                  0 on bar 0), exactly as the backtesters take it. Required:
                  a 0.02 default once stood in for every window.
            seed: Seed for the weight-sampling RNG. This used to draw from the
                  global numpy RNG, so the same universe and date range
                  returned different "optimal" weights on every run and a
                  saved allocation could not be reproduced or audited. Pass
                  None for unseeded behaviour.
        """
        self.price_data = price_data
        self.risk_free_accrual = np.asarray(risk_free_accrual, dtype=float)
        if len(self.risk_free_accrual) != len(price_data):
            raise ValueError(
                f"risk_free_accrual has {len(self.risk_free_accrual)} values for "
                f"{len(price_data)} bars"
            )
        self.seed = seed
        # Bar 0 has a return of 0, as in a backtest's history: the analyzer
        # takes its volatility over the same series, bar 0 included.
        self.returns = self.price_data.pct_change().fillna(0.0)
        self.num_assets = self.returns.shape[1]
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
        # Restart from the seed on every call, so two calls on the same
        # instance draw the same portfolios rather than continuing the stream.
        # One (n, assets) draw is the same stream as n draws of one row.
        rng = np.random.default_rng(self.seed)
        all_weights = rng.random((num_portfolios, self.num_assets))
        all_weights /= all_weights.sum(axis=1, keepdims=True)

        returns = self.returns.to_numpy()
        accrual = self.risk_free_accrual
        index = self.price_data.index
        days = (index[-1] - index[0]).days
        # The same calendar rule as every backtest (decision 38).
        annualize = np.sqrt(periods_per_year(index))
        cash_growth = float(np.prod(1.0 + accrual))

        annual_return = np.empty(num_portfolios)
        volatility = np.empty(num_portfolios)
        sharpe = np.empty(num_portfolios)
        for lo in range(0, num_portfolios, CHUNK):
            if callback:
                callback(lo / num_portfolios)
            hi = min(lo + CHUNK, num_portfolios)
            # bars x chunk: each column a daily-rebalanced portfolio's returns.
            portfolio_returns = returns @ all_weights[lo:hi].T
            growth = np.prod(1.0 + portfolio_returns, axis=0)
            annual_return[lo:hi] = growth ** (365.0 / days) - 1 if days > 0 else 0.0
            volatility[lo:hi] = portfolio_returns.std(axis=0, ddof=1) * annualize
            excess_volatility = (portfolio_returns - accrual[:, None]).std(axis=0, ddof=1)
            sharpe[lo:hi] = cash_account_sharpe(
                growth, cash_growth, days, excess_volatility * annualize
            )

        if callback:
            callback(1.0)

        results_df = pd.DataFrame(
            {
                "Annualized Return": annual_return,
                "Annualized Volatility": volatility,
                "Sharpe Ratio": sharpe,
                "weights": [dict(zip(self.assets, w, strict=True)) for w in all_weights],
            }
        )

        max_sharpe_port = results_df.loc[results_df["Sharpe Ratio"].idxmax()]
        min_vol_port = results_df.loc[results_df["Annualized Volatility"].idxmin()]

        return results_df, max_sharpe_port["weights"], min_vol_port["weights"]
