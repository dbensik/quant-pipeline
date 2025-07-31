import pandas as pd


class RiskManager:
    """
    Acts as a historical risk analyzer for a completed backtest or live portfolio.

    This class is responsible for calculating descriptive risk statistics based on
    a static set of historical returns. Its purpose is to look backward and
    provide a quantitative assessment of how risky a strategy *was*. It does not
    participate in the trade execution loop.

    This is distinct from `backtesting.RiskManager`, which actively controls
    risk *during* a simulation (pre-trade).
    """

    def __init__(
        self, portfolio_returns: pd.Series, constituents_returns: pd.DataFrame = None
    ):
        """
        Initializes the risk analyzer with a completed history of returns.

        Args:
            portfolio_returns (pd.Series): A Series of the total portfolio's
                                           daily returns over the analysis period.
            constituents_returns (pd.DataFrame): A DataFrame of daily returns for
                                                 each individual asset in the portfolio.
        """
        if portfolio_returns.empty:
            raise ValueError("Portfolio returns cannot be empty.")
        self.portfolio_returns = portfolio_returns
        self.constituents_returns = constituents_returns

    def calculate_value_at_risk(self, confidence_level: float = 0.95) -> float:
        """
        Calculates the historical Value at Risk (VaR) for the portfolio.

        VaR answers the question: "What is the most I can expect to lose on a
        single day with a certain level of confidence?"

        Args:
            confidence_level (float): The confidence level for the VaR calculation
                                      (e.g., 0.95 for 95% confidence).

        Returns:
            float: The maximum expected loss (as a positive number). Returns 0 if calculation fails.
        """
        if self.portfolio_returns.empty:
            return 0.0
        # The quantile of returns. A 5% quantile gives the 95% VaR.
        var = self.portfolio_returns.quantile(1 - confidence_level)
        # Return as a positive value representing loss
        return abs(var) if pd.notna(var) else 0.0

    def calculate_conditional_value_at_risk(
        self, confidence_level: float = 0.95
    ) -> float:
        """
        Calculates the Conditional Value at Risk (CVaR), also known as Expected Shortfall.

        CVaR answers the question: "If I do have a loss exceeding my VaR,
        what is the average size of that loss?" It provides a more complete
        picture of tail risk than VaR alone.

        Args:
            confidence_level (float): The confidence level for the CVaR calculation.

        Returns:
            float: The expected shortfall (as a positive number). Returns 0 if calculation fails.
        """
        if self.portfolio_returns.empty:
            return 0.0
        var = -self.calculate_value_at_risk(confidence_level)
        # CVaR is the average of returns that are less than or equal to the VaR threshold
        cvar = self.portfolio_returns[self.portfolio_returns <= var].mean()
        return abs(cvar) if pd.notna(cvar) else 0.0

    def get_all_risk_metrics(self) -> dict:
        """
        A convenience method to calculate and return all key risk metrics.
        """
        return {
            "Value at Risk (95%)": self.calculate_value_at_risk(0.95),
            "Conditional VaR (95%)": self.calculate_conditional_value_at_risk(0.95),
        }

    def get_risk_contribution(self) -> pd.Series:
        """
        Calculates the contribution of each asset to the total portfolio volatility.
        (This is a more advanced calculation and is left as a future implementation).

        This method decomposes the total portfolio risk to show which assets are
        the biggest drivers of volatility. This is crucial for understanding
        concentration risk.

        Returns:
            pd.Series: A Series where the index is the asset ticker and the
                       values are their percentage contribution to total portfolio risk.
        """
        # Logic to calculate marginal contribution to risk for each asset...
        # This requires portfolio weights and the covariance matrix.
        pass
