"""
portfolio/risk_manager.py

The project's two risk classes, side by side and deliberately not merged
(brief task 2, 2026-10-20):

- `PortfolioRiskManager` checks each order BEFORE it trades: the 2% cap per
  buy, the drawdown halt and volatility sizing. Used by the portfolio
  backtester (`backtesting/portfolio_backtester.py`) and the paper trader.
  It lived in backtesting/risk_manager.py until then.
- `RiskManager` describes a FINISHED return series: VaR and CVaR, reported on
  the allocation response.

tests/unit/test_risk_managers.py pins both on fixed inputs.
"""

import pandas as pd

from backtesting.events import OrderEvent
from execution.simulated_handler import to_lot


class PortfolioRiskManager:
    """
    Acts as a real-time risk controller during a backtest simulation.

    This class is responsible for enforcing risk rules *before* a trade is
    executed. Its primary role is to manage position sizing and enforce
    portfolio-level constraints to prevent catastrophic losses and ensure
    the trading strategy adheres to a predefined risk framework.
    """

    def __init__(
        self,
        max_trade_risk_pct: float = 0.02,
        max_portfolio_drawdown_pct: float = 0.20,
    ):
        """
        Initializes the risk manager with portfolio-level risk rules.

        Args:
            max_trade_risk_pct (float): The maximum percentage of total portfolio
                                        equity to risk on a single trade (e.g., 0.02 for 2%).
            max_portfolio_drawdown_pct (float): The maximum portfolio drawdown allowed before
                                        halting all new trades.
        """
        self.max_trade_risk_pct = max_trade_risk_pct
        self.max_portfolio_drawdown_pct = max_portfolio_drawdown_pct
        self.high_water_mark = 0.0

    def assess_order(
        self, order: OrderEvent, portfolio_state: pd.Series, *, lot_size: float
    ) -> OrderEvent | None:
        """
        Assesses a proposed order against the portfolio's risk rules.

        This method checks for drawdown breaches and scales down the order
        quantity if it exceeds the maximum risk per trade limit.

        Args:
            order: The proposed OrderEvent.
            portfolio_state: A pandas Series representing the current state of the portfolio.
                             Must include 'total' equity.
            lot_size: Smallest tradable quantity (1.0 = whole shares). A
                      down-sized order is floored to it; with whole units a
                      2% cap on $20,000 cut a ~$30,000 BTC order to 0.

        Returns:
            The approved (and potentially down-sized) OrderEvent, or None if
            the order is rejected.
        """
        portfolio_equity = portfolio_state["total"]

        # 1. Update High-Water Mark
        if portfolio_equity > self.high_water_mark:
            self.high_water_mark = portfolio_equity

        # 2. Check for Max Drawdown Breach
        drawdown = (self.high_water_mark - portfolio_equity) / self.high_water_mark
        if drawdown > self.max_portfolio_drawdown_pct:
            # Halt all new buy orders if drawdown is breached
            if order.direction == "BUY":
                return None

        # 3. Enforce Max Risk Per Trade (for BUY orders)
        if order.direction == "BUY":
            max_trade_value = portfolio_equity * self.max_trade_risk_pct
            proposed_trade_value = (
                order.quantity * portfolio_state[f"{order.symbol}_price"]
            )  # Assumes price is in state

            if proposed_trade_value > max_trade_value:
                # Scale down the order to meet the risk limit
                new_quantity = to_lot(
                    max_trade_value / portfolio_state[f"{order.symbol}_price"], lot_size
                )
                if new_quantity == 0:
                    return None  # Trade is too small to execute
                order.quantity = new_quantity

        return order

    def calculate_volatility_adjusted_size(
        self,
        portfolio_equity: float,
        asset_volatility: float,
        target_volatility: float = 0.15,
        asset_price: float = 1.0,
        *,
        lot_size: float,
    ) -> float:
        """
        Calculates the position size (quantity) to achieve a target volatility contribution.

        Formula:
            Target Exposure = (Target Vol / Asset Vol) * Equity
            Quantity = Target Exposure / Price

        Args:
            portfolio_equity: Current total portfolio value.
            asset_volatility: Annualized volatility of the asset (decimal, e.g., 0.20).
            target_volatility: Target annualized volatility contribution (decimal, e.g., 0.15).
            asset_price: Current price of the asset.
            lot_size: Smallest tradable quantity (1.0 = whole shares).

        Returns:
            Number of shares to buy/hold.
        """
        if asset_volatility <= 0:
            return 0

        # Cap leverage factor to avoid extreme positions in low-vol assets
        # e.g., Max 200% exposure to any single asset
        leverage_factor = min(2.0, target_volatility / asset_volatility)

        target_exposure = portfolio_equity * leverage_factor
        quantity = to_lot(target_exposure / asset_price, lot_size)

        return quantity


class RiskManager:
    """
    Acts as a historical risk analyzer for a completed backtest or live portfolio.

    This class is responsible for calculating descriptive risk statistics based on
    a static set of historical returns. Its purpose is to look backward and
    provide a quantitative assessment of how risky a strategy *was*. It does not
    participate in the trade execution loop.

    This is distinct from `PortfolioRiskManager` above, which actively
    controls risk *during* a simulation (pre-trade).
    """

    def __init__(self, portfolio_returns: pd.Series, constituents_returns: pd.DataFrame = None):
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

    def calculate_conditional_value_at_risk(self, confidence_level: float = 0.95) -> float:
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
