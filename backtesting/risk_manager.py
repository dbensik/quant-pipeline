import pandas as pd

from .events import OrderEvent


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
        self, order: OrderEvent, portfolio_state: pd.Series
    ) -> OrderEvent | None:
        """
        Assesses a proposed order against the portfolio's risk rules.

        This method checks for drawdown breaches and scales down the order
        quantity if it exceeds the maximum risk per trade limit.

        Args:
            order: The proposed OrderEvent.
            portfolio_state: A pandas Series representing the current state of the portfolio.
                             Must include 'total' equity.

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
                new_quantity = int(
                    max_trade_value / portfolio_state[f"{order.symbol}_price"]
                )
                if new_quantity == 0:
                    return None  # Trade is too small to execute
                order.quantity = new_quantity

        return order
        return order

    def calculate_volatility_adjusted_size(
        self,
        portfolio_equity: float,
        asset_volatility: float,
        target_volatility: float = 0.15,
        asset_price: float = 1.0
    ) -> int:
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
            
        Returns:
            Number of shares to buy/hold.
        """
        if asset_volatility <= 0:
            return 0
            
        # Cap leverage factor to avoid extreme positions in low-vol assets
        # e.g., Max 200% exposure to any single asset
        leverage_factor = min(2.0, target_volatility / asset_volatility)
        
        target_exposure = portfolio_equity * leverage_factor
        quantity = int(target_exposure / asset_price)
        
        return quantity
