import random

from backtesting.events import FillEvent, OrderEvent


class SimulatedExecutionHandler:
    """
    A simulated execution handler that mimics market realities like
    slippage and commissions.
    """

    def __init__(self, slippage_pct: float = 0.0005, commission_per_trade: float = 1.0):
        """
        Initializes the simulated handler.

        Args:
            slippage_pct: The percentage of the price to use as a basis for random slippage.
            commission_per_trade: A fixed commission fee for each trade.
        """
        self.slippage_pct = slippage_pct
        self.commission_per_trade = commission_per_trade

    def execute_order(self, order: OrderEvent, current_price: float) -> FillEvent:
        """
        Simulates the execution of an order, applying slippage and commission.

        Args:
            order: The OrderEvent to be executed.
            current_price: The ideal market price at the time of the order.

        Returns:
            A FillEvent with the details of the executed trade.
        """
        # 1. Simulate Slippage
        slippage = random.uniform(-self.slippage_pct, self.slippage_pct)
        fill_price = current_price * (1 + slippage)

        # 2. Calculate Commission
        commission = self.commission_per_trade

        # 3. Create the Fill Event
        return FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=fill_price,
            commission=commission,
        )
