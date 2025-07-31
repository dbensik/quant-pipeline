from dataclasses import dataclass
from datetime import datetime


@dataclass
class OrderEvent:
    """
    Represents the intention to place an order on the market.
    The backtester generates these, and the execution handler acts on them.
    """

    timestamp: datetime
    symbol: str
    order_type: str  # e.g., 'MKT' for Market Order
    quantity: float
    direction: str  # 'BUY' or 'SELL'


@dataclass
class FillEvent:
    """
    Represents a filled order, as returned by an execution handler.
    It contains the actual cost and quantity of the transaction.
    """

    timestamp: datetime
    symbol: str
    direction: str
    quantity: float
    fill_price: float
    commission: float

    @property
    def total_cost(self) -> float:
        """The total cost of the transaction, including commission."""
        if self.direction == "BUY":
            return (self.quantity * self.fill_price) + self.commission
        else:  # SELL
            return (self.quantity * self.fill_price) - self.commission
