import random
from typing import Optional

from backtesting.events import FillEvent, OrderEvent


class SimulatedExecutionHandler:
    """
    A simulated execution handler that mimics market realities like
    slippage and commissions.
    """

    def __init__(
        self,
        slippage_pct: float = 0.0005,
        commission_per_trade: float = 1.0,
        seed: Optional[int] = None,
    ):
        """
        Initializes the simulated handler.

        Args:
            slippage_pct: The percentage of the price to use as a basis for random slippage.
            commission_per_trade: A fixed commission fee for each trade.
            seed: Seed for the slippage RNG. Pass an int for reproducible fills;
                  None keeps draws non-deterministic.

        NOTE: slippage uses a per-instance random.Random, not the module-level
        `random`. Two reasons. Determinism: the same backtest re-run gave
        different results (final value moved ~$375 and trade count 20 vs 22 on
        AAPL/ma_crossover), so saved results could not be reproduced and
        parameter comparisons were partly comparing random draws. Concurrency:
        the API runs backtests in a threadpool, and a shared global RNG means
        simultaneous requests consume each other's draws — seeding it globally
        would not even make them reproducible.
        """
        self.slippage_pct = slippage_pct
        self.commission_per_trade = commission_per_trade
        self.seed = seed
        self._rng = random.Random(seed)

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
        slippage = self._rng.uniform(-self.slippage_pct, self.slippage_pct)
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
