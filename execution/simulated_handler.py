import math
import random
from dataclasses import replace
from typing import Optional

from backtesting.events import FillEvent, OrderEvent


def to_lot(quantity: float, lot_size: float) -> float:
    """
    `quantity` rounded DOWN to a whole number of lots (for quantity >= 0).

    With lot_size 1.0 this is int(), exactly what sizing used before lots
    existed, so whole-unit runs keep their int quantities and are unchanged.
    A fractional lot snaps to the nearest whole lot when within a millionth of
    one: 32,659,001 satoshi is stored as 0.32659001 and divides to
    32659000.999999996, so a plain floor drops a satoshi, and an absolute
    nudge does not help, because at that size it is below float resolution.
    """
    if lot_size == 1.0:
        return int(quantity)
    lots = quantity / lot_size
    nearest = round(lots)
    return (nearest if abs(lots - nearest) < 1e-6 else math.floor(lots)) * lot_size


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
        commission_rate: float = 0.0,
    ):
        """
        Initializes the simulated handler.

        Args:
            slippage_pct: Upper bound of the random slippage, as a fraction of
                the price. Always ADVERSE: a buy fills above the price and a
                sell below, by a uniform draw in [0, slippage_pct].
            commission_per_trade: A fixed commission fee for each trade.
            seed: Seed for the slippage RNG. Pass an int for reproducible fills;
                  None keeps draws non-deterministic.
            commission_rate: Proportional cost as a fraction of the fill's
                value (0.001 = 10 bps), on top of the fixed fee. This is the
                API's `transaction_cost`.

        Until 2026-10-12 slippage was uniform in [-slippage_pct, +slippage_pct]
        whatever the side, so it averaged to zero and half of all fills were
        better than the price; and `transaction_cost` was accepted by every
        endpoint and read by nothing. A backtest paid $1 a trade and no more.

        NOTE: slippage uses a per-instance random.Random, not the module-level
        `random`. Two reasons. Determinism: the same backtest re-run gave
        different results (final value moved ~$375 and trade count 20 vs 22 on
        AAPL/ma_crossover), so saved results could not be reproduced and
        parameter comparisons were partly comparing random draws. Concurrency:
        the API runs backtests in a threadpool, and a shared global RNG means
        simultaneous requests consume each other's draws — seeding it globally
        would not even make them reproducible.
        """
        if commission_rate < 0:
            raise ValueError(f"commission_rate must be >= 0, got {commission_rate}")
        self.slippage_pct = slippage_pct
        self.commission_per_trade = commission_per_trade
        self.commission_rate = commission_rate
        self.seed = seed
        self._rng = random.Random(seed)

    def commission_for(self, quantity: float, fill_price: float) -> float:
        """Fixed fee plus the proportional cost on the fill's value."""
        return self.commission_per_trade + self.commission_rate * quantity * fill_price

    def execute_order(self, order: OrderEvent, current_price: float) -> FillEvent:
        """
        Simulates the execution of an order, applying slippage and commission.

        Args:
            order: The OrderEvent to be executed.
            current_price: The ideal market price at the time of the order.

        Returns:
            A FillEvent with the details of the executed trade.
        """
        # 1. Slippage: one draw per order, always against the trader.
        slippage = self._rng.uniform(0.0, self.slippage_pct)
        sign = 1.0 if order.direction == "BUY" else -1.0
        fill_price = current_price * (1 + sign * slippage)

        # 2. Create the Fill Event
        return FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=fill_price,
            commission=self.commission_for(order.quantity, fill_price),
        )

    def affordable_quantity(self, cash: float, fill_price: float, lot_size: float) -> float:
        """
        The most lots a BUY at `fill_price` can take without its total cost
        (value + fixed fee + proportional cost) exceeding `cash`; 0 if none.
        lot_size is 1.0 for whole shares (config.settings.lot_size_for).
        """
        if fill_price <= 0 or cash <= self.commission_per_trade:
            return 0.0
        lots = math.floor(
            (cash - self.commission_per_trade)
            / (fill_price * (1 + self.commission_rate))
            / lot_size
        )
        # Floating point can leave the total a hair above cash; step down.
        while lots > 0 and (
            lots * lot_size * fill_price + self.commission_for(lots * lot_size, fill_price) > cash
        ):
            lots -= 1
        return float(lots * lot_size)

    def resize(self, fill: FillEvent, quantity: float) -> FillEvent:
        """The same fill (same price, no new slippage draw) for `quantity`."""
        return replace(
            fill, quantity=quantity, commission=self.commission_for(quantity, fill.fill_price)
        )
