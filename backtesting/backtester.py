import numpy as np
import pandas as pd

from alpha_models.base_model import BaseAlphaModel
from analysis.performance_analyzer import PerformanceAnalyzer
from backtesting.events import OrderEvent
from execution.simulated_handler import SimulatedExecutionHandler


class Backtester:
    """
    A realistic backtester that simulates trading based on capital allocation,
    enforcing whole-share trades and using a robust event-driven loop.
    Its single responsibility is to generate a portfolio history.
    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        transaction_cost: float = 0.001,
        execution_handler: SimulatedExecutionHandler = None,
        seed: int = None,
    ):
        """
        Initializes the Backtester with portfolio settings.

        Args:
            transaction_cost: Proportional cost per fill as a fraction of its
                  value (0.001 = 10 bps), charged by the default execution
                  handler on top of its $1 fee. Until 2026-10-12 it was stored
                  here and read by nothing.
            seed: Seed for the default execution handler's slippage RNG. Pass an
                  int to make a run reproducible.

        An explicit execution_handler owns its own RNG and costs, so seed and
        transaction_cost do not reach it.
        """
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.results = None
        self.trade_log = []
        #: The per-bar cash interest of the last run; metrics measure Sharpe
        #: against this same cash account.
        self.cash_accrual = None
        self.execution_handler = execution_handler or SimulatedExecutionHandler(
            seed=seed, commission_rate=transaction_cost
        )

    def run(
        self,
        price_data: pd.DataFrame,
        model: BaseAlphaModel,
        symbol_name: str = None,
        *,
        cash_accrual: np.ndarray,
    ) -> pd.DataFrame:
        """
        Runs a backtest using a more robust, event-driven loop that delegates
        execution to a dedicated handler.

        cash_accrual: interest one unit of cash earns on each bar, aligned to
        `price_data` (api/risk_free.py builds it from ^IRX; 0 on bar 0). Idle
        cash compounds by it at the START of each bar, before any order is
        sized. Until 2026-10-12 cash earned nothing, so a strategy out of the
        market was charged T-bill interest it was never paid once Sharpe
        became excess of the rate. No default; zeros reproduce the old run
        bit for bit (cash * 1.0 is cash).
        """
        accrual = np.asarray(cash_accrual, dtype=float)
        if len(accrual) != len(price_data):
            raise ValueError(f"cash_accrual has {len(accrual)} values for {len(price_data)} bars")
        self.cash_accrual = accrual
        # 1. Generate signals from the model
        signals = model.generate_signals(price_data=price_data)
        self.trade_log = []

        # 2. Prepare the portfolio DataFrame for state tracking
        portfolio = price_data[["Close"]].copy()
        portfolio["signal"] = signals["signal"].ffill().fillna(0)
        portfolio["holdings"] = 0.0
        portfolio["cash"] = self.initial_capital
        portfolio["position"] = 0.0  # Number of shares held

        # 3. Use a stateful loop to process trades realistically
        position = 0.0
        cash = self.initial_capital
        symbol = (
            symbol_name
            if symbol_name
            else (price_data.name if hasattr(price_data, "name") else "Asset")
        )

        # State is recorded into numpy arrays and assigned to the frame once,
        # after the loop. Writing three cells per bar through `iloc` was 81% of
        # a run (0.51 s of 0.63 s on 2,520 bars, profiled 2026-10-07); the
        # loop itself — the part that keeps order handling sequential and the
        # slippage RNG in step — is unchanged. Values are the same float64
        # arithmetic as before, so results are bit-identical (checked against
        # 56 real runs, see research/monte-carlo-plan-2026-10-07.md).
        n_bars = len(portfolio)
        closes = portfolio["Close"].to_numpy()
        signal_values = portfolio["signal"].to_numpy()
        positions = np.zeros(n_bars)
        cash_values = np.zeros(n_bars)
        holdings = np.zeros(n_bars)

        for i in range(n_bars):
            date = portfolio.index[i]
            price = closes[i]
            signal = signal_values[i]

            # Interest on idle cash for the period ending at this bar.
            cash *= 1.0 + accrual[i]

            # --- REFACTOR: Trading logic now uses the event-driven system ---
            if signal == 1 and np.isclose(position, 0):  # Buy signal
                if price > 0:
                    # Determine max shares possible based on current cash
                    shares_to_buy = np.floor(cash / price)
                    if shares_to_buy > 0:
                        order = OrderEvent(date, symbol, "MKT", shares_to_buy, "BUY")
                        fill = self.execution_handler.execute_order(order, price)

                        # Sized on the close, the fill can cost more than the
                        # cash: adverse slippage plus commission. Until
                        # 2026-10-12 such an order was DROPPED and retried next
                        # bar: 80 unfilled buy bars over 15 real runs (5
                        # symbols x 3 strategies, 2016-2025); TQQQ
                        # mean_reversion alone lost 42. Buy what the cash covers at THIS fill
                        # instead, as a broker would. The fill is reused, not
                        # re-executed: one slippage draw per order keeps the
                        # RNG in step, so runs that never hit this are unchanged.
                        if fill.total_cost > cash:
                            fill = self.execution_handler.resize(
                                fill,
                                self.execution_handler.affordable_quantity(cash, fill.fill_price),
                            )

                        if fill.quantity > 0 and cash >= fill.total_cost:
                            position += fill.quantity
                            cash -= fill.total_cost
                            self.trade_log.append(fill)

            elif signal == 0 and position > 0:  # Sell signal (liquidate position)
                order = OrderEvent(date, symbol, "MKT", position, "SELL")
                fill = self.execution_handler.execute_order(order, price)

                position -= fill.quantity  # Should go to zero
                cash += fill.total_cost
                self.trade_log.append(fill)

            # --- Record Portfolio State for the current day ---
            positions[i] = position
            cash_values[i] = cash
            holdings[i] = position * price

        portfolio["position"] = positions
        portfolio["cash"] = cash_values
        portfolio["holdings"] = holdings

        # 4. Calculate final portfolio values
        portfolio["total"] = portfolio["holdings"] + portfolio["cash"]
        portfolio["returns"] = portfolio["total"].pct_change().fillna(0)

        self.results = portfolio
        return portfolio

    def get_trade_log(self) -> pd.DataFrame:
        """
        Returns the log of all trades executed during the backtest as a DataFrame.
        """
        if not self.trade_log:
            return pd.DataFrame()

        # Convert FillEvent objects to dicts, explicitly adding the property 'total_cost'
        log_data = []
        for fill in self.trade_log:
            data = vars(fill).copy()
            data["total_cost"] = fill.total_cost
            log_data.append(data)

        return pd.DataFrame(log_data)

    def get_performance_metrics(self) -> dict:
        """
        Calculates performance metrics by delegating to the PerformanceAnalyzer.

        Sharpe is measured against the cash account this run credited, so the
        accrual comes from the run rather than being passed again.
        """
        if self.results is None or self.results.empty:
            return {}  # Return empty dict if no results exist

        analyzer = PerformanceAnalyzer(
            self.results,
            initial_capital=self.initial_capital,
            risk_free_accrual=self.cash_accrual,
        )
        return analyzer.calculate_all_metrics()

    def run_and_get_metrics(
        self, price_data: pd.DataFrame, model: BaseAlphaModel, cash_accrual: np.ndarray
    ) -> dict:
        """
        A convenience method that runs a backtest and immediately returns the
        performance metrics. Useful for optimizations.
        """
        self.run(price_data=price_data, model=model, cash_accrual=cash_accrual)
        return self.get_performance_metrics()
