import logging
from typing import Callable, Optional

import numpy as np
import pandas as pd

from backtesting.portfolio_backtester import PortfolioBacktester

logger = logging.getLogger(__name__)


class PortfolioOptimizer:
    """
    Optimizes asset weights in a portfolio using a Monte Carlo simulation
    to maximize a given performance metric for a Buy and Hold strategy.
    This class is decoupled from any specific UI framework.
    """

    def __init__(self, symbols: list, price_data: dict, strategy_model):
        self.symbols = symbols
        self.price_data = price_data
        self.strategy_model = strategy_model
        self.num_symbols = len(symbols)

    def run_monte_carlo(
        self,
        num_trials: int = 500,
        progress_callback: Optional[Callable[[float], None]] = None,
    ):
        """
        Runs the Monte Carlo simulation for weight optimization.

        Args:
            num_trials: The number of random weight combinations to test.
            progress_callback: An optional function that takes a float (0.0 to 1.0)
                               to report progress.
        """
        backtester = PortfolioBacktester()
        all_results = []

        for i in range(num_trials):
            # 1. Generate random weights that sum to 1
            weights = np.random.random(self.num_symbols)
            weights /= np.sum(weights)
            weight_dict = {
                symbol: weight for symbol, weight in zip(self.symbols, weights)
            }

            # 2. Generate signals (for Buy & Hold, this is simple)
            signals_data = {
                s: self.strategy_model.generate_signals(d)
                for s, d in self.price_data.items()
            }

            # 3. Run the backtest with this weight combination
            _, trade_log = backtester.run(self.price_data, signals_data, weight_dict)
            stats = backtester.get_performance_metrics()

            # 4. Store the results
            if stats:
                stats["weights"] = weight_dict
                all_results.append(stats)

            # --- REFACTOR: Use the callback to report progress ---
            if progress_callback:
                progress_callback((i + 1) / num_trials)

        if not all_results:
            return None

        return pd.DataFrame(all_results)
