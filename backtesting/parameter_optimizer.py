"""
backtesting/parameter_optimizer.py
Grid search over one strategy's parameters on a single symbol.

REWRITTEN 2026-08-09 (Phase 5 — decommissioning Streamlit). The previous
version had three defects that made a grid search unable to answer the
question it was for:

  1. `_create_model` was a private factory hardcoding two display names
     ("Mean Reversion", "Moving Average Crossover") and returning None for
     anything else — which `run_optimization` silently skipped, so optimizing
     an unsupported strategy produced an empty DataFrame rather than an error.
     Model construction now goes through alpha_models.registry, the same
     single source of truth the routers and screeners use.

  2. One `Backtester()` was shared across every combination and constructed
     without a seed. Comparing parameter sets is exactly what unseeded
     slippage corrupts: each combo saw a different draw, so the ranking mixed
     parameter effect with noise. Each combo now gets a fresh Backtester
     carrying the same seed, so every candidate sees identical slippage draws
     (common random numbers) and the difference between them is the parameters.

  3. `get_best_parameters` recovered params by subtracting a hardcoded list of
     six metric names from the result row. PerformanceAnalyzer returns ten, so
     "Final Value", "Calmar Ratio", "Max Drawdown Duration (Days)" and
     "Trade Count" were all reported as tuned parameters. Params are now
     selected by name from the grid that produced them.
"""

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from alpha_models import registry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metric directions
# ---------------------------------------------------------------------------
# Ranking needs a direction per metric. The previous version always took
# idxmax, so optimizing for "Annualized Volatility" returned the *most*
# volatile parameter set while reporting it as best.
#
# "Max Drawdown" is maximized, not minimized: PerformanceAnalyzer computes it
# as drawdown.min() of (value - cummax) / cummax, so it is <= 0 and the
# shallowest drawdown is the one closest to zero.
MAXIMIZE = {
    "Final Value",
    "Total Return",
    "Annualized Return",
    "Sharpe Ratio",
    "Sortino Ratio",
    "Calmar Ratio",
    "Max Drawdown",
}
MINIMIZE = {
    "Annualized Volatility",
    "Max Drawdown Duration (Days)",
}

# "Trade Count" is deliberately absent from both: neither more nor fewer
# trades is better on its own, so it is reported but cannot be optimized for.
OPTIMIZABLE_METRICS = sorted(MAXIMIZE | MINIMIZE)


class ParameterOptimizer:
    """
    Runs one backtest per parameter combination and ranks the results.

    Args:
        price_data:  OHLCV frame for a single symbol.
        strategy_id: Registry id (e.g. "ma_crossover"), NOT a display name.
        param_grid:  List of parameter dicts to evaluate.
        metric:      Metric to rank by; must be in OPTIMIZABLE_METRICS.
        initial_capital / transaction_cost: passed to each Backtester.
        seed:        Slippage seed applied to every combination.
    """

    def __init__(
        self,
        price_data: pd.DataFrame,
        strategy_id: str,
        param_grid: List[Dict[str, Any]],
        metric: str = "Sharpe Ratio",
        initial_capital: float = 100_000.0,
        transaction_cost: float = 0.001,
        seed: Optional[int] = 42,
    ):
        if metric not in MAXIMIZE and metric not in MINIMIZE:
            raise ValueError(
                f"Cannot optimize for {metric!r}. "
                f"Choose one of: {', '.join(OPTIMIZABLE_METRICS)}."
            )

        # Raises KeyError for an unknown id — the caller decides whether that
        # is a 404. The old version returned an empty frame instead.
        self.spec = registry.get(strategy_id)

        self.price_data = price_data
        self.strategy_id = strategy_id
        self.param_grid = param_grid
        self.metric = metric
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.seed = seed

        self.results_df: Optional[pd.DataFrame] = None
        #: Combinations the strategy itself rejected, with the reason. Reported
        #: rather than dropped, so a grid that is entirely invalid is visible
        #: as such instead of looking like a search that found nothing.
        self.skipped: List[Dict[str, Any]] = []
        #: The parameter dict behind each row of results_df, in the same order.
        #: Kept separately because reading params back out of the DataFrame
        #: upcasts them: .loc[i] returns a Series with one dtype across params
        #: AND metrics, so an int short_window comes back as np.float64(5.0).
        #: It cannot simply be indexed against param_grid either, since
        #: rejected combinations leave gaps.
        self._result_params: List[Dict[str, Any]] = []

    # -- run ----------------------------------------------------------------

    def run_optimization(
        self, progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> pd.DataFrame:
        """
        Evaluate every combination.

        Args:
            progress_callback: Called as (completed, total) after each combo.
                               Used by the websocket endpoint to stream
                               progress — a grid search runs for minutes.
        """
        from backtesting.backtester import Backtester

        results: List[Dict[str, Any]] = []
        self.skipped = []
        self._result_params = []
        total = len(self.param_grid)

        for index, params in enumerate(self.param_grid, start=1):
            try:
                model = self.spec.build(params)
            except (ValueError, TypeError) as exc:
                # An invalid *combination* — e.g. MovingAverageCrossoverStrategy
                # raises when short_window >= long_window. This replaces the
                # `if s >= l: continue` that MACrossoverParameterGenerator
                # hardcoded, and works for any strategy that validates itself.
                self.skipped.append({"params": dict(params), "reason": str(exc)})
                if progress_callback:
                    progress_callback(index, total)
                continue

            # Fresh, identically seeded handler per combination: common random
            # numbers across candidates. A shared instance would advance its
            # RNG, so later combos would face different slippage than earlier
            # ones and the ranking would partly measure that.
            backtester = Backtester(
                initial_capital=self.initial_capital,
                transaction_cost=self.transaction_cost,
                seed=self.seed,
            )

            try:
                backtester.run(self.price_data, model)
                stats = backtester.get_performance_metrics()
            except (ValueError, KeyError) as exc:
                self.skipped.append({"params": dict(params), "reason": str(exc)})
                if progress_callback:
                    progress_callback(index, total)
                continue

            record = dict(params)
            record.update(stats or {})
            results.append(record)
            self._result_params.append(dict(params))

            if progress_callback:
                progress_callback(index, total)

        self.results_df = pd.DataFrame(results)
        return self.results_df

    # -- rank ---------------------------------------------------------------

    def _best_index(self) -> Optional[int]:
        if self.results_df is None or self.results_df.empty:
            return None
        if self.metric not in self.results_df.columns:
            return None

        column = pd.to_numeric(self.results_df[self.metric], errors="coerce")
        # A combo that never trades yields NaN Sharpe. If EVERY combo does,
        # idxmax/idxmin raises — so the emptiness is checked, not risked.
        if column.dropna().empty:
            return None

        return int(column.idxmax() if self.metric in MAXIMIZE else column.idxmin())

    def get_best_parameters(self) -> Dict[str, Any]:
        """
        The parameter set that optimized the target metric.

        Returned from the recorded combination rather than read back out of
        results_df, for two reasons: the DataFrame upcasts an int parameter to
        float, and recovering params by subtracting known metric names (what
        this used to do) silently reports any newly added metric as a tuned
        parameter — PerformanceAnalyzer returns ten and the hardcoded list
        named six.
        """
        index = self._best_index()
        if index is None:
            return {}
        return dict(self._result_params[index])

    def get_best_metrics(self) -> Dict[str, Any]:
        """Full metric set for the winning combination."""
        index = self._best_index()
        if index is None:
            return {}
        row = self.results_df.loc[index]
        names = set(self._result_params[index])
        return {k: v for k, v in row.items() if k not in names}

    def get_ranked_results(self, top_n: Optional[int] = None) -> pd.DataFrame:
        """Results ordered best-first by the target metric."""
        if self.results_df is None or self.results_df.empty:
            return pd.DataFrame()
        if self.metric not in self.results_df.columns:
            return self.results_df

        ranked = self.results_df.sort_values(
            by=self.metric,
            ascending=self.metric in MINIMIZE,
            na_position="last",
        )
        return ranked.head(top_n) if top_n else ranked

    def get_ranked_records(self, top_n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Ranked results as plain dicts, with parameters at their ORIGINAL types.

        The DataFrame cannot carry these: a row spanning int params and float
        metrics upcasts to one dtype, so short_window comes out 20.0. That
        matters beyond cosmetics — a UI that lets you click a result row to
        re-run it would post 20.0 where the registry declares an int.
        Parameters are taken from the recorded combination via the row's index
        label, which sort_values preserves.
        """
        ranked = self.get_ranked_results(top_n)
        if ranked.empty:
            return []

        records: List[Dict[str, Any]] = []
        for label, row in ranked.iterrows():
            params = self._result_params[int(label)]
            metrics = {k: v for k, v in row.items() if k not in params}
            records.append({**params, **metrics})
        return records
