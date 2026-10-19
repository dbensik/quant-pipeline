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
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from alpha_models import registry
from alpha_models.base_model import BaseAlphaModel
from analysis.performance_analyzer import PerformanceAnalyzer

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

#: Default anchored train length before the first validate year. Three years
#: is what decision 41 measured: bars from 2010-01-04 validate from 2013.
WALK_FORWARD_MIN_TRAIN_YEARS = 3


# ---------------------------------------------------------------------------
# Walk-forward helpers
# ---------------------------------------------------------------------------


def walk_forward_folds(index: pd.Index, min_train_years: int) -> List[Tuple[int, int]]:
    """
    Annual, anchored folds as (boundary, last) bar positions.

    A calendar year Y is a validate fold when the data starts on or before
    8 January of Y - min_train_years, i.e. at least `min_train_years` whole
    calendar years precede it; the week's slack lets a series that opens on
    the first session after New Year (2010-01-04) count its first year.
    `boundary` is the last bar of Y-1: the train window is bars 0..boundary,
    the pick is made on its close and first trades at Y's first open.
    `last` is Y's last bar, so the final fold may be a partial year.
    """
    if min_train_years < 1:
        raise ValueError(f"min_train_years must be >= 1, got {min_train_years}")
    dates = pd.DatetimeIndex(index)
    if len(dates) < 2:
        return []
    first = dates[0]
    years = dates.year.to_numpy()
    folds: List[Tuple[int, int]] = []
    for year in sorted(set(years.tolist())):
        if first > pd.Timestamp(year - min_train_years, 1, 8, tz=first.tz):
            continue
        positions = np.flatnonzero(years == year)
        if positions[0] == 0:
            continue
        folds.append((int(positions[0] - 1), int(positions[-1])))
    return folds


def window_metrics(
    total: np.ndarray,
    position: Optional[np.ndarray],
    index: pd.Index,
    accrual: np.ndarray,
    base: int,
    last: int,
    capital: float,
) -> Dict[str, Any]:
    """
    PerformanceAnalyzer's metrics for bars base+1..last, as an account worth
    `capital` at bar `base` would see them.

    Every walk-forward figure goes through here so it has the definition
    /backtest uses. The value is rebased, so Final Value and Total Return read
    per `capital` whatever the account held before. Fills are counted from
    position changes AFTER the base bar: a position carried into the window
    is not a trade made in it. With base 0 this equals the metrics of a run
    cut off at `last` (bar 0 is flat and earns no interest).
    """
    values = np.asarray(total[base : last + 1], dtype=float)
    values = values * (capital / values[0])
    frame = pd.DataFrame({"total": values}, index=index[base : last + 1])
    frame["returns"] = frame["total"].pct_change().fillna(0.0)
    if position is not None:
        held = np.asarray(position[base : last + 1], dtype=float)
        frame["trades"] = np.concatenate([[0.0], (np.diff(held) != 0).astype(float)])
    rate = np.array(accrual[base : last + 1], dtype=float)
    rate[0] = 0.0
    return PerformanceAnalyzer(
        frame, initial_capital=capital, risk_free_accrual=rate
    ).calculate_all_metrics()


def _pick(scores: List[float], metric: str) -> Optional[int]:
    """Index of the best score in the metric's direction; None if all are NaN."""
    values = np.array(scores, dtype=float)
    if np.all(np.isnan(values)):
        return None
    return int(np.nanargmax(values) if metric in MAXIMIZE else np.nanargmin(values))


class _FixedSignals(BaseAlphaModel):
    """A precomputed signal column; how the walk-forward account is traded."""

    def __init__(self, signal: np.ndarray):
        self.signal = signal

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"signal": self.signal}, index=price_data.index)


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
        *,
        cash_accrual: np.ndarray,
        lot_size: float,
    ):
        if metric not in MAXIMIZE and metric not in MINIMIZE:
            raise ValueError(
                f"Cannot optimize for {metric!r}. Choose one of: {', '.join(OPTIMIZABLE_METRICS)}."
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
        #: Per-bar cash interest (Backtester.run); every combination's idle
        #: cash earns it, and its Sharpe is measured against it. Checked here,
        #: because a mismatch raised inside the loop would be recorded as
        #: every combination being "rejected".
        self.cash_accrual = np.asarray(cash_accrual, dtype=float)
        if len(self.cash_accrual) != len(price_data):
            raise ValueError(
                f"cash_accrual has {len(self.cash_accrual)} values for {len(price_data)} bars"
            )
        #: Smallest tradable quantity (Backtester.run); checked here for the
        #: same reason as the accrual.
        if not lot_size > 0:
            raise ValueError(f"lot_size must be > 0, got {lot_size}")
        self.lot_size = lot_size

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
        #: (total, position) arrays of each evaluated combination, in the
        #: order of results_df: what run_walk_forward slices.
        self._runs: List[Tuple[np.ndarray, np.ndarray]] = []
        #: The walk-forward account's portfolio frame, after run_walk_forward.
        self.walk_forward_portfolio: Optional[pd.DataFrame] = None
        #: The last run_walk_forward result, and why it was None if it was
        #: (the window holds no fold).
        self.walk_forward_report: Optional[Dict[str, Any]] = None
        self.walk_forward_unavailable: Optional[str] = None

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
        self._runs = []
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
                portfolio = backtester.run(
                    self.price_data, model, cash_accrual=self.cash_accrual, lot_size=self.lot_size
                )
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
            self._runs.append(
                (portfolio["total"].to_numpy(copy=True), portfolio["position"].to_numpy(copy=True))
            )

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

    # -- walk-forward -------------------------------------------------------

    def run_walk_forward(
        self, min_train_years: int = WALK_FORWARD_MIN_TRAIN_YEARS
    ) -> Optional[Dict[str, Any]]:
        """
        Anchored walk-forward over the runs of run_optimization (call it first).

        Each calendar year Y of walk_forward_folds, the combination with the
        best `metric` on bars [first, end of Y-1] is picked and traded through
        Y. Strategies pass the look-ahead contract, so a train figure sliced
        from a full run is the figure of a run that stopped at the train end:
        one run per combination, not one per fold.

        The out-of-sample result is ONE continuous account, not the picks'
        own runs stitched together. In year Y its signal is that year's
        pick's; on the boundary bar it is set to enter (1) if the pick's own
        run held a position there, else to exit (-1), so the account holds
        what the new pick holds from Y's first open, paying the costs of the
        switch. Before the first fold it is flat. A fold where every
        combination scored NaN (none traded in the train window) is flat.

        Returns None, with `walk_forward_unavailable` set, when the window
        holds no fold. Otherwise a dict of plain values: per-fold picks with
        their train score and out-of-sample metrics, the account's metrics
        over the whole evaluated span, and the HINDSIGHT figure: the best
        combination's metrics on that same span, which is what ranking on
        the full window reports. Measured against each other on one span,
        the two answer whether the search finds anything that persists.
        """
        from backtesting.backtester import Backtester

        if not self._runs:
            raise RuntimeError("run_optimization() has produced no runs to walk forward over")

        index = self.price_data.index
        folds = walk_forward_folds(index, min_train_years)
        self.walk_forward_portfolio = None
        self.walk_forward_report = None
        if not folds:
            years = pd.DatetimeIndex(index)
            self.walk_forward_unavailable = (
                f"Walk-forward needs {min_train_years} whole calendar year(s) of history before "
                f"a validate year; this window runs {years[0].date()} to {years[-1].date()}."
            )
            return None
        self.walk_forward_unavailable = None

        def score(run: Tuple[np.ndarray, np.ndarray], base: int, last: int) -> Dict[str, Any]:
            return window_metrics(
                run[0], run[1], index, self.cash_accrual, base, last, self.initial_capital
            )

        signals: Dict[int, np.ndarray] = {}
        stitched = np.full(len(index), -1.0)
        picks: List[Optional[int]] = []
        train_scores: List[Optional[float]] = []
        for boundary, last in folds:
            scores = [score(run, 0, boundary).get(self.metric, np.nan) for run in self._runs]
            choice = _pick(scores, self.metric)
            picks.append(choice)
            train_scores.append(None if choice is None else float(scores[choice]))
            if choice is None:
                stitched[boundary : last + 1] = -1.0
                continue
            if choice not in signals:
                model = self.spec.build(self._result_params[choice])
                signals[choice] = (
                    model.generate_signals(price_data=self.price_data)["signal"]
                    .ffill()
                    .fillna(0)
                    .to_numpy(dtype=float)
                )
            stitched[boundary + 1 : last + 1] = signals[choice][boundary + 1 : last + 1]
            stitched[boundary] = 1.0 if self._runs[choice][1][boundary] > 0 else -1.0

        backtester = Backtester(
            initial_capital=self.initial_capital,
            transaction_cost=self.transaction_cost,
            seed=self.seed,
        )
        account = backtester.run(
            self.price_data,
            _FixedSignals(stitched),
            cash_accrual=self.cash_accrual,
            lot_size=self.lot_size,
        )
        self.walk_forward_portfolio = account
        account_run = (account["total"].to_numpy(), account["position"].to_numpy())

        fold_rows: List[Dict[str, Any]] = []
        gaps: List[float] = []
        for (boundary, last), choice, train in zip(folds, picks, train_scores, strict=True):
            oos = score(account_run, boundary, last)
            fold_rows.append(
                {
                    "train_start": index[0].isoformat(),
                    "train_end": index[boundary].isoformat(),
                    "validate_start": index[boundary + 1].isoformat(),
                    "validate_end": index[last].isoformat(),
                    "bars": last - boundary,
                    "params": None if choice is None else dict(self._result_params[choice]),
                    "train_metric": train,
                    "oos_metrics": oos,
                }
            )
            value = oos.get(self.metric)
            if train is not None and value is not None and np.isfinite(value):
                gaps.append(train - value)

        span_base, span_last = folds[0][0], folds[-1][1]
        span_scores = [score(run, span_base, span_last) for run in self._runs]
        hindsight = _pick([s.get(self.metric, np.nan) for s in span_scores], self.metric)
        oos_metrics = score(account_run, span_base, span_last)
        hindsight_metrics = None if hindsight is None else span_scores[hindsight]
        self.walk_forward_report = {
            "metric": self.metric,
            "min_train_years": min_train_years,
            "evaluated_from": index[span_base + 1].isoformat(),
            "evaluated_to": index[span_last].isoformat(),
            "span_base": span_base,
            "folds": fold_rows,
            "oos_metrics": oos_metrics,
            "hindsight_params": (
                None if hindsight is None else dict(self._result_params[hindsight])
            ),
            "hindsight_metrics": hindsight_metrics,
            "hindsight_minus_oos": (
                None
                if hindsight_metrics is None
                else hindsight_metrics.get(self.metric) - oos_metrics.get(self.metric)
            ),
            "mean_train_minus_oos": float(np.mean(gaps)) if gaps else None,
        }
        return self.walk_forward_report
