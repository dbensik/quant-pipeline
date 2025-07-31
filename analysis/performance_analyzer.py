from itertools import groupby
from typing import Dict, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter


class PerformanceAnalyzer:
    """
    A dedicated class for calculating performance metrics and generating visual tearsheets
    from a portfolio history DataFrame. It is decoupled from the backtesting engine.
    """

    def __init__(
        self,
        portfolio_history: pd.DataFrame,
        benchmark_history: Optional[pd.DataFrame] = None,
        initial_capital: float = 100000.0,
    ):
        """
        Initializes the analyzer with the results of a backtest.

        Args:
            portfolio_history: A DataFrame with 'total', 'returns', and 'trades' columns.
            benchmark_history: An optional DataFrame for a benchmark, with a 'total' column.
            initial_capital: The starting capital for calculating total return.
        """
        if portfolio_history is None or portfolio_history.empty:
            raise ValueError("Portfolio history DataFrame cannot be None or empty.")

        self.portfolio = portfolio_history
        self.benchmark = benchmark_history
        self.initial_capital = initial_capital
        self.returns = self.portfolio["returns"]
        self.portfolio_value = self.portfolio["total"]

    def calculate_all_metrics(self) -> Dict[str, float]:
        """
        Calculates and returns a dictionary of all key performance metrics.
        This is the single source of truth for summary statistics.
        """
        # --- Core Metrics ---
        final_value = self.portfolio_value.iloc[-1]
        total_return = (final_value / self.initial_capital) - 1.0
        trades = self.portfolio.get("trades", pd.Series(0, index=self.portfolio.index))
        trade_count = (trades != 0).sum()

        # --- Time-Based Metrics ---
        days = (self.portfolio_value.index[-1] - self.portfolio_value.index[0]).days
        annualized_return = (
            (1 + total_return) ** (365.0 / days) - 1 if days > 0 else 0.0
        )

        # --- Risk & Risk-Adjusted Return Metrics ---
        annualized_volatility = self.returns.std() * np.sqrt(252)
        sharpe_ratio = (
            annualized_return / annualized_volatility
            if annualized_volatility != 0
            else 0.0
        )

        downside_returns = self.returns[self.returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252)
        sortino_ratio = annualized_return / downside_std if downside_std != 0 else 0.0

        _series, max_drawdown, max_drawdown_duration = self._calculate_drawdowns()

        return {
            "Final Value": final_value,
            "Total Return": total_return,
            "Annualized Return": annualized_return,
            "Annualized Volatility": annualized_volatility,
            "Sharpe Ratio": sharpe_ratio,
            "Sortino Ratio": sortino_ratio,
            "Max Drawdown": max_drawdown,
            "Max Drawdown Duration (Days)": max_drawdown_duration,
            "Trade Count": trade_count,
        }

    def _calculate_drawdowns(self) -> Tuple[pd.Series, float, int]:
        """
        Calculates the drawdown series, max drawdown, and the duration of the longest drawdown.
        """
        cumulative_max = self.portfolio_value.cummax()
        drawdown = (self.portfolio_value - cumulative_max) / cumulative_max
        max_drawdown = drawdown.min() if not drawdown.empty else 0.0

        duration_check = np.where(drawdown < 0, 1, 0)
        max_drawdown_duration = 0
        if np.any(duration_check):
            max_drawdown_duration = max(
                sum(1 for _ in g) for k, g in groupby(duration_check) if k == 1
            )

        return drawdown, max_drawdown, max_drawdown_duration

    def get_aggregated_returns(self, period: str) -> pd.Series:
        """
        Aggregates returns by week, month, or year.
        """

        def cumulate_returns(x):
            return np.expm1(np.log1p(x).sum())

        if period == "weekly":
            return self.returns.groupby(self.returns.index.to_period("W")).apply(
                cumulate_returns
            )
        elif period == "monthly":
            return self.returns.groupby(self.returns.index.to_period("M")).apply(
                cumulate_returns
            )
        elif period == "yearly":
            return self.returns.groupby(self.returns.index.to_period("Y")).apply(
                cumulate_returns
            )
        else:
            raise ValueError("Period must be one of 'weekly', 'monthly', or 'yearly'")

    # --- TEARSHEET GENERATION ---

    def generate_tearsheet(self, title: str = "Strategy Performance") -> plt.Figure:
        """
        Generates a full performance tearsheet and returns it as a Matplotlib figure.
        The caller is responsible for displaying or saving the figure.
        """
        sns.set_style("whitegrid")
        fig = plt.figure(figsize=(16, 12))
        fig.suptitle(title, y=0.94, weight="bold", fontsize=14)
        gs = gridspec.GridSpec(5, 3, wspace=0.25, hspace=0.7)

        # Prepare stats for both strategy and benchmark
        strat_stats = self.calculate_all_metrics()
        strat_stats["drawdown_series"], _, _ = self._calculate_drawdowns()
        strat_stats["cum_returns"] = (1 + self.returns).cumprod()

        bench_stats = None
        if self.benchmark is not None and not self.benchmark.empty:
            # Use a separate analyzer instance for the benchmark
            bench_analyzer = PerformanceAnalyzer(self.benchmark, self.initial_capital)
            bench_stats = bench_analyzer.calculate_all_metrics()
            bench_stats["drawdown_series"], _, _ = bench_analyzer._calculate_drawdowns()
            bench_stats["cum_returns"] = (1 + bench_analyzer.returns).cumprod()

        # Create plots
        self._plot_equity(strat_stats, bench_stats, ax=plt.subplot(gs[:2, :]))
        self._plot_drawdown(strat_stats, ax=plt.subplot(gs[2, :]))
        self._plot_monthly_returns(ax=plt.subplot(gs[3, :2]))
        self._plot_yearly_returns(ax=plt.subplot(gs[3, 2]))
        self._plot_stats_table(strat_stats, bench_stats, ax=plt.subplot(gs[4, :]))

        plt.close(fig)  # Prevent immediate display in some environments
        return fig

    def _plot_equity(self, strat_stats, bench_stats, ax):
        """Plots cumulative rolling returns for strategy and benchmark."""
        ax.set_title("Cumulative Returns", fontweight="bold")
        ax.plot(
            strat_stats["cum_returns"],
            lw=2,
            color="green",
            alpha=0.8,
            label="Strategy",
        )
        if bench_stats:
            ax.plot(
                bench_stats["cum_returns"],
                lw=2,
                color="gray",
                alpha=0.8,
                label="Benchmark",
            )

        ax.axhline(1.0, linestyle="--", color="black", lw=1)
        ax.set_ylabel("Cumulative Returns")
        ax.legend(loc="best")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    def _plot_drawdown(self, strat_stats, ax):
        """Plots the underwater curve."""
        ax.set_title("Drawdown (%)", fontweight="bold")
        underwater = -100 * strat_stats["drawdown_series"]
        ax.fill_between(underwater.index, underwater, color="red", alpha=0.3)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:.0f}%"))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    def _plot_monthly_returns(self, ax):
        """Plots a heatmap of the monthly returns."""
        monthly_ret = self.get_aggregated_returns("monthly").unstack()
        monthly_ret = np.round(monthly_ret, 3)
        monthly_ret.rename(
            columns={
                1: "Jan",
                2: "Feb",
                3: "Mar",
                4: "Apr",
                5: "May",
                6: "Jun",
                7: "Jul",
                8: "Aug",
                9: "Sep",
                10: "Oct",
                11: "Nov",
                12: "Dec",
            },
            inplace=True,
        )

        sns.heatmap(
            monthly_ret.fillna(0) * 100.0,
            annot=True,
            fmt=".1f",
            annot_kws={"size": 9},
            cmap="RdYlGn",
            ax=ax,
            cbar=False,
            center=0.0,
        )
        ax.set_title("Monthly Returns (%)", fontweight="bold")
        ax.set_ylabel("")
        ax.set_xlabel("")

    def _plot_yearly_returns(self, ax):
        """Plots a barplot of returns by year."""
        ax.set_title("Yearly Returns (%)", fontweight="bold")
        yly_ret = self.get_aggregated_returns("yearly") * 100.0
        yly_ret.plot(ax=ax, kind="bar")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:.0f}%"))
        ax.set_xticklabels(yly_ret.index, rotation=45, ha="right")
        ax.set_ylabel("")
        ax.set_xlabel("")

    def _plot_stats_table(self, strat_stats, bench_stats, ax):
        """Creates a text-based table of key performance statistics."""
        ax.set_title("Key Performance Metrics", fontweight="bold")
        ax.grid(False)
        ax.spines["top"].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.get_yaxis().set_visible(False)
        ax.get_xaxis().set_visible(False)

        stats_data = [
            ("Total Return", f"{strat_stats['Total Return']:.2%}"),
            ("Annualized Return", f"{strat_stats['Annualized Return']:.2%}"),
            ("Annualized Volatility", f"{strat_stats['Annualized Volatility']:.2%}"),
            ("Sharpe Ratio", f"{strat_stats['Sharpe Ratio']:.2f}"),
            ("Sortino Ratio", f"{strat_stats['Sortino Ratio']:.2f}"),
            ("Max Drawdown", f"{strat_stats['Max Drawdown']:.2%}"),
            (
                "Max Drawdown Duration",
                f"{strat_stats['Max Drawdown Duration (Days)']:.0f} days",
            ),
        ]

        bench_data = []
        if bench_stats:
            bench_data = [
                f"{bench_stats['Total Return']:.2%}",
                f"{bench_stats['Annualized Return']:.2%}",
                f"{bench_stats['Annualized Volatility']:.2%}",
                f"{bench_stats['Sharpe Ratio']:.2f}",
                f"{bench_stats['Sortino Ratio']:.2f}",
                f"{bench_stats['Max Drawdown']:.2%}",
                f"{bench_stats['Max Drawdown Duration (Days)']:.0f} days",
            ]

        y_pos = 0.9
        for i, (metric, value) in enumerate(stats_data):
            ax.text(0.01, y_pos, metric, fontsize=10)
            ax.text(0.45, y_pos, value, fontsize=10, fontweight="bold", ha="right")
            if bench_stats:
                ax.text(
                    0.75,
                    y_pos,
                    bench_data[i],
                    fontsize=10,
                    fontweight="bold",
                    ha="right",
                )
            y_pos -= 0.12

        ax.text(0.45, 0.98, "Strategy", fontsize=11, fontweight="bold", ha="right")
        if bench_stats:
            ax.text(0.75, 0.98, "Benchmark", fontsize=11, fontweight="bold", ha="right")
        ax.axis([0, 1, 0, 1])
