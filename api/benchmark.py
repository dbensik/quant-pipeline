"""
api/benchmark.py

The benchmark report every backtest response carries (decision 53,
research/benchmark-regime-report-2026-10-20.md): its response model, which
benchmark a run is measured against, and loading its bars. The arithmetic is
analysis/benchmark_report.py.

/backtest (and its websocket), /backtest/compare and /backtest/portfolio use
it. The default benchmark (config: SPY, BTC-USD for an all-crypto run) is
optional: without bars the report is left out and `benchmark_unavailable`
says why. A benchmark the request NAMES is 404 or 422, like any symbol.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from fastapi import HTTPException
from pydantic import BaseModel, Field

from analysis.benchmark_report import BenchmarkReport, benchmark_report, default_benchmark
from api.frames import records_to_frame
from config import settings

BENCHMARK_SYMBOL_DESCRIPTION = (
    "Symbol the benchmark report regresses the run on. Default: "
    f"{settings.BENCHMARK_DEFAULT}, or the class's own for a run of one class "
    f"listed in BENCHMARK_BY_ASSET_CLASS ({settings.BENCHMARK_BY_ASSET_CLASS})."
)


class RollingSharpePoint(BaseModel):
    time: datetime
    strategy: Optional[float]
    benchmark: Optional[float]


class RegimeOut(BaseModel):
    name: str = Field(description="low, middle or high third of the benchmark's volatility")
    days: int
    share: float
    strategy_return: Optional[float] = Field(description="Mean excess return a bar, annualized")
    benchmark_return: Optional[float]
    strategy_sharpe: Optional[float]
    benchmark_sharpe: Optional[float]
    strategy_sharpe_error: Optional[float] = Field(description="Standard error of the Sharpe")
    benchmark_sharpe_error: Optional[float]


class BenchmarkReportOut(BaseModel):
    symbol: str
    start: datetime
    end: datetime
    bars: int = Field(description="Daily returns in the regression")
    periods_per_year: int
    beta: float
    alpha: float = Field(description="Annualized, in excess of cash")
    alpha_t: Optional[float] = Field(
        description="Newey-West t-statistic of alpha; null when the run's excess return never"
        " varies (it held nothing)"
    )
    alpha_low: float = Field(description="95% interval, annualized")
    alpha_high: float
    r_squared: Optional[float] = Field(description="Null when alpha_t is")
    rolling_window: int
    rolling_sharpe: List[RollingSharpePoint] = Field(default_factory=list)
    regimes: List[RegimeOut] = Field(default_factory=list)
    unranked_days: int = Field(
        description="Days with too little earlier benchmark history to be given a regime"
    )


def to_out(report: BenchmarkReport) -> BenchmarkReportOut:
    return BenchmarkReportOut(
        symbol=report.symbol,
        start=report.start.to_pydatetime(),
        end=report.end.to_pydatetime(),
        bars=report.bars,
        periods_per_year=report.periods_per_year,
        beta=report.beta,
        alpha=report.alpha,
        alpha_t=report.alpha_t,
        alpha_low=report.alpha_low,
        alpha_high=report.alpha_high,
        r_squared=report.r_squared,
        rolling_window=report.rolling_window,
        rolling_sharpe=[
            RollingSharpePoint(time=t.to_pydatetime(), strategy=s, benchmark=b)
            for t, s, b in report.rolling_sharpe
        ],
        regimes=[RegimeOut(**vars(r)) for r in report.regimes],
        unranked_days=report.unranked_days,
    )


async def load_benchmark(
    repo, named: Optional[str], asset_classes: Iterable[str], start: datetime, end: datetime
) -> Tuple[Optional[Tuple[str, pd.Series]], Optional[str]]:
    """
    ((symbol, adjusted closes from BENCHMARK_HISTORY_DAYS before `start`),
    None), or (None, why not) for a DEFAULT benchmark with no bars. A named
    one that is unknown is 404, one with no bars 422.
    """
    symbol = named.upper() if named else default_benchmark(asset_classes)
    asset = await repo.find_asset(symbol)
    if asset is None:
        if named:
            raise HTTPException(status_code=404, detail=f"Unknown benchmark symbol: {named!r}")
        return None, f"The default benchmark {symbol} is not stored."
    records = await repo.fetch_range(
        symbol=asset.symbol,
        asset_class=None,
        start=start - timedelta(days=settings.BENCHMARK_HISTORY_DAYS),
        end=end,
    )
    frame = records_to_frame(records)
    if frame.empty:
        why = f"No bars stored for benchmark {asset.symbol!r} up to {end.date()}."
        if named:
            raise HTTPException(status_code=422, detail=why)
        return None, why
    return (asset.symbol, frame["Close"]), None


def report_or_reason(
    loaded: Optional[Tuple[str, pd.Series]],
    reason: Optional[str],
    total: pd.Series,
    accrual: pd.Series,
) -> Tuple[Optional[BenchmarkReportOut], Optional[str]]:
    """The report for one equity curve, or why there is none."""
    if loaded is None:
        return None, reason
    if total is None or total.empty:
        return None, "The run produced no equity curve."
    symbol, closes = loaded
    try:
        return to_out(benchmark_report(total, accrual, closes, symbol)), None
    except ValueError as exc:
        return None, str(exc)
