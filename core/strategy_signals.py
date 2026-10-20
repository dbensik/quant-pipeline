"""
core/strategy_signals.py

Turns a registered strategy, its model and a basket's OHLCV frames into the
per-ticker signal frames PortfolioBacktester reads. One function for both
callers: the portfolio backtest router and the paper trader (decision 50,
research/paper-trader-2026-10-20.md), so a paper portfolio follows exactly
the signals its backtest traded.

Moved here from api/routers/portfolio_backtest.py on 2026-10-20, unchanged.
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def wide_close(price_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """{symbol: ohlcv_frame} -> one Close column per symbol (api.frames.frames_to_wide_close)."""
    if not price_data:
        return pd.DataFrame()
    return pd.DataFrame(
        {symbol: frame["Close"] for symbol, frame in price_data.items() if not frame.empty}
    ).sort_index()


def build_signals(
    spec: Any,
    model: Any,
    price_data: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """
    Multi-asset strategies do NOT share one output shape, so signals are
    assembled per shape — ported from the Streamlit AnalysisController, which
    dispatched on isinstance.

    Dispatch is on `spec.signal_shape`, declared in the registry. It used to be
    hardcoded sets of strategy ids in the router, which meant a new multi-asset
    strategy could not work without editing it — and worse, failed
    UNSAFELY: an unlisted id fell through to the per-symbol branch and was
    handed each symbol's own frame in isolation, so a cross-asset strategy
    would silently compare nothing and still return plausible numbers.
    """
    shape = spec.signal_shape

    if shape == "wide_per_asset":
        # Wide frame in, one position column per asset out. Split into the
        # per-ticker dict the PortfolioBacktester consumes.
        wide = wide_close(price_data).dropna()
        signals = model.generate_signals(wide)
        return {
            column: signals[[column]].rename(columns={column: "signal"})
            for column in signals.columns
        }

    if shape == "target":
        # Wide frame in, one target column per asset out (decision 48).
        wide = wide_close(price_data).dropna()
        targets = model.generate_signals(wide)
        return {
            column: targets[[column]].rename(columns={column: "target"})
            for column in targets.columns
        }

    if shape == "wide_portfolio":
        # Wide frame in, one 'signal' column for the basket as a single unit.
        wide = wide_close(price_data).dropna()
        return {"Portfolio": model.generate_signals(wide)}

    if shape == "calendar_shared":
        # Rebalance schedules depend only on the calendar, so any constituent's
        # frame gives the dates; every symbol then shares them.
        any_frame = next(iter(price_data.values()))
        rebalance = model.generate_signals(any_frame)
        return {symbol: rebalance for symbol in price_data}

    # per_symbol: each symbol's own frame produces its own signal.
    #
    # The portfolio backtest route 422s anything whose input_contract is not
    # "multi" before getting here, and
    # test_every_registered_strategy_declares_a_wired_shape forbids a
    # multi-asset spec from declaring per_symbol. The paper trader reaches it
    # for a single-asset strategy on its one symbol.
    return {symbol: model.generate_signals(frame) for symbol, frame in price_data.items()}
