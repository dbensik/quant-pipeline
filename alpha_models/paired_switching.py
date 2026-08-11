"""
alpha_models/paired_switching.py

Hold whichever of two assets had the better trailing return, reassessed on a
fixed schedule. Classically equities vs bonds (SPY/TLT).

Phase 1 — asset allocation
"""

from __future__ import annotations

import pandas as pd

from .base_model import BaseAlphaModel
from .rebalancing import rebalance_dates


class PairedSwitchingStrategy(BaseAlphaModel):
    """
    Switch between exactly two assets on trailing return.

    OUTPUT SHAPE is `wide_per_asset` (see BaseAlphaModel): one position column
    per input column, not a 'signal' column. Registered accordingly, which is
    what routes it through the wide branch of the portfolio backtest router.

    Signals are emitted ONLY on rebalance dates — +1 for the asset to hold, -1
    for the one to leave, 0 on every other bar. That is what "hold until the
    next rebalance" looks like to PortfolioBacktester: 0 means "no action", so
    the position simply persists. Emitting +1 daily would be almost equivalent
    (a buy into a non-zero position is a no-op) but would move the switch to the
    day the trailing returns crossed rather than the scheduled date, which is a
    different — and untested — strategy.

    SIZING IS THE CALLER'S. Values here are directional, not fractional. The
    backtester sizes a +1 to `weights[symbol]` of total equity, and the API
    defaults `weights` to EQUAL across the requested symbols — so the default
    0.5/0.5 over a pair leaves this strategy 50% in cash at all times, because
    only one leg is ever held. For the intended fully-invested behaviour pass
    `weights={"SPY": 1.0, "TLT": 1.0}`: the weight is what that symbol gets
    WHEN HELD, and the two are mutually exclusive here.
    """

    def __init__(self, lookback: int = 63, rebalance_frequency: str = "QE"):
        """
        Args:
            lookback: Bars in the trailing-return comparison. 63 ≈ one quarter
                      of trading days.
            rebalance_frequency: 'W', 'ME', 'QE' or 'YE'.
        """
        super().__init__()
        if lookback < 2:
            raise ValueError(f"lookback must be at least 2; got {lookback}.")
        self.lookback = int(lookback)
        self.rebalance_frequency = rebalance_frequency

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            price_data: A WIDE frame of close prices, exactly two columns.

        Returns:
            One position column per input column, values in {-1, 0, 1}.
        """
        if price_data.shape[1] != 2:
            raise ValueError(
                "PairedSwitchingStrategy trades exactly two assets; got "
                f"{price_data.shape[1]} ({list(price_data.columns)})."
            )

        positions = pd.DataFrame(
            0.0, index=price_data.index, columns=price_data.columns
        )
        trailing = price_data.pct_change(self.lookback, fill_method=None)

        for date in rebalance_dates(price_data.index, self.rebalance_frequency):
            row = trailing.loc[date]
            # Before the lookback has filled, there is nothing to compare.
            # Staying flat is the honest answer; picking a winner from NaNs
            # would be a coin flip dressed as a decision.
            if row.isna().any():
                continue
            winner = row.idxmax()
            positions.loc[date] = -1.0
            positions.loc[date, winner] = 1.0

        return positions
