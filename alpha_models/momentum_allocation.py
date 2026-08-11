"""
alpha_models/momentum_allocation.py

Hold the strongest N sleeves by trailing return, equally weighted, reassessed on
a fixed schedule. Cross-sectional momentum across asset classes.

Phase 1 — asset allocation
"""

from __future__ import annotations

import pandas as pd

from .base_model import BaseAlphaModel
from .rebalancing import rebalance_dates


class MomentumAssetAllocationStrategy(BaseAlphaModel):
    """
    Rank sleeves by trailing return; hold the top `top_n`.

    OUTPUT SHAPE is `wide_per_asset`: one position column per input column.

    This IS a ranking, which is what separates it from asset-class trend
    following: it always holds exactly `top_n` sleeves, including in a broad
    selloff where every trailing return is negative. It rotates rather than
    de-risks. Pair it with a trend filter if you want the cash state.

    A note on what this is NOT. Ranking a handful of ETFs against each other is
    cross-sectional, but it is not the cross-sectional EQUITY factor work that
    the repo assessment lists as blocked. That is blocked on point-in-time index
    membership; a fixed sleeve list has no membership question — these ETFs are
    the universe, and they do not enter or leave it.

    SIZING IS THE CALLER'S. With `top_n` of N sleeves held at 1/N each, the
    API's equal-weight default leaves (N - top_n)/N in cash. To be fully
    invested pass `weights` of 1/top_n per symbol.
    """

    def __init__(
        self, lookback: int = 126, top_n: int = 2, rebalance_frequency: str = "ME"
    ):
        """
        Args:
            lookback: Bars in the trailing-return ranking. 126 ≈ six months.
            top_n: How many sleeves to hold.
            rebalance_frequency: 'W', 'ME', 'QE' or 'YE'.
        """
        super().__init__()
        if lookback < 2:
            raise ValueError(f"lookback must be at least 2; got {lookback}.")
        if top_n < 1:
            raise ValueError(f"top_n must be at least 1; got {top_n}.")
        self.lookback = int(lookback)
        self.top_n = int(top_n)
        self.rebalance_frequency = rebalance_frequency

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            price_data: A WIDE frame of close prices, one column per sleeve.

        Returns:
            One position column per input column, values in {-1, 0, 1}.
        """
        columns = price_data.shape[1]
        if columns < 2:
            raise ValueError(
                f"MomentumAssetAllocationStrategy needs at least two sleeves; got {columns}."
            )
        if self.top_n >= columns:
            raise ValueError(
                f"top_n ({self.top_n}) must be fewer than the {columns} sleeves "
                "supplied, otherwise every sleeve is always held and the "
                "ranking does nothing."
            )

        positions = pd.DataFrame(
            0.0, index=price_data.index, columns=price_data.columns
        )
        trailing = price_data.pct_change(self.lookback, fill_method=None)

        for date in rebalance_dates(price_data.index, self.rebalance_frequency):
            row = trailing.loc[date].dropna()
            # Ranking a partial set would compare sleeves with a filled lookback
            # against sleeves without one, which is not a ranking.
            if len(row) < columns:
                continue
            winners = row.nlargest(self.top_n).index
            positions.loc[date] = -1.0
            positions.loc[date, winners] = 1.0

        return positions
