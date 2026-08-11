"""
alpha_models/asset_class_trend.py

Each sleeve is held while it trades above its own long moving average, and left
in cash otherwise. The classic tactical-allocation rule, applied per asset class
rather than to a single instrument.

Phase 1 — asset allocation
"""

from __future__ import annotations

import pandas as pd

from .base_model import BaseAlphaModel
from .rebalancing import rebalance_dates


class AssetClassTrendFollowingStrategy(BaseAlphaModel):
    """
    Hold each sleeve while price > its own SMA; otherwise hold none of it.

    OUTPUT SHAPE is `wide_per_asset`: one position column per input column.

    Sleeves are judged INDEPENDENTLY — this is not a ranking. Every sleeve above
    its average is held, so the strategy is fully invested in a broad uptrend
    and entirely in cash when everything is below trend. That "all cash" state
    is the whole point of the rule and the reason it is judged on drawdown
    rather than return.

    The canonical formulation is a 10-MONTH average on monthly bars. On daily
    bars the equivalent is ~200 trading days, which is the default here; the
    rebalance schedule stays monthly so the decision is still made once a month.

    SIZING IS THE CALLER'S, and for this strategy the API's equal-weight default
    is CORRECT: with N sleeves each gets 1/N when held, so being in three of five
    means 60% invested and 40% cash. That is the intended behaviour, unlike
    paired switching where the default under-invests.
    """

    def __init__(self, window: int = 200, rebalance_frequency: str = "ME"):
        """
        Args:
            window: Bars in each sleeve's moving average. 200 ≈ the 10-month
                    average of the monthly-bar original.
            rebalance_frequency: 'W', 'ME', 'QE' or 'YE'.
        """
        super().__init__()
        if window < 2:
            raise ValueError(f"window must be at least 2; got {window}.")
        self.window = int(window)
        self.rebalance_frequency = rebalance_frequency

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            price_data: A WIDE frame of close prices, one column per sleeve.

        Returns:
            One position column per input column, values in {-1, 0, 1}.
        """
        if price_data.shape[1] < 2:
            raise ValueError(
                "AssetClassTrendFollowingStrategy needs at least two sleeves; "
                f"got {price_data.shape[1]}."
            )

        positions = pd.DataFrame(
            0.0, index=price_data.index, columns=price_data.columns
        )
        # min_periods=window: a partially-filled average would compare price to
        # a mean of ten bars and call it a 200-day trend.
        sma = price_data.rolling(self.window, min_periods=self.window).mean()

        for date in rebalance_dates(price_data.index, self.rebalance_frequency):
            price_row = price_data.loc[date]
            sma_row = sma.loc[date]
            usable = sma_row.notna() & price_row.notna()
            if not usable.any():
                continue
            above = (price_row > sma_row) & usable
            # +1 hold, -1 exit. Sleeves still warming up stay 0 (no action)
            # rather than being force-sold on missing information.
            positions.loc[date, usable & above] = 1.0
            positions.loc[date, usable & ~above] = -1.0

        return positions
