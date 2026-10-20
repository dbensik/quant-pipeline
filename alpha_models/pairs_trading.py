"""
alpha_models/pairs_trading.py

Long/short pairs trading on a cointegrated spread (rebuilt 2026-10-19,
decision 48, research/short-engine-2026-10-19.md).

The version it replaces never held its short leg: it emitted -1/+1 events,
which PortfolioBacktester reads as exit/enter, so the short leg was ignored at
flat and BOUGHT when the spread trade closed; it wrote 0.0 where it meant "no
change", so it held only while |z| was beyond the threshold (one day, mostly);
and it traded a raw price ratio it never tested.

This one returns TARGET positions (signal_shape "target"), a signed fraction
of equity per leg, NaN meaning unchanged:

- Every `refit` bars it fits A = alpha + beta x B by OLS on the trailing
  `formation` closes, and tests that window for cointegration
  (Engle-Granger). Only that window is read: nothing after the bar.
- Between refits the fit is fixed, and z is the residual over the
  residual's standard deviation in the window.
- Long the spread (long A, short B) when z < -entry, short it when
  z > +entry; out when z crosses back past `exit` (0 = the fitted mean), or
  at a stop when |z| passes `stop`. After a stop it re-enters only once |z|
  has come back inside `entry`.
- A refit that finds no cointegration (p >= max_pvalue), or a hedge ratio
  of 0 or below, closes any position and trades nothing until a refit finds
  one again.
- Legs are dollar-neutral, LEG of equity each (gross 1x). The hedge ratio
  defines the spread, not the sizes.
"""

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint

from .base_model import BaseAlphaModel

#: Fraction of equity in each leg: half long, half short.
LEG = 0.5


class PairsTradingStrategy(BaseAlphaModel):
    def __init__(
        self,
        formation: int = 252,
        refit: int = 21,
        entry: float = 2.0,
        exit: float = 0.0,
        stop: float = 4.0,
        max_pvalue: float = 0.05,
    ):
        super().__init__()
        if formation < 30:
            raise ValueError("formation must be at least 30 bars for a cointegration test")
        if refit < 1:
            raise ValueError("refit must be at least 1 bar")
        if not 0 <= exit < entry < stop:
            raise ValueError("Need 0 <= exit < entry < stop")
        if not 0 < max_pvalue < 1:
            raise ValueError("max_pvalue must be between 0 and 1")
        self.formation = formation
        self.refit = refit
        self.entry = entry
        self.exit = exit
        self.stop = stop
        self.max_pvalue = max_pvalue

    def fit(self, a: np.ndarray, b: np.ndarray):
        """(alpha, beta, residual std) for a formation window, or None if it is not a pair."""
        if np.isnan(a).any() or np.isnan(b).any() or np.ptp(b) == 0:
            return None
        beta, alpha = np.polyfit(b, a, 1)
        resid = a - alpha - beta * b
        sd = resid.std(ddof=1)
        if beta <= 0 or not sd > 0:
            return None
        if coint(a, b)[1] >= self.max_pvalue:
            return None
        return alpha, beta, sd

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        if len(price_data.columns) != 2:
            raise ValueError(
                "PairsTradingStrategy requires a DataFrame with exactly two price columns."
            )
        col_a, col_b = price_data.columns
        a = price_data[col_a].to_numpy(float)
        b = price_data[col_b].to_numpy(float)
        n = len(price_data)
        out = np.full((n, 2), np.nan)

        state = 0  # +1 long the spread, -1 short it
        stopped = False
        params = None
        for t in range(self.formation - 1, n):
            new = state
            if (t - self.formation + 1) % self.refit == 0:
                lo = t - self.formation + 1
                params = self.fit(a[lo : t + 1], b[lo : t + 1])
                if params is None:
                    new, stopped = 0, False
            if params is not None and not (np.isnan(a[t]) or np.isnan(b[t])):
                alpha, beta, sd = params
                z = (a[t] - alpha - beta * b[t]) / sd
                if stopped and abs(z) < self.entry:
                    stopped = False
                if new == 1 and (z >= -self.exit or z < -self.stop):
                    stopped = z < -self.stop
                    new = 0
                elif new == -1 and (z <= self.exit or z > self.stop):
                    stopped = z > self.stop
                    new = 0
                # Never enter beyond the stop: it would be stopped out next bar.
                if new == 0 and not stopped:
                    if -self.stop <= z < -self.entry:
                        new = 1
                    elif self.entry < z <= self.stop:
                        new = -1
            if new != state:
                out[t] = (LEG * new, -LEG * new)
                state = new
        return pd.DataFrame(out, index=price_data.index, columns=[col_a, col_b])
