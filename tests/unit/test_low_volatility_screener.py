"""
LowVolatilityScreener (decision 43, research/low-volatility-screener-2026-10-19.md).

It read a precomputed `volatility_90d` column nothing has produced since the
TimescaleDB cutover, so every screen returned an empty list. Each test below
fails against that version: it is given OHLCV frames, as the API gives it.
"""

import numpy as np
import pandas as pd
import pytest

from screeners.low_volatility_screener import LowVolatilityScreener, realised_volatility

DAILY = {"CALM": 0.005, "MILD": 0.01, "WILD": 0.02, "WILDER": 0.04}


def _frame(daily_vol: float, bars: int = 200, seed: int = 0, calendar: str = "B") -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=bars, freq=calendar, tz="UTC")
    rng = np.random.default_rng(seed)
    close = 100 * np.cumprod(1 + rng.normal(0, daily_vol, bars))
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1e6}, index=index
    )


def _universe():
    return {sym: _frame(vol, seed=i) for i, (sym, vol) in enumerate(DAILY.items())}


def test_keeps_the_bottom_quantile_by_realised_volatility():
    data = _universe()
    kept = LowVolatilityScreener(quantile=0.5).screen(list(data), data)
    assert sorted(kept) == ["CALM", "MILD"]


def test_quantile_one_keeps_every_name_it_can_measure():
    data = _universe()
    assert sorted(LowVolatilityScreener(quantile=1.0).screen(list(data), data)) == sorted(DAILY)


def test_a_name_with_too_few_returns_is_not_ranked():
    data = _universe()
    data["YOUNG"] = _frame(0.001, bars=50)
    kept = LowVolatilityScreener(quantile=1.0, window=90).screen(list(data), data)
    assert "YOUNG" not in kept
    assert len(kept) == 4


def test_volatility_is_annualized_on_the_series_calendar():
    business = _frame(0.01, bars=300, seed=3)
    every_day = _frame(0.01, bars=300, seed=3, calendar="D")
    ratio = realised_volatility(every_day["Close"], 90) / realised_volatility(business["Close"], 90)
    assert ratio == pytest.approx(np.sqrt(365 / 252))
    assert realised_volatility(business["Close"], 90) == pytest.approx(
        business["Close"].pct_change().iloc[-90:].std() * np.sqrt(252)
    )


def test_the_window_ends_at_the_last_bar():
    # Calm for 150 bars, then wild: a 30-bar window sees only the wild end.
    calm = _frame(0.002, bars=150, seed=1)["Close"].to_numpy()
    wild = calm[-1] * np.cumprod(1 + np.random.default_rng(2).normal(0, 0.05, 50))
    close = pd.Series(
        np.concatenate([calm, wild]), index=pd.bdate_range("2024-01-01", periods=200, tz="UTC")
    )
    assert realised_volatility(close, 30) > 0.5
    assert realised_volatility(close.iloc[:150], 30) < 0.1
