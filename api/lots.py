"""
api/lots.py

The smallest quantity a backtest of an asset may trade, resolved from its
asset class (config.settings.lot_size_for): whole shares for equities and
ETFs, 1e-8 for crypto. Every router that runs a backtest (backtest, compare,
optimize, portfolio backtest, simulate, and their websockets) resolves it
here. Until 2026-10-12 all of them traded whole units, so a BTC backtest
whose cash was below one coin bought nothing and reported a flat line.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from config.settings import lot_size_for

__all__ = ["lot_size_or_422"]


def lot_size_or_422(asset: Any) -> float:
    """The asset's lot size; 422 for an asset class with none configured."""
    try:
        return lot_size_for(asset.asset_class)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"{asset.symbol} cannot be backtested: {exc}"
        ) from None
