"""
core/paper_history.py

Loads the MarketHistory a paper portfolio's state is derived with (decision
49, research/paper-shorts-2026-10-19.md): each ticker's corporate actions,
its RAW daily closes (the borrow base), and ^IRX as a bond-equivalent rate.

The one loader for both write paths. The REST router passes its request's
repositories; the gRPC store opens its own session (portfolio_store.py),
because db/session.py's async engine binds to one event loop.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, Iterable, Optional, Tuple

from config import settings
from core.portfolio import MARKET_TZ, MarketHistory
from core.rates import discount_to_bond_equivalent


def new_york_today(now: Optional[datetime] = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(MARKET_TZ).date()


async def load_history(
    tickers: Iterable[str],
    first_day: date,
    as_of: date,
    market,
    rates,
    *,
    cash_from: Optional[date] = None,
) -> MarketHistory:
    """
    `market` is a MarketDataRepository with corporate_actions(); `rates` a
    RateRepository. Closes are read from a week before `first_day`, so the
    first night of a short has a previous close. `cash_from` is the
    portfolio's `metadata.cash_from` (MarketHistory.start).
    """
    tickers = sorted(set(tickers))
    if cash_from is not None:
        first_day = min(first_day, cash_from)
    start = datetime.combine(first_day - timedelta(days=7), time(), tzinfo=timezone.utc)
    end = datetime.combine(as_of, time(23, 59), tzinfo=timezone.utc)
    actions, closes = {}, {}
    for ticker in tickers:
        actions[ticker] = await market.corporate_actions(ticker)
        records = await market.fetch_range(
            symbol=ticker, asset_class=None, start=start, end=end, adjust="none"
        )
        closes[ticker] = [
            (r.ohlcv.timestamp.utc.date(), float(r.ohlcv.close))
            for r in records
            if r.ohlcv.close is not None
        ]
    observations = await rates.observations_between(
        settings.RISK_FREE_RATE_SERIES,
        first_day - timedelta(days=settings.RISK_FREE_RATE_MAX_AGE_DAYS),
        as_of,
    )
    return MarketHistory(
        actions=actions,
        closes=closes,
        rates=[(o.obs_date, discount_to_bond_equivalent(o.value)) for o in observations],
        as_of=as_of,
        max_rate_age_days=settings.RISK_FREE_RATE_MAX_AGE_DAYS,
        start=cash_from,
    )


async def latest_prices(
    tickers: Iterable[str], market
) -> Tuple[Dict[str, float], Optional[datetime]]:
    """
    Most recent stored close per ticker, and the newest bar's time.

    From the database, NOT yfinance: a portfolio read must not acquire a
    network dependency. Read-time adjusted, so the latest bar is the price as
    it traded. The REST router and the gRPC store both value through this.
    """
    prices: Dict[str, float] = {}
    latest: Optional[datetime] = None
    for ticker in tickers:
        if await market.find_asset(ticker) is None:
            continue
        records = await market.fetch_range(
            symbol=ticker,
            asset_class=None,
            start=datetime(1970, 1, 1, tzinfo=timezone.utc),
            end=datetime.now(timezone.utc),
        )
        if not records:
            continue
        last = max(records, key=lambda r: r.ohlcv.timestamp.utc)
        if last.ohlcv.close is None:
            continue
        prices[ticker] = float(last.ohlcv.close)
        if latest is None or last.ohlcv.timestamp.utc > latest:
            latest = last.ohlcv.timestamp.utc
    return prices, latest


async def asset_classes(tickers: Iterable[str], market) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for ticker in tickers:
        asset = await market.find_asset(ticker)
        out[ticker] = None if asset is None else asset.asset_class
    return out
