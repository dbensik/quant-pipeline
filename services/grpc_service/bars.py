"""
services/grpc_service/bars.py
Daily bars for the signal service, read from TimescaleDB like every other reader.

WHY NOT YFINANCE. Until 2026-10-14 the service downloaded prices from Yahoo by
the bare symbol it was asked for. That skipped every guard the pipeline has:
read-time adjustment, the crypto identity audit and provider-ticker map, the
reassignment checks. `verify_all.py` asked for "BTC", which on Yahoo is the
Grayscale Bitcoin Mini Trust ETF ($36.09 on 2026-10-14, against Bitcoin's
$81,479), so the signal it verified was for an ETF. A symbol the registry does not hold now has no bars, and
the service answers NOT_FOUND.

WHY A LOOP THREAD AND ITS OWN ENGINE. gRPC servicer methods are synchronous;
the repository and its read-time adjustment are async. Pooled asyncpg
connections bind to the event loop that opened them (see
services/execution_service/portfolio_store.py), so the reader owns ONE
long-lived loop on a background thread and an engine created on it, never
db/session.py's module-level one, which another loop in the process may
already hold. The prices are exactly the ones `fetch_range` serves the API.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import pandas as pd

from api.frames import records_to_frame

#: Calendar days of bars before `as_of`: enough for a 20-bar rolling window
#: over weekends and holidays (the window the service has always used).
LOOKBACK_DAYS = 200


class BarReader:
    """Total-return daily bars for one symbol, on or before a date."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, name="bar-reader", daemon=True).start()
        self._sessions = None  # created on the loop, at first use

    async def _fetch(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        from db.repositories.market_data import TimescaleMarketDataRepo
        from db.session import settings

        if self._sessions is None:
            engine = create_async_engine(settings.DATABASE_URL, pool_size=2, pool_pre_ping=True)
            self._sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with self._sessions() as session:
            records = await TimescaleMarketDataRepo(session).fetch_range(
                symbol=symbol, asset_class=None, start=start, end=end, adjust="total"
            )
        return records_to_frame(records)

    def frame(self, symbol: str, as_of: Optional[date] = None) -> pd.DataFrame:
        """Bars from LOOKBACK_DAYS before `as_of` (default today, UTC) through it."""
        day = as_of or datetime.now(timezone.utc).date()
        end = datetime.combine(day, time.max, tzinfo=timezone.utc)
        start = end - timedelta(days=LOOKBACK_DAYS)
        return asyncio.run_coroutine_threadsafe(
            self._fetch(symbol, start, end), self._loop
        ).result()
