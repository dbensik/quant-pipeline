"""
services/execution_service/portfolio_store.py
Synchronous database access for the gRPC execution service.

WHY A SECOND ACCESS PATH EXISTS. gRPC servicer methods are synchronous, and
db/session.py's engine is async and module-level — its pooled asyncpg
connections bind to whichever event loop first uses them. Calling
`asyncio.run()` from a servicer opens a second loop and asyncpg rejects it
("got result for unknown protocol state"). So this uses a SYNC engine over
psycopg2 instead of bridging loops.

MARKET CONTEXT (decision 49). Splits, dividends, interest, borrow and the
buying-power check need corporate actions, raw closes and ^IRX, which only
the async repositories read. `_market_context` runs them under asyncio.run on
an engine of its OWN (NullPool, disposed after), never db/session.py's
module-level one, so no connection outlives its loop.

It does NOT duplicate the accounting. Positions, cash and P&L come from
`core.portfolio.derive_state`, exactly as the REST router's do, so the signed
gRPC path and the API can never disagree about what a portfolio holds.

WHAT THIS REPLACES. The service previously used
`services/execution_service/portfolio_manager.py`, which read `portfolios.json`
— a file that stored two incompatible shapes and that nothing else has read
since portfolios moved to the database in 0003. `GetPortfolio` did
`state["cash"]` on a trade-log-shaped portfolio and raised KeyError against
real data; the paper portfolio view had been broken in production.

Phase 5 — reconnecting the signed execution layer
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Dict, List, NamedTuple, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import settings as app_settings
from core.paper_trader import binding_of
from core.portfolio import (
    BUY,
    SELL,
    MarketHistory,
    PortfolioState,
    Trade,
    cash_from,
    derive_state,
    short_terms_of,
    trade_day,
    trade_refusal,
)

logger = logging.getLogger(__name__)

#: Used when a request names no portfolio. Configurable so a deployment can
#: point the signed path at whichever book it actually trades.
DEFAULT_PORTFOLIO = "Default Portfolio"


class PortfolioNotFound(LookupError):
    """The named portfolio does not exist. Never silently substituted."""


class MarketContext(NamedTuple):
    history: Optional[MarketHistory]
    prices: Dict[str, float]
    asset_classes: Dict[str, Optional[str]]


async def _load_context(
    tickers: List[str], first_day: Optional[date], start: Optional[date] = None
) -> MarketContext:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.pool import NullPool

    from core.paper_history import asset_classes, latest_prices, load_history, new_york_today
    from db.repositories.market_data import TimescaleMarketDataRepo
    from db.repositories.rates import TimescaleRateRepo
    from db.session import settings

    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with AsyncSession(engine) as session:
            market = TimescaleMarketDataRepo(session)
            history = None
            if first_day is not None:
                history = await load_history(
                    tickers,
                    first_day,
                    new_york_today(),
                    market,
                    TimescaleRateRepo(session),
                    cash_from=start,
                )
            prices, _ = await latest_prices(tickers, market)
            classes = await asset_classes(tickers, market)
    finally:
        await engine.dispose()
    return MarketContext(history, prices, classes)


class PortfolioStore:
    """Reads and appends trades over a synchronous connection."""

    def __init__(self, engine: Optional[Engine] = None) -> None:
        if engine is None:
            from db.session import settings

            engine = create_engine(settings.SYNC_DATABASE_URL, pool_pre_ping=True)
        self.engine = engine

    # -- reads ---------------------------------------------------------------

    def _portfolio_row(self, connection, name: str):
        row = connection.execute(
            text("SELECT id, initial_cash, metadata FROM portfolios WHERE name = :name"),
            {"name": name},
        ).first()
        if row is None:
            # PortfolioManager fell back to "the only portfolio" here, so a
            # typo traded in a different book. There is no fallback.
            raise PortfolioNotFound(f"No portfolio named {name!r}.")
        return row

    def load_trades(self, name: str) -> tuple[List[Trade], float]:
        with self.engine.connect() as connection:
            portfolio = self._portfolio_row(connection, name)
            rows = connection.execute(
                text(
                    "SELECT id, time, ticker, action, quantity, price, costs "
                    "FROM portfolio_trades WHERE portfolio_id = :pid "
                    "ORDER BY time, id"
                ),
                {"pid": portfolio.id},
            ).all()

        trades = [
            Trade(
                id=str(r.id),
                ts=r.time,
                ticker=r.ticker,
                action=r.action,
                quantity=r.quantity,
                price=r.price,
                costs=r.costs or 0.0,
            )
            for r in rows
        ]
        return trades, float(portfolio.initial_cash)

    def metadata(self, name: str) -> Optional[dict]:
        with self.engine.connect() as connection:
            return self._portfolio_row(connection, name).metadata

    def _market_context(
        self, tickers: List[str], first_day: Optional[date], start: Optional[date] = None
    ) -> MarketContext:
        """Corporate actions, closes, ^IRX, latest prices and classes (module docstring)."""
        return asyncio.run(_load_context(sorted(set(tickers)), first_day, start))

    def state(self, name: str, prices: Optional[dict] = None) -> PortfolioState:
        """
        Derived state — the same function, history and terms the REST router
        uses. With `prices` None, valued at the latest stored closes.
        """
        trades, initial_cash = self.load_trades(name)
        metadata = self.metadata(name)
        start = cash_from(metadata)
        tickers = [t.ticker for t in trades]
        first = min([trade_day(t) for t in trades] + ([start] if start else []), default=None)
        context = self._market_context(tickers, first, start)
        return derive_state(
            trades,
            initial_cash,
            context.prices if prices is None else prices,
            history=context.history,
            terms=short_terms_of(metadata),
        )

    # -- writes --------------------------------------------------------------

    def append_trade(
        self,
        name: str,
        symbol: str,
        action: str,
        quantity: float,
        price: float,
        when: Optional[datetime] = None,
        costs: float = 0.0,
    ) -> int:
        """
        Append one trade. Returns its id.

        Validation mirrors the REST endpoint, through the same
        core.portfolio.trade_refusal, so this path cannot record something the
        API would reject. A refusal is a ValueError with the API's reason.
        """
        action = (action or "").upper()
        if action not in (BUY, SELL):
            raise ValueError(f"`action` must be BUY or SELL; got {action!r}.")
        if quantity <= 0:
            raise ValueError("Quantity must be positive.")
        if price <= 0:
            raise ValueError("Price must be positive.")
        if costs < 0:
            raise ValueError("Costs must not be negative.")

        metadata = self.metadata(name)
        binding = binding_of(metadata)
        if binding is not None:
            # Decision 50: an attached portfolio's record is its strategy's.
            raise ValueError(
                f"{name!r} is traded by '{binding.strategy}'; a manual trade would mix a "
                "manual decision into its record. Detach the strategy first."
            )

        when = when or datetime.now(timezone.utc)
        trade = Trade(symbol.upper(), action, quantity, price, when, costs=costs)
        trades, initial_cash = self.load_trades(name)
        start = cash_from(metadata)
        first = min([trade_day(t) for t in trades + [trade]] + ([start] if start else []))
        context = self._market_context([t.ticker for t in trades] + [trade.ticker], first, start)
        refusal = trade_refusal(
            trades,
            trade,
            initial_cash,
            context.prices,
            history=context.history,
            terms=short_terms_of(metadata),
            asset_class=context.asset_classes.get(trade.ticker),
            shortable=app_settings.SHORTABLE_ASSET_CLASSES,
        )
        if refusal:
            raise ValueError(refusal)

        with self.engine.begin() as connection:
            portfolio = self._portfolio_row(connection, name)
            row = connection.execute(
                text(
                    "INSERT INTO portfolio_trades "
                    "(portfolio_id, time, ticker, action, quantity, price, costs) "
                    "VALUES (:pid, :time, :ticker, :action, :quantity, :price, :costs) "
                    "RETURNING id"
                ),
                {
                    "pid": portfolio.id,
                    "time": when,
                    "ticker": symbol.upper(),
                    "action": action,
                    "quantity": quantity,
                    "price": price,
                    "costs": costs,
                },
            ).first()
        return int(row.id)

    def dispose(self) -> None:
        self.engine.dispose()
