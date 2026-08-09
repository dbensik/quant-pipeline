"""
services/execution_service/portfolio_store.py
Synchronous database access for the gRPC execution service.

WHY A SECOND ACCESS PATH EXISTS. gRPC servicer methods are synchronous, and
db/session.py's engine is async and module-level — its pooled asyncpg
connections bind to whichever event loop first uses them. Calling
`asyncio.run()` from a servicer opens a second loop and asyncpg rejects it
("got result for unknown protocol state"). So this uses a SYNC engine over
psycopg2 instead of bridging loops.

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

import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.portfolio import BUY, SELL, PortfolioState, Trade, derive_state

logger = logging.getLogger(__name__)

#: Used when a request names no portfolio. Configurable so a deployment can
#: point the signed path at whichever book it actually trades.
DEFAULT_PORTFOLIO = "Default Portfolio"


class PortfolioNotFound(LookupError):
    """The named portfolio does not exist. Never silently substituted."""


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
            text("SELECT id, initial_cash FROM portfolios WHERE name = :name"),
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

    def state(self, name: str, prices: Optional[dict] = None) -> PortfolioState:
        """Derived state — the same function the REST router uses."""
        trades, initial_cash = self.load_trades(name)
        return derive_state(trades, initial_cash, prices)

    def latest_prices(self, tickers: List[str]) -> dict:
        """
        Most recent stored close per ticker, from the database.

        Not from yfinance: the REST portfolio endpoint values positions from
        the migrated database and the two must agree. The previous
        implementation valued positions at their ENTRY price ("using entry
        price as proxy for value in MVP"), which made unrealised P&L
        identically zero.
        """
        if not tickers:
            return {}
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT DISTINCT ON (a.symbol) a.symbol, m.close
                    FROM market_data m
                    JOIN assets a ON a.id = m.asset_id
                    WHERE a.symbol = ANY(:symbols) AND m.close IS NOT NULL
                    ORDER BY a.symbol, m.time DESC
                    """
                ),
                {"symbols": list(tickers)},
            ).all()
        return {r.symbol: float(r.close) for r in rows}

    # -- writes --------------------------------------------------------------

    def append_trade(
        self,
        name: str,
        symbol: str,
        action: str,
        quantity: float,
        price: float,
        when: Optional[datetime] = None,
    ) -> int:
        """
        Append one trade. Returns its id.

        Validation mirrors the REST endpoint so the signed path cannot record
        something the API would reject.
        """
        action = (action or "").upper()
        if action not in (BUY, SELL):
            raise ValueError(f"`action` must be BUY or SELL; got {action!r}.")
        if quantity <= 0:
            raise ValueError("Quantity must be positive.")
        if price <= 0:
            raise ValueError("Price must be positive.")

        when = when or datetime.now(timezone.utc)

        with self.engine.begin() as connection:
            portfolio = self._portfolio_row(connection, name)
            row = connection.execute(
                text(
                    "INSERT INTO portfolio_trades "
                    "(portfolio_id, time, ticker, action, quantity, price, costs) "
                    "VALUES (:pid, :time, :ticker, :action, :quantity, :price, 0) "
                    "RETURNING id"
                ),
                {
                    "pid": portfolio.id,
                    "time": when,
                    "ticker": symbol.upper(),
                    "action": action,
                    "quantity": quantity,
                    "price": price,
                },
            ).first()
        return int(row.id)

    def dispose(self) -> None:
        self.engine.dispose()
