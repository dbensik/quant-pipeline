"""
services/execution_service/service.py
gRPC ExecutionService — the SIGNED path for paper trades.

REWIRED 2026-08-09. This read `portfolios.json` through PortfolioManager, a
file nothing else has used since portfolios moved into the database in 0003.
GetPortfolio did `state["cash"]` on a trade-log-shaped portfolio and raised
KeyError against the user's real data, so the paper portfolio view had been
broken in production; deleting Streamlit merely removed the only client that
reached it.

It now reads and writes the same `portfolios` / `portfolio_trades` tables the
REST API uses, deriving cash and positions with `core.portfolio.derive_state`
— so the signed layer and the API cannot disagree about what a portfolio
holds. That is the point of keeping this service: an audit-logged execution
path over the real book, rather than a second book nobody reads.

Access is synchronous (see portfolio_store.py): gRPC servicers are sync, and
db/session.py's async engine binds its pool to one event loop.

Phase 5 — reconnecting the signed execution layer
"""

import logging
from datetime import datetime, timedelta, timezone

import dateutil.parser
import grpc

from services.execution_service.portfolio_store import (
    DEFAULT_PORTFOLIO,
    PortfolioNotFound,
    PortfolioStore,
)
from services.proto import execution_pb2, execution_pb2_grpc

logger = logging.getLogger("ExecutionService")

#: How long a quote stays valid. The client shows a countdown against this.
QUOTE_VALIDITY = timedelta(seconds=30)


class ExecutionService(execution_pb2_grpc.ExecutionServiceServicer):
    """gRPC service for paper trades against the database-backed portfolios."""

    def __init__(self, store: PortfolioStore | None = None):
        self.store = store or PortfolioStore()

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _name(requested: str) -> str:
        return requested.strip() if requested and requested.strip() else DEFAULT_PORTFOLIO

    def _quote_expired(self, timestamp: str) -> bool:
        """
        True when the quote the user confirmed against is too old.

        A malformed timestamp is treated as EXPIRED, not as valid. The previous
        version logged the parse error and proceeded — so the one input that
        defeats the check was also the one that skipped it.
        """
        if not timestamp:
            return True
        try:
            quoted_at = dateutil.parser.isoparse(timestamp)
        except (ValueError, TypeError) as exc:
            logger.warning("Unparseable quote timestamp %r: %s", timestamp, exc)
            return True
        if quoted_at.tzinfo is None:
            quoted_at = quoted_at.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - quoted_at) > QUOTE_VALIDITY

    # -- rpcs ----------------------------------------------------------------

    def ExecuteTrade(self, request, context):
        logger.info(
            "Trade request: %s %s %s @ %s in %r",
            request.action, request.quantity, request.symbol,
            request.price, request.portfolio or DEFAULT_PORTFOLIO,
        )

        if self._quote_expired(request.timestamp):
            message = "Trade Rejected: Quote expired (limit 30s)."
            logger.warning(message)
            return execution_pb2.TradeResponse(success=False, message=message)

        name = self._name(request.portfolio)
        try:
            trade_id = self.store.append_trade(
                name=name,
                symbol=request.symbol,
                action=request.action,
                quantity=request.quantity,
                price=request.price,
            )
        except PortfolioNotFound as exc:
            # Named but absent. PortfolioManager fell back to "the only
            # portfolio" here, so a typo traded in a different book.
            logger.warning("Trade rejected: %s", exc)
            return execution_pb2.TradeResponse(
                success=False, message=f"Trade Rejected: {exc}"
            )
        except ValueError as exc:
            logger.warning("Trade rejected: %s", exc)
            return execution_pb2.TradeResponse(
                success=False, message=f"Trade Rejected: {exc}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Internal execution error: %s", exc, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return execution_pb2.TradeResponse()

        verb = "BOUGHT" if request.action.upper() == "BUY" else "SOLD"
        return execution_pb2.TradeResponse(
            success=True,
            message=f"{verb} {request.quantity} {request.symbol} @ {request.price}",
            # The trade's own row id, so a caller can find what was recorded.
            # It used to be a timestamp, which referred to nothing.
            transaction_id=str(trade_id),
            filled_price=request.price,
        )

    def GetPortfolio(self, request, context):
        name = self._name(request.portfolio)
        try:
            state = self.store.state(name)
            # Value at the latest STORED close, not at entry price. The old
            # implementation used the entry price "as proxy for value in MVP",
            # which made unrealised P&L identically zero and total equity wrong.
            prices = self.store.latest_prices([p.ticker for p in state.positions])
            state = self.store.state(name, prices)
        except PortfolioNotFound as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return execution_pb2.PortfolioResponse()
        except Exception as exc:  # noqa: BLE001
            logger.error("Could not read portfolio %r: %s", name, exc, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return execution_pb2.PortfolioResponse()

        positions = {
            position.ticker: execution_pb2.Position(
                symbol=position.ticker,
                quantity=position.quantity,
                average_price=position.average_price,
                current_value=(
                    position.market_value
                    if position.market_value is not None
                    else position.quantity * position.average_price
                ),
            )
            for position in state.positions
        }

        return execution_pb2.PortfolioResponse(
            positions=positions,
            cash_balance=state.cash,
            total_equity=state.total_equity,
        )
