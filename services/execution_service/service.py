import grpc
import logging
from datetime import datetime, timedelta
import dateutil.parser

from services.proto import execution_pb2
from services.proto import execution_pb2_grpc
from services.execution_service.portfolio_manager import PortfolioManager

logger = logging.getLogger("ExecutionService")

class ExecutionService(execution_pb2_grpc.ExecutionServiceServicer):
    """
    gRPC Service for handling paper trades and portfolio management.
    """
    def __init__(self):
        self.portfolio_manager = PortfolioManager()
        
    def ExecuteTrade(self, request, context):
        """
        Executes a trade if it passes validation (e.g. timeout check).
        """
        logger.info(f"Received trade request: {request.action} {request.quantity} {request.symbol} @ {request.price}")
        
        # 1. Validation: Check if the trade execution is within the valid 30s window
        # The request.timestamp is when the user *viewed* the quote/signal.
        try:
            quote_time = dateutil.parser.isoparse(request.timestamp)
            now = datetime.utcnow()
            
            # Allow 30 seconds validity window for the quote
            if now - quote_time > timedelta(seconds=30):
                msg = "Trade Rejected: Quote expired (limit 30s)."
                logger.warning(msg)
                return execution_pb2.TradeResponse(
                    success=False,
                    message=msg
                )
        except Exception as e:
            logger.error(f"Timestamp parsing error: {e}")
            # In a strict system, we might reject. For MVP, we proceed or log.
            pass

        # 2. Execute via Portfolio Manager
        try:
            result_msg = self.portfolio_manager.execute_trade(
                symbol=request.symbol,
                action=request.action,
                quantity=request.quantity,
                price=request.price
            )
            
            logger.info(f"Trade Executed: {result_msg}")
            
            return execution_pb2.TradeResponse(
                success=True,
                message=result_msg,
                transaction_id=f"txn_{int(datetime.utcnow().timestamp())}",
                filled_price=request.price
            )
            
        except ValueError as e:
            msg = f"Trade Rejected: {str(e)}"
            logger.warning(msg)
            return execution_pb2.TradeResponse(
                success=False,
                message=msg
            )
        except Exception as e:
            logger.error(f"Internal execution error: {e}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return execution_pb2.TradeResponse()

    def GetPortfolio(self, request, context):
        """Returns the current portfolio state."""
        state = self.portfolio_manager.get_portfolio_state()
        
        # Convert dictionary state to Protobuf messages
        # State structure: {'cash': 100000.0, 'positions': {'AAPL': {'quantity': 10, 'average_price': 150.0}}}
        
        positions_pb = {}
        total_equity = state["cash"]
        
        for sym, pos_data in state.get("positions", {}).items():
            # Calculate current value (mocking current price as avg price for now, 
            # ideally we fetch real-time price here or pass it in)
            qty = pos_data["quantity"]
            avg = pos_data["average_price"]
            val = qty * avg # using entry price as proxy for value in MVP
            
            positions_pb[sym] = execution_pb2.Position(
                symbol=sym,
                quantity=qty,
                average_price=avg,
                current_value=val
            )
            total_equity += val
            
        return execution_pb2.PortfolioResponse(
            positions=positions_pb,
            cash_balance=state["cash"],
            total_equity=total_equity
        )
