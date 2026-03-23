import strawberry
import grpc
import json
from typing import List, Optional
from strawberry.scalars import JSON

from services.proto import execution_pb2
from services.proto import execution_pb2_grpc
from services.proto import signals_pb2
from services.proto import signals_pb2_grpc

# Import legacy components
from dashboard_app.results_manager import ResultsManager
from services.utils import DataSerializer
from services.config import ServiceConfig

# ... existing imports ...

# Initialize managers
results_manager = ResultsManager()

# Lazy stub initialization
_signal_stub = None
_execution_stub = None

def get_signal_stub():
    global _signal_stub
    if _signal_stub is None:
         channel = grpc.insecure_channel(ServiceConfig.get_grpc_channel_target())
         _signal_stub = signals_pb2_grpc.SignalServiceStub(channel)
    return _signal_stub

def get_execution_stub():
    global _execution_stub
    if _execution_stub is None:
         channel = grpc.insecure_channel(ServiceConfig.get_grpc_channel_target())
         _execution_stub = execution_pb2_grpc.ExecutionServiceStub(channel)
    return _execution_stub

# --- Types ---

@strawberry.type
class SignedSignalType:
    value: float
    confidence: float
    features_hash: str
    signature: str
    public_key_id: str
    timestamp: str
    payload_json: str

@strawberry.type
class PositionType:
    symbol: str
    quantity: float
    average_price: float
    current_value: float

@strawberry.type
class PortfolioType:
    positions: List[PositionType]
    cash_balance: float
    total_equity: float

@strawberry.type
class TradeResponseType:
    success: bool
    message: str
    transaction_id: Optional[str]
    filled_price: Optional[float]

import asyncio

@strawberry.type
class Query:
    @strawberry.field
    async def signal(self, symbol: str, as_of: Optional[str] = None) -> SignedSignalType:
        if as_of is None:
            from datetime import datetime
            as_of = datetime.utcnow().isoformat()
        req = signals_pb2.SignalRequest(symbol=symbol, as_of=as_of)
        try:
            resp = await asyncio.to_thread(get_signal_stub().GetSignal, req, timeout=60)
            return SignedSignalType(
                value=resp.value,
                confidence=resp.confidence,
                features_hash=resp.features_hash,
                signature=resp.signature,
                public_key_id=resp.public_key_id,
                timestamp=resp.timestamp,
                payload_json=resp.payload_json
            )
        except grpc.RpcError as e:
            raise Exception(f"Failed to fetch signal: {e.details()}")

    @strawberry.field
    async def portfolio(self) -> PortfolioType:
        req = execution_pb2.PortfolioRequest()
        try:
            resp = await asyncio.to_thread(get_execution_stub().GetPortfolio, req, timeout=10)
            positions = [
                PositionType(
                    symbol=p.symbol,
                    quantity=p.quantity,
                    average_price=p.average_price,
                    current_value=p.current_value
                ) for p in resp.positions.values()
            ]
            return PortfolioType(
                positions=positions,
                cash_balance=resp.cash_balance,
                total_equity=resp.total_equity
            )
        except grpc.RpcError as e:
            raise Exception(f"Failed to fetch portfolio: {e.details()}")

    @strawberry.field
    def results(self) -> List[str]:
        return results_manager.get_saved_files()

    @strawberry.field
    def result(self, filename: str) -> Optional[JSON]:
        actual_filename = filename if filename.endswith(".pkl") else f"{filename}.pkl"
        data = results_manager.load(actual_filename)
        if data is None:
            return None
        return DataSerializer.serialize(data)

@strawberry.type
class Mutation:
    @strawberry.mutation
    async def execute_trade(self, symbol: str, action: str, quantity: float, price: float, timestamp: str) -> TradeResponseType:
        req = execution_pb2.TradeRequest(
            symbol=symbol, action=action, quantity=quantity, price=price, timestamp=timestamp
        )
        try:
            resp = await asyncio.to_thread(get_execution_stub().ExecuteTrade, req, timeout=10)
            return TradeResponseType(
                success=resp.success,
                message=resp.message,
                transaction_id=resp.transaction_id,
                filled_price=resp.filled_price
            )
        except grpc.RpcError as e:
             return TradeResponseType(success=False, message=f"RPC Error: {e.details()}", transaction_id=None, filled_price=None)

schema = strawberry.Schema(query=Query, mutation=Mutation)
