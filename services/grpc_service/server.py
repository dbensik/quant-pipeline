import grpc
from concurrent import futures
import json
import random
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

from services.proto import signals_pb2
from services.proto import signals_pb2_grpc
from services.proto import execution_pb2_grpc
from services.execution_service.service import ExecutionService

from services.crypto.signer import Signer
from services.crypto.hasher import Hasher
from services.crypto.audit_log import AuditLog
from alpha_models.mean_reversion import MeanReversionStrategy
from services.config import ServiceConfig
from services.logging_config import configure_logging

logger = configure_logging("SignalService")

class SignalService(signals_pb2_grpc.SignalServiceServicer):
    """
    gRPC Service implementation that acts as the trusted core of the pipeline.
    
    Responsibilities:
    1. Fetch real market data (via yfinance).
    2. Execute quantitative models (e.g., MeanReversion).
    3. Cryptographically sign the results using Ed25519.
    4. Log every signal generation event to an immutable audit log.
    """
    def __init__(self, signer: Signer, audit_log: AuditLog):
        """
        Initialize the SignalService with injected dependencies. 
        """
        self.signer = signer
        self.audit_log = audit_log
        logger.info(f"Server Public Key ID: {self.signer.get_public_key_id()}")

    def GetSignal(self, request, context):
        """
        RPC method to generate a signal for a requested symbol.
        """
        symbol = request.symbol
        as_of = request.as_of
        
        logger.info(f"Received request for {symbol} as of {as_of}")

        try:
            # --- 1. Fetch Real Data ---
            # Fetch last 200 days to ensure enough data for rolling windows (e.g. 20-day MA)
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=200)
            
            # yfinance download
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            
            if df.empty:
                logger.warning(f"No data found for symbol {symbol}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"No data found for symbol {symbol}")
                return signals_pb2.SignedSignal()

            # Handle MultiIndex columns (common in newer yfinance versions)
            if isinstance(df.columns, pd.MultiIndex):
                # Try to drop the Ticker level if it exists to get a flat DataFrame
                if 'Ticker' in df.columns.names:
                     df = df.xs(symbol, axis=1, level='Ticker')
                elif len(df.columns.levels) > 1:
                     df.columns = df.columns.get_level_values(0)

            # Ensure we have a 'Close' column for the strategy
            if "Close" not in df.columns:
                 if "Adj Close" in df.columns:
                     df["Close"] = df["Adj Close"]
                 else:
                    logger.error(f"Data for {symbol} missing 'Close' column")
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"Data for {symbol} missing 'Close' column")
                    return signals_pb2.SignedSignal()

            # --- 2. Run Strategy ---
            # Instantiate the verified model with specific parameters
            # Lower threshold to 1.0 to increase signal frequency for demo purposes
            strategy = MeanReversionStrategy(window=20, threshold=1.0)
            signals_df = strategy.generate_signals(df)
            
            if signals_df.empty:
                 value = 0.0
                 confidence = 0.0
            else:
                 # Get the latest signal available
                 latest = signals_df.iloc[-1]
                 value = float(latest["signal"])
                 
                 # Dynamic Confidence Calculation
                 # Base confidence on how extreme the Z-score is relative to threshold
                 z_score = latest.get("z_score", 0.0)
                 abs_z = abs(z_score)
                 # Cap confidence at 0.99
                 confidence = min(0.99, max(0.5, abs_z / 2.0)) if value != 0 else 0.5 

            # --- 3. Cryptography & Audit ---
            # Prepare the canonical payload to be signed
            payload_data = {
                "symbol": symbol,
                "as_of": as_of,
                "value": value,
                "confidence": confidence,
                "nonce": random.randint(0, 1000000) # Ensure uniqueness even for same data
            }
            # Serialize deterministically (sorted keys)
            payload_json = json.dumps(payload_data, sort_keys=True)
            
            # Hash the payload
            payload_hash = Hasher.hash_str(payload_json)
            
            # Sign the hash (Proof of Authenticity)
            signature = self.signer.sign(payload_hash.encode('utf-8'))
            
            # Add to Immutable Audit Log (Proof of History)
            self.audit_log.add_entry(
                payload_hash=payload_hash,
                metadata=f"Generated signal for {symbol}"
            )
            
            logger.info(f"Generated and Logged signal for {symbol}: {value}")

            return signals_pb2.SignedSignal(
                value=value,
                confidence=confidence,
                features_hash=payload_hash,
                signature=signature,
                public_key_id=self.signer.get_public_key_id(),
                timestamp=datetime.utcnow().isoformat(),
                payload_json=payload_json
            )

        except Exception as e:
            logger.error(f"Error processing request: {e}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return signals_pb2.SignedSignal()

def serve():
    # Instantiate dependencies
    signer = Signer.from_config(ServiceConfig.PRIVATE_KEY_B64)
    if not ServiceConfig.PRIVATE_KEY_B64:
         logger.warning("Running with EPHEMERAL key. Set QUANT_PRIVATE_KEY for persistence.")
    
    audit_log = AuditLog(file_path=ServiceConfig.AUDIT_LOG_PATH)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Inject dependencies into SignalService
    signals_pb2_grpc.add_SignalServiceServicer_to_server(SignalService(signer, audit_log), server)
    execution_pb2_grpc.add_ExecutionServiceServicer_to_server(ExecutionService(), server)
    
    target = ServiceConfig.get_grpc_server_target()
    server.add_insecure_port(target)
    logger.info(f"SignalService gRPC server running on {target}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
