import json
import random
from concurrent import futures
from datetime import date, datetime
from typing import Optional

import grpc
import pandas as pd

from alpha_models.mean_reversion import MeanReversionStrategy
from services.config import ServiceConfig
from services.crypto.audit_log import AuditLog
from services.crypto.hasher import Hasher
from services.crypto.signer import Signer
from services.execution_service.service import ExecutionService
from services.grpc_service.bars import BarReader
from services.logging_config import configure_logging
from services.proto import execution_pb2_grpc, signals_pb2, signals_pb2_grpc

logger = configure_logging("SignalService")


class SignalService(signals_pb2_grpc.SignalServiceServicer):
    """
    gRPC Service implementation that acts as the trusted core of the pipeline.

    Responsibilities:
    1. Read daily bars from TimescaleDB (bars.py), on or before the request's as_of.
    2. Execute quantitative models (e.g., MeanReversion).
    3. Cryptographically sign the results using Ed25519.
    4. Log every signal generation event to an immutable audit log.
    """

    def __init__(self, signer: Signer, audit_log: AuditLog, bars: Optional[BarReader] = None):
        """
        Initialize the SignalService with injected dependencies.
        """
        self.signer = signer
        self.audit_log = audit_log
        self.bars = bars or BarReader()
        logger.info(f"Server Public Key ID: {self.signer.get_public_key_id()}")

    def GetSignal(self, request, context):
        """
        RPC method to generate a signal for a requested symbol.
        """
        symbol = request.symbol
        as_of = request.as_of

        logger.info(f"Received request for {symbol} as of {as_of}")

        try:
            # --- 1. Bars from TimescaleDB (services/grpc_service/bars.py) ---
            # On or before the request's as_of, which until 2026-10-14 was
            # logged and ignored: every signal used the latest 200 days.
            try:
                day = date.fromisoformat(as_of) if as_of else None
            except ValueError:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"as_of {as_of!r} is not an ISO date")
                return signals_pb2.SignedSignal()
            df = self.bars.frame(symbol, day)

            if df.empty:
                logger.warning(f"No data found for symbol {symbol}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(
                    f"No bars for {symbol!r} on or before {as_of or 'today'}: "
                    "not a registry symbol, or nothing stored in the window"
                )
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
                # The signed value is the POSITION at the latest bar (1 long,
                # 0 flat), not the latest event. A strategy's signal is an
                # event (1 enter, -1 exit, 0 no change), so until 2026-10-14
                # this served 0 on every bar between an entry and its exit,
                # which reads as "flat" while the strategy is long.
                position = signals_df["signal"].cumsum().clip(0, 1)
                value = float(position.iloc[-1])

                # Confidence from how extreme the Z-score is. Until 2026-10-14
                # it read a z_score column the strategy never returned, so
                # every signal carried 0.5.
                z_score = strategy.z_score(df["Close"]).iloc[-1]
                abs_z = abs(float(z_score)) if pd.notna(z_score) else 0.0
                # Cap confidence at 0.99
                confidence = min(0.99, max(0.5, abs_z / 2.0)) if value != 0 else 0.5

            # --- 3. Cryptography & Audit ---
            # Prepare the canonical payload to be signed
            payload_data = {
                "symbol": symbol,
                "as_of": as_of,
                "value": value,
                "confidence": confidence,
                "nonce": random.randint(0, 1000000),  # Ensure uniqueness even for same data
            }
            # Serialize deterministically (sorted keys)
            payload_json = json.dumps(payload_data, sort_keys=True)

            # Hash the payload
            payload_hash = Hasher.hash_str(payload_json)

            # Sign the hash (Proof of Authenticity)
            signature = self.signer.sign(payload_hash.encode("utf-8"))

            # Add to Immutable Audit Log (Proof of History)
            self.audit_log.add_entry(
                payload_hash=payload_hash, metadata=f"Generated signal for {symbol}"
            )

            logger.info(f"Generated and Logged signal for {symbol}: {value}")

            return signals_pb2.SignedSignal(
                value=value,
                confidence=confidence,
                features_hash=payload_hash,
                signature=signature,
                public_key_id=self.signer.get_public_key_id(),
                timestamp=datetime.utcnow().isoformat(),
                payload_json=payload_json,
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


if __name__ == "__main__":
    serve()
