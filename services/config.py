import os

class ServiceConfig:
    """
    Central configuration for all Quant Pipeline services.
    Values can be overridden via environment variables.
    """
    
    # --- gRPC Service Settings ---
    # Bind address. Default is loopback-only (changed 2026-07-31): the signal
    # service has no auth, so it must not listen on LAN interfaces by default.
    # For containerized/remote deployment, override explicitly:
    #   QUANT_GRPC_BIND_ADDRESS=0.0.0.0 (or [::])
    GRPC_BIND_ADDRESS = os.getenv("QUANT_GRPC_BIND_ADDRESS", "127.0.0.1")
    # Host for clients to connect to (localhost for local dev)
    GRPC_HOST = os.getenv("QUANT_GRPC_HOST", "localhost")
    GRPC_PORT = int(os.getenv("QUANT_GRPC_PORT", "50051"))
    
    # --- GraphQL Gateway Settings ---
    GRAPHQL_HOST = os.getenv("QUANT_GRAPHQL_HOST", "127.0.0.1")
    GRAPHQL_PORT = int(os.getenv("QUANT_GRAPHQL_PORT", "8000"))
    
    # --- Paths ---
    # Path to the immutable audit log file
    AUDIT_LOG_PATH = os.getenv("QUANT_AUDIT_LOG_PATH", "audit_log.json")
    
    # --- Security ---
    # Base64 encoded Ed25519 private key. 
    # If not set, a new ephemeral key is generated on startup.
    PRIVATE_KEY_B64 = os.getenv("QUANT_PRIVATE_KEY")
    
    # --- Helpers ---
    @classmethod
    def get_grpc_server_target(cls) -> str:
        """Returns the address string for Binding the gRPC server."""
        return f"{cls.GRPC_BIND_ADDRESS}:{cls.GRPC_PORT}"
        
    @classmethod
    def get_grpc_channel_target(cls) -> str:
        """Returns the address string for Clients to connect to gRPC."""
        return f"{cls.GRPC_HOST}:{cls.GRPC_PORT}"

    @classmethod
    def get_graphql_url(cls) -> str:
        """Returns the full URL for the GraphQL endpoint."""
        return f"http://{cls.GRAPHQL_HOST}:{cls.GRAPHQL_PORT}/graphql"
