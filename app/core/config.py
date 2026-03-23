"""
Centralised configuration for the Quant Pipeline API.

Reads from environment variables (set in docker-compose.yml or .env) with
sensible local-dev defaults so the app works both inside containers and on
bare metal.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings populated from environment variables."""

    # -- Database --------------------------------------------------------
    # Docker Compose sets DATABASE_URL; local dev falls back to SQLite.
    database_url: str = "sqlite+aiosqlite:///quant_pipeline.db"

    # -- CORS ------------------------------------------------------------
    # Comma-separated origins, e.g. "http://localhost:8501,https://app.example.com"
    cors_origins_str: str = "http://localhost:8501,http://localhost:3000"

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins_str.split(",") if o.strip()]

    # -- General ---------------------------------------------------------
    debug: bool = False

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
