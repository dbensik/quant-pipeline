"""
Centralised configuration for the Quant Pipeline API.

Reads from environment variables (set in docker-compose.yml or .env) with
sensible local-dev defaults so the app works both inside containers and on
bare metal.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    API-layer settings only.

    NOTE: database configuration deliberately does NOT live here. `db/session.py`
    owns DATABASE_URL / SYNC_DATABASE_URL and the async engine; this class owns
    HTTP concerns. Two Settings classes both reading DATABASE_URL is how the app
    ended up pointing at SQLite while the data lived in TimescaleDB.
    """

    # -- CORS ------------------------------------------------------------
    # Comma-separated origins, e.g. "http://localhost:8501,https://app.example.com"
    cors_origins_str: str = "http://localhost:8501,http://localhost:3000"

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins_str.split(",") if o.strip()]

    # -- General ---------------------------------------------------------
    debug: bool = False

    # extra="ignore" is required, not optional: the project's .env carries
    # DATABASE_URL / SYNC_DATABASE_URL for db/session.py and Alembic. Without
    # this, merely creating that .env makes `import api.main` raise
    # ValidationError("Extra inputs are not permitted").
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
