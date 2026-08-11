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
    # Comma-separated origins, e.g. "http://localhost:5174,https://app.example.com"
    #
    # 5174 is this project's Vite dev port — NOT Vite's 5173 default, which every
    # other Vite project also takes. Must match `port` in frontend/vite.config.ts
    # and QUANT_VITE_PORT in run_pipeline.sh; a drifting dev port shows up as a
    # CORS failure that reads like an API bug. 127.0.0.1 and localhost are
    # distinct origins to a browser, so both spellings are listed.
    #
    # 5173 is kept so a manually-started `vite` on its default still works.
    # 8501 (Streamlit) was REMOVED on 2026-08-11: dashboard_app was deleted on
    # 2026-08-09, and cfa-study-app is what actually runs on 8501 now — leaving
    # it listed would have granted a different project's UI access to this API.
    cors_origins_str: str = (
        "http://localhost:5174,http://127.0.0.1:5174,"
        "http://localhost:5173,http://127.0.0.1:5173,"
        "http://localhost:3000"
    )

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
