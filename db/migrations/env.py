"""
db/migrations/env.py
Alembic migration environment — loads .env and wires up the SQLAlchemy models.

IMPORTANT: Alembic uses synchronous connections for migrations.
           Use SYNC_DATABASE_URL (postgresql://...), NOT DATABASE_URL (postgresql+asyncpg://...).

Phase 2 — TimescaleDB Schema & Repository Layer
"""

import os
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------------
# Load .env — must happen before importing Settings or creating the engine
# ---------------------------------------------------------------------------
# Resolve .env relative to the project root (two levels up from db/migrations/)
_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)

# ---------------------------------------------------------------------------
# Import all ORM models so Alembic's autogenerate can see the full schema
# ---------------------------------------------------------------------------
from db.models import Base  # noqa: E402  (import after load_dotenv is intentional)

# ---------------------------------------------------------------------------
# Alembic Config object (wraps alembic.ini)
# ---------------------------------------------------------------------------
config = context.config

# Interpolate SYNC_DATABASE_URL from environment into alembic.ini's
# sqlalchemy.url = %(SYNC_DATABASE_URL)s placeholder
config.set_main_option(
    "sqlalchemy.url",
    os.environ["SYNC_DATABASE_URL"],
)

# Wire up Python logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


# ---------------------------------------------------------------------------
# Keep TimescaleDB's own objects out of autogenerate
# ---------------------------------------------------------------------------
# TimescaleDB creates and maintains objects the ORM deliberately does not
# declare: the default `<table>_time_idx` on every hypertable, the per-chunk
# `_hyper_*` tables, and everything in its internal schemas. Without this
# filter, `alembic revision --autogenerate` proposes dropping them on every
# run — and committing that would break the hypertable.
_TIMESCALE_SCHEMAS = {
    "_timescaledb_catalog",
    "_timescaledb_internal",
    "_timescaledb_config",
    "_timescaledb_cache",
    "timescaledb_information",
    "timescaledb_experimental",
}


def include_object(object_, name, type_, reflected, compare_to):
    if getattr(object_, "schema", None) in _TIMESCALE_SCHEMAS:
        return False
    if type_ == "table" and name and name.startswith("_hyper_"):
        return False
    # TimescaleDB's default time index on each hypertable.
    if type_ == "index" and name and name.endswith("_time_idx"):
        return False
    return True


# ---------------------------------------------------------------------------
# Run migrations offline (generates SQL without a live DB connection)
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Ensure Alembic detects column-type changes (e.g. Float → Numeric)
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Run migrations online (against a live DB connection)
# ---------------------------------------------------------------------------
def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,    # single-use connection — no pooling during migrations
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
