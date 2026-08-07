"""
db/models.py
SQLAlchemy ORM models for the quant pipeline.

Phase 2 — TimescaleDB Schema & Repository Layer
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class AssetORM(Base):
    """
    Canonical asset registry.
    One row per unique (symbol, asset_class, source) triple.
    All market_data rows reference an asset_id foreign key into this table.
    """

    __tablename__ = "assets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    asset_class = Column(String, nullable=False)  # 'equity' | 'crypto' | 'option' | 'future'
    source = Column(String, nullable=False)        # 'yfinance' | 'coingecko'
    metadata_ = Column("metadata", JSONB, nullable=True)

    # NOTE: lazy="dynamic" is deprecated in SQLAlchemy 2.0 (removed in 2.1) and is
    # incompatible with AsyncSession anyway. "selectin" issues one batched SELECT and
    # works under asyncio; for large per-asset scans, query MarketDataORM directly
    # via the repository instead of traversing this relationship.
    market_data = relationship(
        "MarketDataORM", back_populates="asset", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint("symbol", "asset_class", "source", name="uq_asset_identity"),
    )

    def __repr__(self) -> str:
        return f"<AssetORM {self.symbol} [{self.asset_class}] via {self.source}>"


class MarketDataORM(Base):
    """
    OHLCV time-series table, partitioned as a TimescaleDB hypertable on `time`.
    The hypertable is created in the Alembic migration (not here) via:
        SELECT create_hypertable('market_data', 'time', ...)

    NOTE: Do NOT declare a single-column primary key on `time` alone here —
    TimescaleDB hypertables require `time` to be part of a composite PK or
    have no PK so Alembic can add it. We use a composite PK (time, asset_id)
    which satisfies TimescaleDB's partitioning requirement.

    Partitioning is time-only with 90-day chunks (revision 0002). The original
    4-way space partition on asset_id was removed: on a single node it bought
    no chunk exclusion while multiplying the chunk count 4x, which pushed query
    planning to 36ms against 2ms of execution. See 0002_retune_hypertable.
    """

    __tablename__ = "market_data"

    time = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    asset_id = Column(
        Integer,
        ForeignKey("assets.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    source = Column(String, nullable=False)

    asset = relationship("AssetORM", back_populates="market_data")

    __table_args__ = (
        # Composite index optimised for per-asset time-range queries — the
        # access path every API router uses. `time DESC` must match the
        # migration's DDL exactly, or autogenerate reports phantom drift.
        Index("ix_market_data_asset_time", "asset_id", text("time DESC")),
    )

    def __repr__(self) -> str:
        return f"<MarketDataORM asset_id={self.asset_id} time={self.time}>"
