"""
db/models.py
SQLAlchemy ORM models for the quant pipeline.

Phase 2 — TimescaleDB Schema & Repository Layer
"""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Text,
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

    # --- corporate actions (0005) -----------------------------------------
    # When this symbol's WHOLE series was last restated by a full backfill.
    # yfinance's auto_adjust re-adjusts a series for splits as of the fetch
    # date, so a split newer than this timestamp means the stored bars are
    # adjusted to a stale as-of date and the series has a discontinuity.
    last_full_refresh_at = Column(DateTime(timezone=True), nullable=True)

    # Set when a fetch returns nothing for a symbol whose history is well
    # behind. Without it, "written: 0" means both "already current" and "no
    # longer trades", which is the ambiguity the Data page had to hedge.
    delisted_at = Column(DateTime(timezone=True), nullable=True)

    # --- served prices (0007) ---------------------------------------------
    # Which price columns are authoritative for this asset. NULL: not yet
    # migrated, the adjusted columns are used as they always were. 'served':
    # passed the per-symbol gate, prices are derived at read time from the
    # served_* columns and corporate_actions. 'legacy': Yahoo cannot serve
    # this series correctly (PARA, delisted names), so it keeps its current
    # values for good. See research/dividend-drift-plan-2026-09-27.md.
    price_basis = Column(String, nullable=True)

    # NOTE: lazy="dynamic" is deprecated in SQLAlchemy 2.0 (removed in 2.1) and is
    # incompatible with AsyncSession anyway. "selectin" issues one batched SELECT and
    # works under asyncio; for large per-asset scans, query MarketDataORM directly
    # via the repository instead of traversing this relationship.
    market_data = relationship(
        "MarketDataORM", back_populates="asset", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint("symbol", "asset_class", "source", name="uq_asset_identity"),
        CheckConstraint(
            "price_basis IS NULL OR price_basis IN ('served', 'legacy')",
            name="ck_assets_price_basis",
        ),
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

    # --- served prices (0007) ---------------------------------------------
    # Yahoo's auto_adjust=False values exactly as returned, and when. That
    # close is ALREADY split- and spinoff-adjusted as of `fetched_at`, but not
    # dividend-adjusted — Yahoo has no truly raw price. Raw is recoverable as
    # served x every split/manual factor with an ex-date in
    # (bar date, fetched_at], so these are written once and never restated.
    # NULL until written; the columns above stay authoritative until cutover.
    served_open = Column(Float, nullable=True)
    served_high = Column(Float, nullable=True)
    served_low = Column(Float, nullable=True)
    served_close = Column(Float, nullable=True)
    served_volume = Column(Float, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=True)

    asset = relationship("AssetORM", back_populates="market_data")

    __table_args__ = (
        # Composite index optimised for per-asset time-range queries — the
        # access path every API router uses. `time DESC` must match the
        # migration's DDL exactly, or autogenerate reports phantom drift.
        Index("ix_market_data_asset_time", "asset_id", text("time DESC")),
    )

    def __repr__(self) -> str:
        return f"<MarketDataORM asset_id={self.asset_id} time={self.time}>"


class CorporateActionORM(Base):
    """
    One split, dividend or manual price factor for one asset (revision 0007).

    `value` is a split ratio (10.0 for 10:1), a dividend per share AS SERVED
    (Yahoo's amounts are split-adjusted as of `fetched_at`), or a
    multiplicative price factor. `manual_factor` covers events Yahoo applies
    but never lists: HWM's 2020-04-01 Arconic spinoff, 0.76687, is baked into
    its served close and absent from every action feed. The table refuses a
    manual factor without `evidence`.
    """

    __tablename__ = "corporate_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(
        Integer, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    ex_date = Column(Date, nullable=False)
    kind = Column(String, nullable=False)  # 'split' | 'dividend' | 'manual_factor'
    value = Column(Float, nullable=False)
    source = Column(String, nullable=False)  # 'yfinance' | 'manual'
    fetched_at = Column(DateTime(timezone=True), nullable=False)
    evidence = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("asset_id", "ex_date", "kind", name="uq_corporate_action"),
        CheckConstraint(
            "kind IN ('split', 'dividend', 'manual_factor')",
            name="ck_corporate_action_kind",
        ),
        CheckConstraint("value > 0", name="ck_corporate_action_value"),
        CheckConstraint(
            "kind <> 'manual_factor' OR evidence IS NOT NULL",
            name="ck_manual_factor_has_evidence",
        ),
    )

    def __repr__(self) -> str:
        return f"<CorporateActionORM asset_id={self.asset_id} {self.kind} {self.ex_date}>"


# ---------------------------------------------------------------------------
# Paper / manual portfolios
# ---------------------------------------------------------------------------
# The trade log is the ONLY stored state. Cash, positions, average cost and
# P&L are derived from it by core/portfolio.py — there is deliberately no
# `cash` or `positions` column. `portfolios.json` stored both a trade log and
# a cash/positions ledger under one key, they disagreed, and the gRPC service
# raised KeyError('cash') reading a trade-log portfolio. A derived value
# cannot drift from the trades that produce it.


class PortfolioORM(Base):
    """A named paper or manually-recorded portfolio."""

    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    initial_cash = Column(Float, nullable=False, default=100_000.0)
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    # Free-form: the migrated `MAG7PortTest1` carried `constituents` and
    # `weights` keys that belong to no column. Kept rather than discarded.
    metadata_ = Column("metadata", JSONB, nullable=True)

    trades = relationship(
        "PortfolioTradeORM",
        back_populates="portfolio",
        lazy="selectin",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"<PortfolioORM {self.name!r}>"


class PortfolioTradeORM(Base):
    """
    One executed trade.

    NOT a hypertable: portfolios hold hundreds of rows, not millions, and the
    dominant query is "every trade for one portfolio" rather than a time
    range. Chunking that would add planning cost for no exclusion benefit —
    the lesson 0002_retune_hypertable recorded for market_data.
    """

    __tablename__ = "portfolio_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(
        Integer,
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
    )
    time = Column(DateTime(timezone=True), nullable=False)
    ticker = Column(String, nullable=False)
    # 'BUY' | 'SELL'. There is no `direction` column — see core/portfolio.py:
    # the Streamlit form recorded Long/Short independently of Buy/Sell, nothing
    # ever read it, and a short is simply a negative net quantity.
    action = Column(String, nullable=False)
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    costs = Column(Float, nullable=False, default=0.0)
    broker = Column(String, nullable=True)
    notes = Column(String, nullable=True)

    portfolio = relationship("PortfolioORM", back_populates="trades")

    __table_args__ = (
        # Average cost is order-dependent, so the log is always read in
        # timestamp order for one portfolio.
        Index("ix_portfolio_trades_portfolio_time", "portfolio_id", "time"),
    )

    def __repr__(self) -> str:
        return (
            f"<PortfolioTradeORM {self.action} {self.quantity} "
            f"{self.ticker} @ {self.price}>"
        )


# ---------------------------------------------------------------------------
# Watchlists
# ---------------------------------------------------------------------------

class WatchlistORM(Base):
    """A named list of tickers."""

    __tablename__ = "watchlists"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    symbols = relationship(
        "WatchlistSymbolORM",
        back_populates="watchlist",
        lazy="selectin",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"<WatchlistORM {self.name!r}>"


class WatchlistSymbolORM(Base):
    """
    One ticker in one watchlist.

    A child table rather than a JSONB array on `watchlists`. Two reasons:
    the unique constraint below makes a duplicate ticker impossible rather
    than merely discouraged, and "which watchlists contain AAPL?" — which the
    news feed asks — is an index lookup instead of a containment scan.
    """

    __tablename__ = "watchlist_symbols"

    id = Column(Integer, primary_key=True, autoincrement=True)
    watchlist_id = Column(
        Integer,
        ForeignKey("watchlists.id", ondelete="CASCADE"),
        nullable=False,
    )
    symbol = Column(String, nullable=False)
    # Preserves the order the user arranged them in; a set would not.
    position = Column(Integer, nullable=False, default=0)

    watchlist = relationship("WatchlistORM", back_populates="symbols")

    __table_args__ = (
        UniqueConstraint("watchlist_id", "symbol", name="uq_watchlist_symbol"),
        Index("ix_watchlist_symbols_symbol", "symbol"),
    )

    def __repr__(self) -> str:
        return f"<WatchlistSymbolORM {self.symbol}>"


# ---------------------------------------------------------------------------
# Point-in-time universe membership
# ---------------------------------------------------------------------------

class UniverseMembershipORM(Base):
    """
    One observation window of a symbol's membership in an index.

    WHY THIS EXISTS. Screeners and backtests resolve their universe from the
    `assets` table, which holds whatever is registered TODAY. Screening a 2024
    window against the 2026 index is survivorship bias: the names that were
    dropped are exactly the ones that did badly, and removing them flatters
    every result — silently, because the numbers still look plausible.

    OBSERVED, NOT RECONSTRUCTED. `first_seen` and `last_seen` are the dates
    this system SAW the symbol in the index, not the dates it truly joined or
    left. Nothing available here can reconstruct membership before the first
    snapshot, and inventing it would be worse than admitting the gap — so
    queries before `first_seen` for an index return an explicit "not observed"
    rather than today's list.
    """

    __tablename__ = "universe_membership"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_name = Column(String, nullable=False)   # 'sp500' | 'dow_jones' | ...
    symbol = Column(String, nullable=False)
    first_seen = Column(DateTime(timezone=True), nullable=False)
    #: Last snapshot in which the symbol was present. A symbol absent from a
    #: later snapshot keeps its old last_seen, which is what marks it as gone.
    last_seen = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("index_name", "symbol", name="uq_universe_member"),
        Index("ix_universe_membership_index_seen", "index_name", "last_seen"),
    )

    def __repr__(self) -> str:
        return f"<UniverseMembershipORM {self.index_name}/{self.symbol}>"


class UniverseSnapshotORM(Base):
    """
    A record that an index's membership was observed at a point in time.

    Kept separately from the memberships so "we looked and AAPL was absent" is
    distinguishable from "we never looked". Without it, a query for a date
    before any snapshot and a query for a date when a symbol had left would
    both return nothing.
    """

    __tablename__ = "universe_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_name = Column(String, nullable=False)
    taken_at = Column(DateTime(timezone=True), nullable=False)
    member_count = Column(Integer, nullable=False)

    __table_args__ = (
        Index("ix_universe_snapshots_index_taken", "index_name", "taken_at"),
    )


class ReconstructedMembershipORM(Base):
    """
    One symbol in one RECONSTRUCTED member list of an index.

    Not an observation. `UniverseMembershipORM` holds what the daily job saw;
    this holds what a past revision of the source page listed, loaded after
    the fact by scripts/reconstruct_sp500_membership.py. The two are kept
    apart so a reader has to choose reconstructed data by name.

    One full list per `as_of` (month-end). `symbol` is the ticker as the
    revision wrote it, so a renamed company appears under its old ticker in
    old lists. See research/sp500-membership-reconstruction-2026-10-04.md for
    what was validated and what this does NOT fix (delisted names have no
    prices).
    """

    __tablename__ = "universe_membership_reconstructed"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_name = Column(String, nullable=False)
    as_of = Column(Date, nullable=False)
    symbol = Column(String, nullable=False)
    source = Column(String, nullable=False)  # 'wikipedia_revision'
    source_revision = Column(BigInteger, nullable=False)
    #: When the revision was saved — at or before the end of `as_of`.
    revision_at = Column(DateTime(timezone=True), nullable=False)
    loaded_at = Column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "index_name", "as_of", "symbol", name="uq_reconstructed_member"
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ReconstructedMembershipORM {self.index_name}/{self.symbol}"
            f"@{self.as_of}>"
        )


class RateObservationORM(Base):
    """
    One daily observation of an interest-rate series, exactly as served.

    Not a price, so not in `assets`/`market_data`: ^IRX has zero volume, went
    negative on 7 days in 2020 and moved more than 50% day-over-day on 112
    days since 2015, every one of which would trip a price guard or check.

    Insert-only, like served prices: an observation is written once and never
    rewritten. `value` is in the series' own quoting convention (percent,
    bank-discount basis for ^IRX); core/rates.py converts on read.
    """

    __tablename__ = "rate_observations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    series = Column(String, nullable=False)  # '^IRX'
    obs_date = Column(Date, nullable=False)
    value = Column(Float, nullable=False)
    source = Column(String, nullable=False)  # 'yfinance'
    fetched_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("series", "obs_date", name="uq_rate_observation"),
    )
