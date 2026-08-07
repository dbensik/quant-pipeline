"""retune_hypertable

Revision ID: 0002_retune_hypertable
Revises: 0001_initial_schema
Create Date: 2026-08-07

Re-partitions `market_data` for the query shape Phase 3's routers actually use.

Why
---
0001 created the hypertable with a 7-day chunk interval AND a 4-way space
partition on asset_id. For 837,908 daily bars that produced 1,160 chunks
averaging ~722 rows each. Measured cost of a single-ticker, one-year query:

    Planning Time:  36.119 ms      <-- 16x execution
    Execution Time:  2.309 ms
    212 chunks scanned, 20,324 shared buffer hits, for 250 returned rows

Planning dominated because the planner must consider every chunk in range, and
space partitioning multiplies the chunk count by 4 with no exclusion benefit on
a single node. TimescaleDB recommends space partitioning only for multi-disk or
multi-node deployments.

This revision:
  * drops the space dimension (time-only partitioning)
  * widens chunk_time_interval 7 days -> 90 days

90 days rather than 1 year: at current density a 90-day chunk holds ~39k rows
(616 tickers x ~63 trading days), which is comfortably small, and it leaves
headroom for the intraday backfill into price_data_hourly without chunks
becoming oversized. 5.5 years of history lands at ~23 chunks instead of 1,160.

Also drops `ix_market_data_asset_time`. 0001 created it via raw SQL after
create_hypertable, but it never propagated to any chunk — it existed only as an
8 KB phantom on the parent, duplicating the (asset_id, time DESC) index that
TimescaleDB maintains itself. It is dropped here so `alembic revision
--autogenerate` stops reporting drift against db/models.py.

Data is preserved by copy — this does NOT require re-running the SQLite
migration.

Phase 2 — TimescaleDB Schema & Repository Layer
"""

from alembic import op
import sqlalchemy as sa

revision = "0002_retune_hypertable"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


CHUNK_INTERVAL = "90 days"


def _market_data_columns() -> list:
    """Column definitions shared by upgrade() and downgrade()."""
    return [
        sa.Column("time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("open", sa.Float(), nullable=True),
        sa.Column("high", sa.Float(), nullable=True),
        sa.Column("low", sa.Float(), nullable=True),
        sa.Column("close", sa.Float(), nullable=True),
        sa.Column("volume", sa.Float(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
    ]


def _rebuild(chunk_interval: str, space_partitions: int | None) -> None:
    """
    Rebuild market_data with the given partitioning, preserving all rows.

    TimescaleDB cannot re-chunk an existing hypertable in place, so the only
    way to change chunk_time_interval or drop a space dimension retroactively
    is to build a new hypertable and copy the data across.
    """
    # The phantom parent-only index from 0001, if still present.
    op.execute("DROP INDEX IF EXISTS ix_market_data_asset_time")

    op.create_table(
        "market_data_rebuild",
        *_market_data_columns(),
        sa.ForeignKeyConstraint(
            ["asset_id"], ["assets.id"],
            name="fk_market_data_rebuild_asset_id_assets",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("time", "asset_id", name="pk_market_data_rebuild"),
    )

    if space_partitions:
        op.execute(
            f"""
            SELECT create_hypertable(
                'market_data_rebuild', 'time',
                partitioning_column  => 'asset_id',
                number_partitions    => {space_partitions},
                chunk_time_interval  => INTERVAL '{chunk_interval}',
                if_not_exists        => TRUE
            )
            """
        )
    else:
        op.execute(
            f"""
            SELECT create_hypertable(
                'market_data_rebuild', 'time',
                chunk_time_interval => INTERVAL '{chunk_interval}',
                if_not_exists       => TRUE
            )
            """
        )

    # Copy every row. ORDER BY time keeps chunk writes sequential.
    op.execute(
        """
        INSERT INTO market_data_rebuild (time, asset_id, open, high, low, close, volume, source)
        SELECT time, asset_id, open, high, low, close, volume, source
        FROM market_data
        ORDER BY time
        """
    )

    op.execute("DROP TABLE market_data CASCADE")
    op.execute("ALTER TABLE market_data_rebuild RENAME TO market_data")

    # Renaming the table does not rename its constraints — restore canonical names
    # so a later autogenerate diff stays quiet.
    op.execute(
        "ALTER TABLE market_data RENAME CONSTRAINT pk_market_data_rebuild TO pk_market_data"
    )
    op.execute(
        "ALTER TABLE market_data "
        "RENAME CONSTRAINT fk_market_data_rebuild_asset_id_assets "
        "TO fk_market_data_asset_id_assets"
    )
    # create_hypertable named its default time index after the scratch table;
    # rename it so chunks don't inherit "..._rebuild_time_idx" forever.
    op.execute(
        "ALTER INDEX IF EXISTS market_data_rebuild_time_idx "
        "RENAME TO market_data_time_idx"
    )


def upgrade() -> None:
    # Time-only partitioning, 90-day chunks.
    _rebuild(chunk_interval=CHUNK_INTERVAL, space_partitions=None)

    # Dropping the space dimension also drops the (asset_id, time DESC) index
    # TimescaleDB had been maintaining as a side effect of partitioning on
    # asset_id. Recreate it explicitly: "one asset over a date range" is the
    # access path every Phase 3 router uses, and without it those queries fall
    # back to the time-leading PK and filter asset_id across every row in range
    # (~39k scanned per 90-day chunk to return ~63). Created on the hypertable
    # parent so TimescaleDB propagates it to all current and future chunks.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_market_data_asset_time "
        "ON market_data (asset_id, time DESC)"
    )


def downgrade() -> None:
    # Restore 0001's shape: 7-day chunks with a 4-way space partition, whose
    # partitioning re-creates the asset_id index automatically.
    _rebuild(chunk_interval="7 days", space_partitions=4)
