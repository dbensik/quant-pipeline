"""watchlists

Revision ID: 0004_watchlists
Revises: 0003_portfolios
Create Date: 2026-08-09

Moves watchlists off `watchlists.json` and into the database, for the same
reason portfolios moved in 0003: the file is mutable state with more than one
writer, and Streamlit held it in memory and rewrote the whole document on
every save.

Symbols live in a child table rather than a JSONB array. The unique
constraint makes a duplicate ticker impossible rather than merely
discouraged, and "which watchlists contain AAPL?" — which the news feed asks
— becomes an index lookup instead of a containment scan.

`position` preserves the order the user arranged the list in; a set would not.

Data migration is NOT performed here — scripts/import_watchlists_json.py does
it explicitly and re-runnably, matching 0003.
"""

import sqlalchemy as sa
from alembic import op

revision = "0004_watchlists"
down_revision = "0003_portfolios"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watchlists",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_watchlist_name"),
    )

    op.create_table(
        "watchlist_symbols",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("watchlist_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False, server_default="0"),
        sa.ForeignKeyConstraint(
            ["watchlist_id"], ["watchlists.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("watchlist_id", "symbol", name="uq_watchlist_symbol"),
    )
    op.create_index("ix_watchlist_symbols_symbol", "watchlist_symbols", ["symbol"])


def downgrade() -> None:
    op.drop_index("ix_watchlist_symbols_symbol", table_name="watchlist_symbols")
    op.drop_table("watchlist_symbols")
    op.drop_table("watchlists")
