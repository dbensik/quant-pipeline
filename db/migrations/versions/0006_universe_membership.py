"""universe_membership

Revision ID: 0006_universe_membership
Revises: 0005_corporate_actions
Create Date: 2026-08-09

Point-in-time index membership, so a screen or backtest over a past window can
use the universe as it was rather than as it is.

WHY
---
Screeners resolve their universe from `assets`, which holds whatever is
registered today. Screening a 2024 window against the 2026 S&P 500 is
survivorship bias in its purest form: the names dropped from the index are
disproportionately the ones that did badly, so removing them flatters every
result — and silently, because the output still looks reasonable. Measured
2026-08-09: 515 equities are registered and the current S&P 500 has 503, so
the two lists already disagree.

WHAT IS AND IS NOT RECORDED
---------------------------
`first_seen` / `last_seen` are when this system OBSERVED a symbol in an index,
not when it truly joined or left. Nothing available here can reconstruct
membership before the first snapshot. That gap is recorded rather than papered
over: `universe_snapshots` says when an index was actually looked at, so a
query for an earlier date returns "not observed" instead of quietly falling
back to today's list — which would reintroduce exactly the bias this table
exists to remove.

The value compounds from the first snapshot forward. It cannot be
backdated, which is the argument for taking the first one now.
"""

import sqlalchemy as sa
from alembic import op

revision = "0006_universe_membership"
down_revision = "0005_corporate_actions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "universe_membership",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("index_name", sa.String(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("first_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("index_name", "symbol", name="uq_universe_member"),
    )
    op.create_index(
        "ix_universe_membership_index_seen",
        "universe_membership",
        ["index_name", "last_seen"],
    )

    op.create_table(
        "universe_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("index_name", sa.String(), nullable=False),
        sa.Column("taken_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_universe_snapshots_index_taken",
        "universe_snapshots",
        ["index_name", "taken_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_universe_snapshots_index_taken", table_name="universe_snapshots")
    op.drop_table("universe_snapshots")
    op.drop_index(
        "ix_universe_membership_index_seen", table_name="universe_membership"
    )
    op.drop_table("universe_membership")
