"""reconstructed_membership

Revision ID: 0008_reconstructed_membership
Revises: 0007_served_prices
Create Date: 2026-10-04

Index membership RECONSTRUCTED from a source's own history, for dates before
this system began observing it. See
research/sp500-membership-reconstruction-2026-10-04.md.

WHY A SEPARATE TABLE
--------------------
`universe_membership` (0006) is documented as observed-only: every row is
something the daily job saw. This is a different kind of fact — what a past
revision of the Wikipedia constituents page listed — and mixing the two would
make an inferred row indistinguishable from an observed one. A reader has to
opt in to reconstructed data by naming this table.

WHAT A ROW MEANS
----------------
"`symbol` was listed as a member of `index_name` by the revision
`source_revision`, which was the page as it stood at the end of `as_of`."
One full member list per `as_of`, month-end resolution: a change inside a
month is dated to that month's end, and the symbol is the ticker AS WRITTEN
THEN (FB, not META).

Nothing reads this table yet. Membership alone does not make a cross-sectional
backtest honest: measured the same day, Yahoo has no prices for 151 of the 268
tickers that left the S&P 500 since 2015.
"""

import sqlalchemy as sa
from alembic import op

revision = "0008_reconstructed_membership"
down_revision = "0007_served_prices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "universe_membership_reconstructed",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("index_name", sa.String(), nullable=False),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("source_revision", sa.BigInteger(), nullable=False),
        sa.Column("revision_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "index_name", "as_of", "symbol", name="uq_reconstructed_member"
        ),
    )


def downgrade() -> None:
    op.drop_table("universe_membership_reconstructed")
