"""rate_observations

Revision ID: 0009_rate_observations
Revises: 0008_reconstructed_membership
Create Date: 2026-10-08

Daily interest-rate observations, starting with ^IRX (13-week T-bill, bank
discount basis, percent) as the risk-free rate for option pricing and, later,
the DCF discount rate. See research/option-pricing-plan-2026-10-08.md phase 0.

WHY NOT market_data
-------------------
A yield is not a price. Measured on ^IRX 2015-01-02..2026-10-08: volume is
zero on every row, 7 closes are zero or negative (2020), 434 are at or below
0.05, and 112 days move more than 50%. As an asset it would enter the symbol
picker, backtests, Monte Carlo, the missing-day check (which takes every
non-crypto class) and the dollar-volume reassignment check.

Insert-only: one row per (series, obs_date), never rewritten.
"""

import sqlalchemy as sa
from alembic import op

revision = "0009_rate_observations"
down_revision = "0008_reconstructed_membership"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rate_observations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("series", sa.String(), nullable=False),
        sa.Column("obs_date", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("series", "obs_date", name="uq_rate_observation"),
    )


def downgrade() -> None:
    op.drop_table("rate_observations")
