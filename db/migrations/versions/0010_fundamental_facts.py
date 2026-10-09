"""fundamental_facts

Revision ID: 0010_fundamental_facts
Revises: 0009_rate_observations
Create Date: 2026-10-09

SEC XBRL company facts, raw and allow-listed, INSERT-ONLY. Tier 3 phase 1 of
research/dcf-plan-2026-10-08.md.

One row is one fact as one filing reported it. The same period appears again
in every later filing that carries it as a comparative, under that filing's
accession number and `filed` date: a restatement is a NEW row, never an
update. Reads choose by `filed` (strictly before the as-of date) in
modeling/statements.py.

THE KEY: (cik, taxonomy, concept, unit, period_start, period_end, accn).
Measured unique on the AAPL and SVB downloads of 2026-10-08 (25,135 and
28,585 rows, no duplicates). `period_start` is NULL for balance-sheet
(instant) facts, and a primary key cannot hold NULL, so the key is a UNIQUE
constraint with NULLS NOT DISTINCT (Postgres 15+; the server is 15.13) and
the primary key is a surrogate id. ON CONFLICT targets the constraint by name.
"""

import sqlalchemy as sa
from alembic import op

revision = "0010_fundamental_facts"
down_revision = "0009_rate_observations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fundamental_facts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("cik", sa.Integer(), nullable=False),
        sa.Column("taxonomy", sa.String(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("unit", sa.String(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("fy", sa.Integer(), nullable=True),
        sa.Column("fp", sa.String(), nullable=True),
        sa.Column("form", sa.String(), nullable=False),
        sa.Column("filed", sa.Date(), nullable=False),
        sa.Column("accn", sa.String(), nullable=False),
        sa.Column("frame", sa.String(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        "ALTER TABLE fundamental_facts ADD CONSTRAINT uq_fundamental_fact "
        "UNIQUE NULLS NOT DISTINCT (cik, taxonomy, concept, unit, period_start, period_end, accn)"
    )
    op.create_index("ix_fundamental_facts_lookup", "fundamental_facts", ["cik", "concept", "filed"])


def downgrade() -> None:
    op.drop_index("ix_fundamental_facts_lookup", table_name="fundamental_facts")
    op.drop_table("fundamental_facts")
