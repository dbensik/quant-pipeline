"""insider_transactions

Revision ID: 0011_insider_transactions
Revises: 0010_fundamental_facts
Create Date: 2026-10-16

SEC Form 4 transactions for registry issuers, INSERT-ONLY (decision 40a,
research/desk-pack-2026-10-16.md): the Equity Research Desk's Management &
incentives seat reads what insiders actually did beside what they say.

One row is one transaction line of one filing, derivative or not. An amended
filing (4/A) is a new accession, so new rows; nothing is updated. THE KEY:
(accn, row_no) — a line's position within its filing.
"""

import sqlalchemy as sa
from alembic import op

revision = "0011_insider_transactions"
down_revision = "0010_fundamental_facts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "insider_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("issuer_cik", sa.Integer(), nullable=False),
        sa.Column("accn", sa.String(), nullable=False),
        sa.Column("row_no", sa.Integer(), nullable=False),
        sa.Column("form", sa.String(), nullable=False),
        sa.Column("filed", sa.Date(), nullable=False),
        sa.Column("reporter_cik", sa.Integer(), nullable=True),
        sa.Column("reporter_name", sa.String(), nullable=True),
        sa.Column("is_director", sa.Boolean(), nullable=False),
        sa.Column("is_officer", sa.Boolean(), nullable=False),
        sa.Column("is_ten_pct_owner", sa.Boolean(), nullable=False),
        sa.Column("officer_title", sa.String(), nullable=True),
        sa.Column("is_derivative", sa.Boolean(), nullable=False),
        sa.Column("security_title", sa.String(), nullable=True),
        sa.Column("transaction_date", sa.Date(), nullable=True),
        sa.Column("code", sa.String(), nullable=True),
        sa.Column("acquired_disposed", sa.String(), nullable=True),
        sa.Column("shares", sa.Float(), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("shares_after", sa.Float(), nullable=True),
        sa.Column("ownership", sa.String(), nullable=True),
        sa.Column("plan_10b5_1", sa.Boolean(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("accn", "row_no", name="uq_insider_transaction"),
    )
    op.create_index(
        "ix_insider_transactions_issuer", "insider_transactions", ["issuer_cik", "filed"]
    )


def downgrade() -> None:
    op.drop_index("ix_insider_transactions_issuer", table_name="insider_transactions")
    op.drop_table("insider_transactions")
