"""fundamental_fact_source

Revision ID: 0014_fundamental_fact_source
Revises: 0013_news_fetch_own_tagged
Create Date: 2026-10-23

fundamental_facts.source: 'companyfacts' or 'instance' (decision 59,
research/sec-companyfacts-gap-2026-10-23.md). companyfacts leaves out whole
filings, which are then read from their own XBRL instance. Unlike 0013's
NULL, the existing rows are not unknown: the gateway path and --from-file
both load companyfacts documents, so every row before this came from it.
"""

import sqlalchemy as sa
from alembic import op

revision = "0014_fundamental_fact_source"
down_revision = "0013_news_fetch_own_tagged"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fundamental_facts",
        sa.Column("source", sa.String(), nullable=False, server_default="companyfacts"),
    )


def downgrade() -> None:
    op.drop_column("fundamental_facts", "source")
