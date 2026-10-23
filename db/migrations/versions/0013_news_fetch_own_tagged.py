"""news_fetch_own_tagged

Revision ID: 0013_news_fetch_own_tagged
Revises: 0012_news_archive
Create Date: 2026-10-22

news_fetches.own_tagged: how many of a search's stories Yahoo tagged with the
symbol (decision 57 B step 2, research/news-archive-gaps-2026-10-22.md). A
page mostly about other tickers is not a full page. NULL on the rows written
before it, which read as unknown, never as zero.
"""

import sqlalchemy as sa
from alembic import op

revision = "0013_news_fetch_own_tagged"
down_revision = "0012_news_archive"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("news_fetches", sa.Column("own_tagged", sa.Integer(), nullable=True))
    op.create_index(
        "ix_news_fetches_symbol_term", "news_fetches", ["symbol", "search_term", "fetched_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_news_fetches_symbol_term", table_name="news_fetches")
    op.drop_column("news_fetches", "own_tagged")
