"""news_archive

Revision ID: 0012_news_archive
Revises: 0011_insider_transactions
Create Date: 2026-10-22

Yahoo search headlines, INSERT-ONLY (decision 57 B,
research/news-archive-2026-10-22.md). Search serves only the newest 50 a
ticker, so a story not stored goes out of reach.

news_stories        one row per Yahoo story id, as first served (tags included)
news_story_symbols  which symbol's search found it, and when first
news_fetches        one row per symbol per run: the coverage log, failures too
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0012_news_archive"
down_revision = "0011_insider_transactions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_stories",
        sa.Column("story_id", sa.String(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("publisher", sa.String(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("story_type", sa.String(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("related_tickers", JSONB(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("story_id"),
    )
    op.create_table(
        "news_story_symbols",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("story_id", sa.String(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("search_term", sa.String(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["story_id"], ["news_stories.story_id"]),
        sa.UniqueConstraint("story_id", "symbol", name="uq_news_story_symbol"),
    )
    op.create_index("ix_news_story_symbols_symbol", "news_story_symbols", ["symbol"])
    op.create_table(
        "news_fetches",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("search_term", sa.String(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("returned", sa.Integer(), nullable=False),
        sa.Column("new_stories", sa.Integer(), nullable=False),
        sa.Column("oldest_published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("newest_published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("empty_attempts", sa.Integer(), nullable=False),
        sa.Column("window_truncated", sa.Boolean(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_news_fetches_symbol", "news_fetches", ["symbol", "fetched_at"])


def downgrade() -> None:
    op.drop_index("ix_news_fetches_symbol", table_name="news_fetches")
    op.drop_table("news_fetches")
    op.drop_index("ix_news_story_symbols_symbol", table_name="news_story_symbols")
    op.drop_table("news_story_symbols")
    op.drop_table("news_stories")
