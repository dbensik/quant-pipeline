"""corporate_actions

Revision ID: 0005_corporate_actions
Revises: 0004_watchlists
Create Date: 2026-08-09

Two columns on `assets`, both there to remove an ambiguity that was silently
corrupting data.

`last_full_refresh_at`
----------------------
yfinance's `auto_adjust=True` restates a WHOLE series for splits as of the
fetch date. Both the legacy SQLite pipeline and the current adapter use it, so
nothing was ever "unadjusted" — but a symbol whose bars were fetched at two
different times ends up with two segments adjusted to two different as-of
dates.

Measured on 2026-08-09, before the fix: NFLX closed 1260.27 on 2025-07-15 and
125.03 on 2025-07-16. A 10:1 split in November 2025 had been applied to the
newer segment only. Every strategy read that as a -90% day. Fourteen symbols
were affected — AMCR AZN BDX BKNG CMCSA CRWD DD FDX HON KLAC NFLX NOW SPGI TPL
— every S&P name that split after the migration boundary.

Recording when a series was last restated makes the drift detectable: any
split newer than this timestamp means the stored bars need a full backfill.

`delisted_at`
-------------
An ingest that fetches nothing reports `written: 0` whether the symbol is
already current or no longer trades. Eleven symbols sat at 2025-07-15 after a
full run — ANSS, BK, CTRA, DAY, FI, HES, HOLX, IPG, K, MMC, WBA — all
acquired or taken private, all reporting exactly what an up-to-date symbol
reports.

NOT ADDED: successor/predecessor links for renames. FI is Fiserv's post-rename
ticker and FISV still holds its history, but nothing in yfinance says they are
the same company, so a `successor_id` column would be one nobody could
populate. Left open deliberately.
"""

import sqlalchemy as sa
from alembic import op

revision = "0005_corporate_actions"
down_revision = "0004_watchlists"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "assets",
        sa.Column("last_full_refresh_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "assets",
        sa.Column("delisted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("assets", "delisted_at")
    op.drop_column("assets", "last_full_refresh_at")
