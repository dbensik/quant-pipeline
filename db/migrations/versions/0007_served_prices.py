"""served_prices

Revision ID: 0007_served_prices
Revises: 0006_universe_membership
Create Date: 2026-09-27

Phase 1 of research/dividend-drift-plan-2026-09-27.md. Schema only: nothing
writes these columns yet and nothing reads them.

WHY
---
Every stored bar is Yahoo's `auto_adjust=True` value, adjusted for every split,
spinoff and dividend Yahoo knew of ON THE DAY IT WAS FETCHED. Bars fetched on
different days are adjusted to different as-of dates, so the series has steps
that are not market moves: the 2025-07-16 seam (121 dividend payers, CAG
-8.8%), every ex-date since daily ingest resumed (MO -1.58pp on 2026-09-15),
and the NFLX/APH/MNST split drift that `last_full_refresh_at` (0005) exists to
detect. It grows with every ex-date of every payer.

The fix is to store what the provider SERVED, and when, and derive adjusted
prices at read time. Yahoo has no truly raw price: `auto_adjust=False` gives a
close already adjusted for splits and spinoffs as of the fetch, but not for
dividends. Recording `fetched_at` beside it is what makes the raw price
recoverable — raw = served x every split or manual factor with an ex-date in
(bar date, fetched_at] — so a later split never forces a stored bar to be
rewritten.

WHAT IS ADDED
-------------
`market_data.served_*`, `market_data.fetched_at`
    Yahoo's `auto_adjust=False` OHLCV exactly as returned, and when. Nullable:
    NULL until the phase-4 migration or phase-3 ingest writes them. The
    existing open/high/low/close/volume are untouched and stay authoritative
    until cutover, so backing out is ignoring these.

`assets.price_basis`
    NULL — not yet migrated; readers use the existing columns (today's
    behaviour). 'served' — passed the per-symbol gate; read-time adjustment
    applies. 'legacy' — Yahoo cannot serve this series correctly (PARA's key
    serves another company; ANSS, AVB and other delisted names are gone), so it
    keeps its current values permanently.

`corporate_actions`
    One row per event. `value`: split ratio (10.0 for 10:1), dividend per
    share as served, or a multiplicative price factor. `manual_factor` is for
    events Yahoo applies but never lists — HWM's 2020-04-01 Arconic spinoff,
    0.76687, is baked into its served close and absent from every action feed.
    Manual rows must carry `evidence`.

Adding nullable columns without a default is a catalogue-only change in
Postgres, so this is instant on the ~1M-row hypertable. Compression is off
(0 of 49 chunks, checked 2026-09-27).
"""

import sqlalchemy as sa
from alembic import op

revision = "0007_served_prices"
down_revision = "0006_universe_membership"
branch_labels = None
depends_on = None

SERVED = ("open", "high", "low", "close", "volume")


def upgrade() -> None:
    for field in SERVED:
        op.add_column(
            "market_data", sa.Column(f"served_{field}", sa.Float(), nullable=True)
        )
    op.add_column(
        "market_data",
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.add_column("assets", sa.Column("price_basis", sa.String(), nullable=True))
    op.create_check_constraint(
        "ck_assets_price_basis",
        "assets",
        "price_basis IS NULL OR price_basis IN ('served', 'legacy')",
    )

    op.create_table(
        "corporate_actions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("ex_date", sa.Date(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "asset_id", "ex_date", "kind", name="uq_corporate_action"
        ),
        sa.CheckConstraint(
            "kind IN ('split', 'dividend', 'manual_factor')",
            name="ck_corporate_action_kind",
        ),
        sa.CheckConstraint("value > 0", name="ck_corporate_action_value"),
        sa.CheckConstraint(
            "kind <> 'manual_factor' OR evidence IS NOT NULL",
            name="ck_manual_factor_has_evidence",
        ),
    )


def downgrade() -> None:
    op.drop_table("corporate_actions")
    op.drop_constraint("ck_assets_price_basis", "assets", type_="check")
    op.drop_column("assets", "price_basis")
    op.drop_column("market_data", "fetched_at")
    for field in reversed(SERVED):
        op.drop_column("market_data", f"served_{field}")
