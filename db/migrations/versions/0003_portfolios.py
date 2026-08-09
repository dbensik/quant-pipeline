"""portfolios

Revision ID: 0003_portfolios
Revises: 0002_retune_hypertable
Create Date: 2026-08-09

Moves paper / manually-recorded portfolios off `portfolios.json` and into the
database.

Why
---
The JSON file stored two incompatible shapes under the same key. The Streamlit
portfolio tab wrote `{"trades": [...]}`; the gRPC paper-trading service
expected `{"cash": ..., "positions": {...}}` and read `state["cash"]`
unguarded. Against the live file — whose two portfolios were both trade-log
shaped — `ExecutionService.GetPortfolio` raised KeyError('cash'). The paper
portfolio view was broken in production.

There is deliberately no `cash` or `positions` column here. The trade log is
the only stored state; everything else is derived by core/portfolio.py, so the
two can no longer disagree.

A second reason to leave the file: the gRPC service and Streamlit each held a
SEPARATE in-memory copy of portfolios.json, loaded at construction, and each
`_save_portfolios()` wrote the whole dict back — so whichever saved last
clobbered the other's changes. Adding the FastAPI router would have made it a
third writer.

Neither table is a hypertable. Portfolios hold hundreds of rows and the
dominant query is "every trade for one portfolio", not a time range; chunking
would add planning cost with no chunk-exclusion benefit — the lesson
0002_retune_hypertable recorded for market_data.

Data migration is NOT performed here. `scripts/import_portfolios_json.py`
does it explicitly, because it must reconcile hand-edited JSON and should be
re-runnable and inspectable rather than firing once inside a schema upgrade.
Both live portfolios had zero trades, so nothing but names and metadata moves.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0003_portfolios"
down_revision = "0002_retune_hypertable"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "portfolios",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("initial_cash", sa.Float(), nullable=False, server_default="100000.0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("metadata", JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_portfolio_name"),
    )

    op.create_table(
        "portfolio_trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("portfolio_id", sa.Integer(), nullable=False),
        sa.Column("time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("costs", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("broker", sa.String(), nullable=True),
        sa.Column("notes", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["portfolio_id"], ["portfolios.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_portfolio_trades_portfolio_time",
        "portfolio_trades",
        ["portfolio_id", "time"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_portfolio_trades_portfolio_time", table_name="portfolio_trades"
    )
    op.drop_table("portfolio_trades")
    op.drop_table("portfolios")
