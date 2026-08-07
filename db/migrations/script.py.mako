"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# ---------------------------------------------------------------------------
# Alembic revision identifiers
# ---------------------------------------------------------------------------
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


# ---------------------------------------------------------------------------
# Upgrade — apply changes
#
# NOTE: Alembic does not know about TimescaleDB DDL. Hypertable creation,
# chunk_time_interval changes, compression and retention policies all have to
# be added by hand as op.execute(...) calls. See 0002_retune_hypertable.py.
# ---------------------------------------------------------------------------

def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


# ---------------------------------------------------------------------------
# Downgrade — undo changes
# ---------------------------------------------------------------------------

def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
