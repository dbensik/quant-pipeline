#!/bin/bash
#
# scripts/cron/daily_maintenance.sh
#
# Daily data maintenance, for cron. Two steps, in this order:
#
#   1. Ingest new price bars into TimescaleDB.
#   2. Snapshot index membership.
#
# Ingest runs FIRST so that any name which joined an index today already has
# bars by the time the snapshot records it.
#
# WHY A WRAPPER AND NOT A CRONTAB ONE-LINER
#   cron runs with a near-empty environment and an arbitrary working
#   directory. Both matter here: the Poetry venv is not on cron's PATH, and
#   db/session.py reads `.env` RELATIVELY, so a run from the wrong directory
#   silently falls back to default settings.
#
#   The previous crontab entry was a one-liner that activated conda and ran a
#   script under a Google Drive path which no longer exists. It failed every
#   morning since, with its own error output redirected into a directory that
#   is also gone — so nothing recorded that anything was wrong.
#
# Usage:  scripts/cron/daily_maintenance.sh [--snapshot-only|--ingest-only]

set -uo pipefail

# Resolve the project root from this script's own location, so moving the
# checkout does not require editing crontab.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT" || exit 1

VENV_PYTHON="$(poetry env info --path 2>/dev/null)/bin/python"
if [ ! -x "$VENV_PYTHON" ]; then
    # poetry itself may not be on cron's PATH; fall back to the known venv.
    VENV_PYTHON="$HOME/Library/Caches/pypoetry/virtualenvs/quant-pipeline-nPGsYOTA-py3.12/bin/python"
fi
if [ ! -x "$VENV_PYTHON" ]; then
    echo "ERROR: no Poetry venv python found. Run 'poetry install'." >&2
    exit 1
fi

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_maintenance.log"

# One run at a time. A slow ingest must not overlap the next day's, which
# would double the requests to the data provider for no benefit — the same
# reason the ingest endpoint is single-flight.
LOCK_FILE="$LOG_DIR/.daily_maintenance.lock"
if [ -e "$LOCK_FILE" ] && kill -0 "$(cat "$LOCK_FILE" 2>/dev/null)" 2>/dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - SKIPPED: a run is already in progress" \
        >> "$LOG_FILE"
    exit 0
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

MODE="${1:-all}"
STATUS=0

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $*" >> "$LOG_FILE"; }

log "=== daily maintenance starting (mode: $MODE) ==="

# Both steps need TimescaleDB. Checked up front so an overnight Docker restart
# produces one clear line instead of two stack traces — the difference between
# a log someone skims and one they ignore.
if ! "$VENV_PYTHON" - <<'PYCHECK' >> "$LOG_FILE" 2>&1
import socket, sys, urllib.parse
sys.path.insert(0, ".")
from db.session import settings

url = urllib.parse.urlparse(settings.DATABASE_URL.replace("+asyncpg", ""))
host, port = url.hostname or "127.0.0.1", url.port or 5432
try:
    with socket.create_connection((host, port), timeout=5):
        pass
except OSError as exc:
    print(f"Database unreachable at {host}:{port} ({exc}). Is Docker running?")
    raise SystemExit(1)
PYCHECK
then
    log "ABORTED: database unreachable — nothing was ingested or snapshotted."
    exit 1
fi

if [ "$MODE" != "--snapshot-only" ]; then
    log "--- ingest ---"
    if "$VENV_PYTHON" -m cli.run_pipeline >> "$LOG_FILE" 2>&1; then
        log "ingest OK"
    else
        log "ingest FAILED (exit $?)"
        STATUS=1
    fi
fi

if [ "$MODE" != "--ingest-only" ]; then
    log "--- universe snapshot ---"
    if "$VENV_PYTHON" scripts/snapshot_universes.py >> "$LOG_FILE" 2>&1; then
        log "snapshot OK"
    else
        log "snapshot FAILED (exit $?)"
        STATUS=1
    fi
fi

log "=== daily maintenance finished (exit $STATUS) ==="

# Keep the log from growing without bound; 30 days is plenty to notice a
# pattern of failures.
if [ -f "$LOG_FILE" ]; then
    tail -n 20000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

exit "$STATUS"
