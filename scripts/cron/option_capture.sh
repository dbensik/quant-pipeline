#!/bin/bash
#
# scripts/cron/option_capture.sh
#
# launchd wrapper for scripts/capture_option_chains.py. Weekdays 15:45 and
# 17:00 ET — see scripts/launchd/com.dbensik.quant-pipeline.option-capture.plist.
#
# Same shape as daily_maintenance.sh, for the same reasons: launchd gives a
# near-empty environment and no terminal, so the venv is resolved by absolute
# path, the run is single-instance, and a failure raises a notification
# rather than one line in a log nobody opens.
#
# Usage:  scripts/cron/option_capture.sh [extra args passed to the script]

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT" || exit 1

VENV_PYTHON="$(poetry env info --path 2>/dev/null)/bin/python"
if [ ! -x "$VENV_PYTHON" ]; then
    # poetry is not on launchd's PATH; fall back to the known venv.
    VENV_PYTHON="$HOME/Library/Caches/pypoetry/virtualenvs/quant-pipeline-nPGsYOTA-py3.12/bin/python"
fi

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/option_capture.log"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $*" >> "$LOG_FILE"; }

notify() {
    # Skipped on a terminal: whoever ran it by hand is already reading the output.
    [ -t 1 ] || osascript -e "display notification \"$1\" with title \"quant-pipeline option capture\"" >/dev/null 2>&1 || true
}

if [ ! -x "$VENV_PYTHON" ]; then
    log "ABORTED: no Poetry venv python found. Run 'poetry install'."
    notify "ABORTED: no Poetry venv. Missed option-chain days cannot be backfilled."
    exit 1
fi

# Single instance. The 17:00 run must not overlap a slow 15:45 one: both would
# fetch the same chains, and the second's writes would race the first's.
LOCK_FILE="$LOG_DIR/.option_capture.lock"
if [ -e "$LOCK_FILE" ] && kill -0 "$(cat "$LOCK_FILE" 2>/dev/null)" 2>/dev/null; then
    log "SKIPPED: a run is already in progress"
    exit 0
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

log "=== option capture starting ==="
"$VENV_PYTHON" scripts/capture_option_chains.py "$@" >> "$LOG_FILE" 2>&1
STATUS=$?
log "=== option capture finished (exit $STATUS) ==="

if [ "$STATUS" -ne 0 ]; then
    notify "Capture failed or incomplete - see logs/option_capture.log. Missed option-chain days cannot be backfilled."
fi

if [ -f "$LOG_FILE" ]; then
    tail -n 20000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

exit "$STATUS"
