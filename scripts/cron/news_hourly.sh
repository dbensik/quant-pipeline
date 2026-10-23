#!/bin/bash
#
# scripts/cron/news_hourly.sh
#
# launchd wrapper for `scripts/archive_news.py --due`, hourly — see
# scripts/launchd/com.dbensik.quant-pipeline.news-hourly.plist.
#
# The 06:00 job archives every symbol's newest 50 headlines; for the busiest
# names 50 cover a few hours (NVDA 1.8 h on 2026-10-22), so this pass
# refetches only the pages that would be outrun before the next one, and
# those whose last answer was empty (step 2: Yahoo's empty answers stick for
# minutes but often clear within the hour): 138 searches in 13 s on step 2's
# first run, then about 70 an hour, nearly all of them pages that came back
# empty (decision 57 B,
# research/news-archive-2026-10-22.md, research/news-archive-gaps-2026-10-22.md).
#
# Same shape as option_capture.sh: the venv by absolute path, single instance,
# and a notification when the run fails outright. A run that overlaps the 06:00
# step is harmless: every write is insert-only.
#
# Usage:  scripts/cron/news_hourly.sh [extra args passed to the script]

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
LOG_FILE="$LOG_DIR/news_hourly.log"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $*" >> "$LOG_FILE"; }

notify() {
    # Skipped on a terminal: whoever ran it by hand is already reading the output.
    [ -t 1 ] || osascript -e "display notification \"$1\" with title \"quant-pipeline headlines\"" >/dev/null 2>&1 || true
}

if [ ! -x "$VENV_PYTHON" ]; then
    log "ABORTED: no Poetry venv python found. Run 'poetry install'."
    notify "ABORTED: no Poetry venv. Headlines not archived cannot be fetched later."
    exit 1
fi

LOCK_FILE="$LOG_DIR/.news_hourly.lock"
if [ -e "$LOCK_FILE" ] && kill -0 "$(cat "$LOCK_FILE" 2>/dev/null)" 2>/dev/null; then
    log "SKIPPED: a run is already in progress"
    exit 0
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

log "=== hourly headlines starting ==="
"$VENV_PYTHON" scripts/archive_news.py --due "$@" >> "$LOG_FILE" 2>&1
STATUS=$?
log "=== hourly headlines finished (exit $STATUS) ==="

# Exit 1 (some symbols failed, the rest stored) is in the log and in
# news_fetches; only a run that stored nothing interrupts.
if [ "$STATUS" -ge 2 ]; then
    notify "Hourly headline archive failed - see logs/news_hourly.log. Missed headlines cannot be fetched later."
fi

if [ -f "$LOG_FILE" ]; then
    tail -n 20000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

exit "$STATUS"
