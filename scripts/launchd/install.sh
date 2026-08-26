#!/bin/bash
#
# Install the daily-maintenance launchd agent, replacing the crontab entry.
#
# Both must never be active at once: two runs at 06:00 would race for the same
# lock file, and the loser logs "SKIPPED: a run is already in progress" — which
# looks like a bug rather than a duplicate schedule.
#
# Idempotent. Safe to re-run after editing the plist.
set -uo pipefail

LABEL="com.dbensik.quant-pipeline.daily-maintenance"
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC="$ROOT/scripts/launchd/$LABEL.plist"
DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

[ -f "$SRC" ] || { echo "ERROR: $SRC not found"; exit 1; }

# COPIED, not symlinked: launchd is unreliable about symlinked plists, and a
# copy also means a half-edited file in the repo cannot break the live schedule.
mkdir -p "$HOME/Library/LaunchAgents"
cp "$SRC" "$DEST"
echo "installed  $DEST"

plutil -lint "$DEST" >/dev/null || { echo "ERROR: plist is malformed"; exit 1; }

# Reload: bootout first so an edited plist actually takes effect. `|| true`
# because booting out an agent that is not loaded is an error, not a problem.
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
if launchctl bootstrap "gui/$(id -u)" "$DEST" 2>/dev/null; then
    echo "loaded     $LABEL"
else
    echo "ERROR: launchctl bootstrap failed."
    echo "  Try: launchctl bootstrap gui/$(id -u) $DEST"
    exit 1
fi

# Remove the crontab line, keeping every other entry and all comments.
if crontab -l 2>/dev/null | grep -q "daily_maintenance.sh"; then
    crontab -l 2>/dev/null \
        | grep -v "^[^#]*daily_maintenance.sh" \
        > /tmp/crontab.new.$$
    crontab /tmp/crontab.new.$$ && rm -f /tmp/crontab.new.$$
    echo "removed    crontab entry (launchd now owns the 06:00 schedule)"
else
    echo "no active crontab entry to remove"
fi

echo
echo "Verify:  launchctl print gui/$(id -u)/$LABEL | grep -E 'state|runs|last exit'"
