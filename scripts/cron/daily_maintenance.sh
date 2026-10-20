#!/bin/bash
#
# scripts/cron/daily_maintenance.sh
#
# Daily data maintenance, for launchd. In this order: ingest new price bars,
# the risk-free rate, SEC facts for recent filers, the read-only checks
# (reassigned tickers, missing days, fresh returns), the paper trader, the paper margin
# check, the index snapshot, new
# index members, the DCF line-concept check, option surfaces, then the backup.
#
# Ingest only fetches REGISTERED assets. This header used to say ingest ran
# first "so that any name which joined an index today already has bars"; it
# never did, and 30 S&P members went unpriced. Joiners are registered and
# backfilled by the new-member step right after the snapshot (decision 44).
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

# QUANT_MAINT_PYTHON exists for tests/unit/test_daily_maintenance_exit.py,
# which substitutes a stub that fakes each step's exit code. Unset in launchd.
VENV_PYTHON="${QUANT_MAINT_PYTHON:-$(poetry env info --path 2>/dev/null)/bin/python}"
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
# Two outcomes, kept apart on purpose (split 2026-10-12):
#   FAILED_STEPS  a step did not do its job (ingest, rate, SEC facts, a check
#                 that could not run, a snapshot). Exit 1: something may be
#                 lost, and a missed snapshot cannot be backdated.
#   FLAGS         a check RAN and found stored data it disagrees with. Exit 0:
#                 the job did its job; the finding is named on the final line
#                 and raises its own notification. From 2026-10-08 to 10-12 a
#                 single CTVA mismatch made five complete runs read as failed.
# Each holds space-separated step names, so the final line says which.
FAILED_STEPS=""
FLAGS=""
fail() { FAILED_STEPS="${FAILED_STEPS:+$FAILED_STEPS }$1"; }
flag() { FLAGS="${FLAGS:+$FLAGS }$1"; }

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
    # Notify HERE, not at the bottom. This path exits before the notification
    # block below ever runs, so until 2026-09-07 the likeliest failure this job
    # has — Docker down at 06:00 — was the one failure it could not report.
    # Measured cost: 2026-08-28 to 08-31, four consecutive mornings aborted
    # here, four index-days lost that cannot be backdated, and nothing said so.
    [ -t 1 ] || osascript -e 'display notification "daily maintenance ABORTED: database unreachable. Missed index-days cannot be backdated." with title "quant-pipeline daily maintenance"' >/dev/null 2>&1 || true
    exit 1
fi

if [ "$MODE" != "--snapshot-only" ]; then
    log "--- ingest ---"
    if "$VENV_PYTHON" -m cli.run_pipeline >> "$LOG_FILE" 2>&1; then
        log "ingest OK"
    else
        log "ingest FAILED (exit $?)"
        fail ingest
    fi
fi

# The risk-free rate (^IRX, 13-week T-bill) for option pricing and, later, the
# DCF discount rate. Insert-only; an empty fetch is a failure, because a T-bill
# series never goes the 14-day overlap without a print. See core/rates.py.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- risk-free rate ---"
    if "$VENV_PYTHON" scripts/ingest_rates.py >> "$LOG_FILE" 2>&1; then
        log "risk-free rate OK"
    else
        log "risk-free rate FAILED (exit $?)"
        fail risk-free-rate
    fi
fi

# SEC facts for the DCF (tier 3): only registry companies that filed a 10-K,
# 10-Q, 20-F or 40-F in the last SEC_REFRESH_LOOKBACK_DAYS, read from EDGAR's
# daily form index — a few requests, not the ~500 and 2 GB of a full reload.
# Insert-only. Needs SEC_USER_AGENT in .env; without it this step fails and
# says so, and nothing else in the job depends on it.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- SEC fundamentals ---"
    if "$VENV_PYTHON" scripts/load_fundamentals.py --recent >> "$LOG_FILE" 2>&1; then
        log "SEC fundamentals OK"
    else
        log "SEC fundamentals FAILED (exit $?)"
        fail sec-fundamentals
    fi
fi

# Right after ingest, because ingest is what appends another company's bars
# under a reassigned ticker. Flags only; writes nothing. On 2026-08-07 Yahoo's
# PARA key began serving a penny stock and 32 of its bars were appended to
# Paramount's history before anyone looked — found only by a manual run of
# scripts/find_successors.py. See scripts/check_reassigned.py.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- reassigned-ticker check ---"
    "$VENV_PYTHON" scripts/check_reassigned.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "reassignment check OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "reassignment check FLAGGED a series — see SUSPECT lines above"
        flag reassigned
    else
        log "reassignment check FAILED to run (exit $CHECK_EXIT)"
        fail reassigned-check
    fi
fi

# Holes INSIDE a series. Ingest re-requests the last 14 days and refills a
# recent hole by itself; this reports the ones it can no longer reach. Found
# 2026-09-25: 463 equities had silently lost 2026-08-28 a month earlier.
# Flags only; filling is `python -m cli.run_pipeline --symbols ... --start ...`.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- missing-day check ---"
    "$VENV_PYTHON" scripts/check_missing_days.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "missing-day check OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "missing-day check FLAGGED lost bars — see MISSING lines above"
        flag missing-days
    else
        log "missing-day check FAILED to run (exit $CHECK_EXIT)"
        fail missing-day-check
    fi
fi

# Returns readers see vs a fresh Yahoo fetch, last 30 sessions. The standing
# test of read-time adjustment (research/dividend-drift-plan-2026-09-27.md):
# catches an unlisted spinoff (HWM-type), a dividend Yahoo later corrected, and
# the first real test of the morning-split rule. Flags only.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- fresh-return check ---"
    "$VENV_PYTHON" scripts/check_fresh_returns.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "fresh-return check OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "fresh-return check FLAGGED returns that disagree with Yahoo — see MISMATCH lines above"
        flag fresh-return
    else
        log "fresh-return check FAILED to run (exit $CHECK_EXIT)"
        fail fresh-return-check
    fi
fi

# Paper portfolios with an attached strategy trade the sessions they have
# not (decision 50, research/paper-trader-2026-10-20.md): fills at each
# session's raw open, decided on the close before it. Before the margin check,
# so it sees today's fills. Each is then compared with its own backtest
# (decision 51). A refused fill, drift, a held session, ruin or a RESTATED
# decision is a flag; nothing trades until a strategy is attached.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- paper trader ---"
    "$VENV_PYTHON" scripts/run_paper_strategies.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "paper trader OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "paper trader FLAGGED a portfolio — see REFUSED/DRIFT/HELD/RUINED/RESTATED lines above"
        flag paper-trader
    else
        log "paper trader FAILED to run (exit $CHECK_EXIT)"
        fail paper-trader
    fi
fi

# Paper portfolios below their maintenance margin (decision 49,
# research/paper-shorts-2026-10-19.md), marked at the closes ingest just
# wrote. Flags only: a cover nobody placed is never written into a paper book.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- paper margin check ---"
    "$VENV_PYTHON" scripts/check_paper_margin.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "paper margin check OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "paper margin check FLAGGED a portfolio — see BELOW MAINTENANCE/UNPRICED lines above"
        flag paper-margin
    else
        log "paper margin check FAILED to run (exit $CHECK_EXIT)"
        fail paper-margin-check
    fi
fi

if [ "$MODE" != "--ingest-only" ]; then
    log "--- universe snapshot ---"
    if "$VENV_PYTHON" scripts/snapshot_universes.py >> "$LOG_FILE" 2>&1; then
        log "snapshot OK"
    else
        log "snapshot FAILED (exit $?)"
        fail snapshot
    fi
fi

# Register the equities that just joined an index (decision 44,
# research/new-index-members-2026-10-19.md). Right after the snapshot that
# records them; ingest only fetches REGISTERED assets, so until 2026-10-19 a
# joiner was never priced (30 S&P members had no bars). A probable rename (its
# CIK already registered), a joiner with no CIK, or a new series opening on
# zero-volume bars is FLAGGED for a human, never registered or trimmed.
# Crypto members are listed, never registered. See scripts/register_new_members.py.
if [ "$MODE" != "--ingest-only" ]; then
    log "--- new index members ---"
    "$VENV_PYTHON" scripts/register_new_members.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "new-member registration OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "new-member registration FLAGGED a joiner — see RENAME?/NO CIK/PLACEHOLDER/EMPTY lines above"
        flag new-members
    else
        log "new-member registration FAILED to run (exit $CHECK_EXIT)"
        fail new-members
    fi
fi

# DCF lines the filings may now report differently (2026-10-13): another
# concept for a valued company's newest fiscal year that moves its value, or a
# per-filer map naming concepts the filer stopped using. After the SEC refresh
# (new filings change the answer) and after the snapshot, so its ~45 s never
# delays the one step that cannot be backdated. Flags only.
# See scripts/check_line_concepts.py.
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- DCF line-concept check ---"
    "$VENV_PYTHON" scripts/check_line_concepts.py >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "line-concept check OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "line-concept check FLAGGED DCF lines — see CONCEPT or STALE lines above"
        flag line-concepts
    else
        log "line-concept check FAILED to run (exit $CHECK_EXIT)"
        fail line-concept-check
    fi
fi

# Option surfaces for the last week's captures (2026-10-13), so the options
# page answers from the cache instead of pricing for 10-35 s; and missed
# capture days named, because 2026-10-09 was lost for every ticker and nobody
# knew for four days. A surface that fails to price is a failure; a missed
# day is a flag (the capture job, not this one, lost it).
if [ "$MODE" != "--snapshot-only" ]; then
    log "--- option surfaces ---"
    "$VENV_PYTHON" scripts/precompute_surfaces.py --days 7 >> "$LOG_FILE" 2>&1
    CHECK_EXIT=$?
    if [ "$CHECK_EXIT" -eq 0 ]; then
        log "option surfaces OK"
    elif [ "$CHECK_EXIT" -eq 1 ]; then
        log "option surfaces OK; FLAGGED missed capture days — see MISSED CAPTURE lines above"
        flag missed-captures
    else
        log "option surfaces FAILED (exit $CHECK_EXIT)"
        fail option-surfaces
    fi
fi

# LAST, so the backup holds today's snapshot and bars. The database and the
# option-chain archive to QUANT_BACKUP_DIR (Google Drive), each dump proven by
# restoring it into a scratch database first. Until 2026-10-12 nothing was
# backed up, and most of what this project stores cannot be re-fetched.
log "--- backup ---"
if "$VENV_PYTHON" scripts/backup.py >> "$LOG_FILE" 2>&1; then
    log "backup OK"
else
    log "backup FAILED (exit $?)"
    fail backup
fi

# The final line is parsed by the runbook's liveness and weekly drift checks:
#   finished (exit 0) ===                       clean
#   finished (exit 0) with FLAGS: <checks> ===  ran fully; data findings to read
#   finished (exit 1) FAILED: <steps> ===       a step failed (FLAGS appended if any)
if [ -n "$FAILED_STEPS" ]; then
    EXIT=1
    log "=== daily maintenance finished (exit 1) FAILED: $FAILED_STEPS${FLAGS:+; FLAGS: $FLAGS} ==="
elif [ -n "$FLAGS" ]; then
    EXIT=0
    log "=== daily maintenance finished (exit 0) with FLAGS: $FLAGS ==="
else
    EXIT=0
    log "=== daily maintenance finished (exit 0) ==="
fi

# Surface a failure where a human will actually see it.
#
# A non-zero exit is necessary but not sufficient: under cron it went to
# /dev/null, and under launchd it goes to a log nobody opens. dow_jones failed
# every morning from 2026-08-16 and the only trace was one line in a 20k-line
# file. Since a missed snapshot is unrecoverable, the failure has to interrupt.
#
# Skipped when stdout is a terminal, so running this by hand does not fire a
# notification at the person who just ran it and is already reading the output.
if [ -n "$FAILED_STEPS" ] && [ ! -t 1 ]; then
    osascript -e 'display notification "Snapshot, ingest or a check failed - see logs/daily_maintenance.log. Missed index-days cannot be backdated." with title "quant-pipeline daily maintenance"' >/dev/null 2>&1 || true
fi

if [ -n "$FLAGS" ] && [ ! -t 1 ]; then
    osascript -e 'display notification "A check flagged stored data - see SUSPECT, MISSING, MISMATCH, CONCEPT, STALE or MISSED CAPTURE in logs/daily_maintenance.log. Nothing was changed." with title "quant-pipeline daily maintenance"' >/dev/null 2>&1 || true
fi

# Keep the log from growing without bound; 30 days is plenty to notice a
# pattern of failures.
if [ -f "$LOG_FILE" ]; then
    tail -n 20000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

exit "$EXIT"
