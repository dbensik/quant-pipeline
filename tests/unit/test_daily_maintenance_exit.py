"""
scripts/cron/daily_maintenance.sh: the final line and exit code separate a
FAILED step from a data FLAG (split 2026-10-12). The runbook's liveness and
weekly drift checks parse that line.

The script runs from a copy in tmp_path with a stub interpreter
(QUANT_MAINT_PYTHON) that fakes each step's exit code, and a stub osascript,
so nothing real runs, nothing is notified, and the real log is untouched.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cron" / "daily_maintenance.sh"

# The stub exits with the code named for its step in STEP_EXITS ("name=code ..."),
# 0 for anything not listed (including the database reachability check on stdin).
STUB = """#!/bin/bash
[ "$1" = "-" ] && { cat >/dev/null; exit 0; }
step="$1"; [ "$1" = "-m" ] && step="$2"
step="$(basename "$step" .py)"
for pair in $STEP_EXITS; do
    [ "${pair%%=*}" = "$step" ] && exit "${pair#*=}"
done
exit 0
"""


def run(tmp_path: Path, step_exits: str) -> tuple[int, str]:
    root = tmp_path / "proj"
    (root / "scripts" / "cron").mkdir(parents=True)
    shutil.copy(SCRIPT, root / "scripts" / "cron" / "daily_maintenance.sh")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("python", STUB), ("osascript", "#!/bin/bash\nexit 0\n")):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "QUANT_MAINT_PYTHON": str(bin_dir / "python"),
        "STEP_EXITS": step_exits,
    }
    proc = subprocess.run(
        ["/bin/bash", str(root / "scripts" / "cron" / "daily_maintenance.sh")],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=60,
    )
    lines = (root / "logs" / "daily_maintenance.log").read_text().splitlines()
    finished = [line for line in lines if "daily maintenance finished" in line]
    assert len(finished) == 1
    return proc.returncode, finished[0].split(" - ", 1)[1]


def test_a_clean_run_exits_0_with_the_plain_line(tmp_path):
    code, line = run(tmp_path, "")
    assert (code, line) == (0, "=== daily maintenance finished (exit 0) ===")


def test_a_flagged_check_is_exit_0_with_named_flags(tmp_path):
    # CTVA 2026-10-08..12: a complete run whose only news was a data finding.
    code, line = run(tmp_path, "check_fresh_returns=1 check_missing_days=1")
    assert code == 0
    assert (
        line == "=== daily maintenance finished (exit 0) with FLAGS: missing-days fresh-return ==="
    )


@pytest.mark.parametrize(
    "exits, failed",
    [
        ("snapshot_universes=1", "snapshot"),
        ("check_fresh_returns=2", "fresh-return-check"),  # 2026-10-11: the check could not run
        ("cli.run_pipeline=1", "ingest"),
    ],
)
def test_a_failed_step_is_exit_1_and_named(tmp_path, exits, failed):
    code, line = run(tmp_path, exits)
    assert code == 1
    assert line == f"=== daily maintenance finished (exit 1) FAILED: {failed} ==="


def test_flags_ride_along_on_a_failure(tmp_path):
    code, line = run(tmp_path, "snapshot_universes=1 check_reassigned=1")
    assert code == 1
    assert line == "=== daily maintenance finished (exit 1) FAILED: snapshot; FLAGS: reassigned ==="
