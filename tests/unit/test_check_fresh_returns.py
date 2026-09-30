"""scripts/check_fresh_returns.py — the check must not pass on data it never got.

On 2026-09-29 and 09-30 launchd's 256 open-file limit made yfinance drop 219
and 200 of 516 symbols, and the check exited 0 both mornings.
"""

import subprocess
import sys
from pathlib import Path

from scripts.check_fresh_returns import MAX_NO_DATA, exit_code

ROOT = Path(__file__).resolve().parents[2]


def test_a_mass_fetch_failure_is_not_a_pass():
    # The 2026-09-30 morning: nothing mismatched, because 200 were never compared.
    assert exit_code(mismatches=0, uncovered=0, no_data=200) == 2


def test_a_few_unserved_symbols_are_tolerated():
    assert exit_code(mismatches=0, uncovered=0, no_data=MAX_NO_DATA) == 0
    assert exit_code(mismatches=0, uncovered=0, no_data=MAX_NO_DATA + 1) == 2


def test_mismatches_and_uncovered_bars_still_flag():
    assert exit_code(mismatches=3, uncovered=0, no_data=0) == 1
    assert exit_code(mismatches=0, uncovered=1, no_data=0) == 1


def test_open_file_limit_is_raised_from_launchds_256():
    # A child process, so lowering the limit cannot leak into the test run.
    code = (
        "import resource\n"
        "resource.setrlimit(resource.RLIMIT_NOFILE, (256, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))\n"
        "from scripts.check_fresh_returns import raise_open_file_limit, WANT_OPEN_FILES\n"
        "assert raise_open_file_limit() >= min(WANT_OPEN_FILES, resource.getrlimit(resource.RLIMIT_NOFILE)[1])\n"
        "assert resource.getrlimit(resource.RLIMIT_NOFILE)[0] > 256\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
