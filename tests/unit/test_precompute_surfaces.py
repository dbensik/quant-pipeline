"""scripts/precompute_surfaces.py: which capture days the 06:00 job names as missed."""

from datetime import date
from pathlib import Path

from core.options_surface import CaptureFile
from scripts.precompute_surfaces import exit_code, missed_days

D = date


def cap(ticker, day, partial=False):
    return CaptureFile(ticker, day, Path(f"{ticker}/{day}.parquet"), 100, partial)


def test_a_session_without_a_complete_capture_is_missed():
    """2026-10-09: SPY traded, no chain was written for any ticker."""
    sessions = [D(2026, 10, 8), D(2026, 10, 9), D(2026, 10, 12)]
    captures = [cap(t, d) for t in ("SPY", "QQQ") for d in (D(2026, 10, 8), D(2026, 10, 12))]
    assert missed_days(sessions, captures, ("SPY", "QQQ"), {}) == [
        ("SPY", D(2026, 10, 9)),
        ("QQQ", D(2026, 10, 9)),
    ]


def test_a_partial_capture_is_still_missed_and_a_known_loss_is_not():
    sessions = [D(2026, 10, 9), D(2026, 10, 12)]
    captures = [cap("SPY", D(2026, 10, 12), partial=True)]
    lost = {"2026-10-09": "both runs crashed"}
    assert missed_days(sessions, captures, ("SPY",), lost) == [("SPY", D(2026, 10, 12))]


def test_a_pricing_failure_outranks_a_missed_day_and_a_missed_day_is_a_flag():
    assert exit_code(0, 0) == 0
    assert exit_code(0, 6) == 1  # the job reads 1 as a FLAG
    assert exit_code(1, 6) == 2  # and 2 as a failed step
