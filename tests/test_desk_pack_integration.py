"""
scripts/desk_pack.py against the real database (decision 39).

    poetry run pytest -m integration tests/test_desk_pack_integration.py
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[1]


def run(tmp_path, *args):
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "desk_pack.py"), *args, "--out", str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )


def test_a_pack_is_point_in_time_and_hashed(tmp_path):
    proc = run(tmp_path, "MSFT", "--as-of", "2020-06-30")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    manifest = json.loads((tmp_path / "pack.json").read_text())
    assert manifest["status"] == "complete"
    facts = json.loads((tmp_path / "facts.json").read_text())
    assert facts["count"] > 0
    assert max(f["filed"] for f in facts["facts"]) <= "2020-06-30", "no fact filed after as_of"
    prices = json.loads((tmp_path / "prices.json").read_text())
    assert prices["stats"]["last_day"] <= "2020-06-30", "no price after as_of"
    history = json.loads((tmp_path / "valuation_history.json").read_text())
    assert all(p["as_of"] <= "2020-06-30" for p in history["points"])
    assert "did not rank" in history["caveat"]
    from core.desk_pack import sha256_of

    for name, digest in manifest["files"].items():
        assert sha256_of(tmp_path / name) == digest


def test_a_utility_is_refused_and_nothing_else_is_gathered(tmp_path):
    proc = run(tmp_path, "DUK")
    assert proc.returncode == 3
    assert sorted(p.name for p in tmp_path.iterdir()) == ["pack.json", "screen.json"]


def test_a_ticker_outside_the_registry_says_so(tmp_path):
    proc = run(tmp_path, "ZZZZ")
    assert proc.returncode == 2
    assert json.loads((tmp_path / "pack.json").read_text())["status"] == "not-in-registry"
