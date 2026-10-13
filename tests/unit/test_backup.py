"""
scripts/backup.py: refuses without a destination, and prunes per kind.

The dump-and-restore path needs the database container, so it is proven by
running it (2026-10-12: 13 tables, 3,087,367 rows restored and matched; a
truncated dump fails, naming each table that came back short), not here.
"""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backup.py"


def load():
    spec = importlib.util.spec_from_file_location("backup_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_no_destination_is_a_failure_not_a_silent_skip(monkeypatch, capsys):
    backup = load()
    monkeypatch.setattr(backup.settings, "backup_dir", lambda: None)
    monkeypatch.setattr("sys.argv", ["backup.py"])
    assert backup.main() == 1
    assert "QUANT_BACKUP_DIR is not set" in capsys.readouterr().out


def test_prune_keeps_the_newest_of_each_kind(tmp_path):
    backup = load()
    for day in range(1, 6):
        (tmp_path / f"quant-pipeline-db-2026-10-0{day}.dump").write_text("x")
        (tmp_path / f"quant-pipeline-files-2026-10-0{day}.tar.gz").write_text("x")
    (tmp_path / "unrelated.txt").write_text("x")
    removed = backup.prune(tmp_path, keep=2)
    assert len(removed) == 6
    left = sorted(p.name for p in tmp_path.iterdir())
    assert left == [
        "quant-pipeline-db-2026-10-04.dump",
        "quant-pipeline-db-2026-10-05.dump",
        "quant-pipeline-files-2026-10-04.tar.gz",
        "quant-pipeline-files-2026-10-05.tar.gz",
        "unrelated.txt",
    ]
