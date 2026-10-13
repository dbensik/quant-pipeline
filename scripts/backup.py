"""
scripts/backup.py

Back up what cannot be re-fetched: the whole database and the option-chain
archive, to QUANT_BACKUP_DIR (a synced Google Drive folder; see .env.example).
The last step of the 06:00 job.

    python scripts/backup.py              # dump, verify by restoring, copy, prune
    python scripts/backup.py --no-verify  # skip the restore check (faster)

Until 2026-10-12 nothing was backed up: no Time Machine destination, `data/`
gitignored, the database in one Docker volume. A dead disk would have lost the
option-chain archive (yfinance serves only today's chain), every universe
snapshot (a missed index-day cannot be backdated), the prices as Yahoo served
them, the legacy names it no longer serves, and the decisions recorded on each
asset.

A backup is only proven by a restore, so each dump is restored into a scratch
database in the same container and every table's row count compared with the
live one before the files are copied out. A dump that does not restore is a
failure, and the previous backups are kept. TimescaleDB needs
timescaledb_pre_restore()/post_restore() around pg_restore; restoring on a
fresh machine needs the same TimescaleDB version (the docker-compose image).

Writes `<dir>/daily/quant-pipeline-db-<date>.dump` (pg_dump custom format,
from the container's own pg_dump, so pg_restore 15+ reads it) and
`quant-pipeline-files-<date>.tar.gz`; keeps BACKUP_KEEP_DAILY of each, and the
first of each month in `<dir>/monthly/` for BACKUP_KEEP_MONTHLY months.
Exit 0 on success, 1 on any failure (named on the last line).

To restore: run the compose container, then in it create a database,
`CREATE EXTENSION timescaledb; SELECT timescaledb_pre_restore();`,
`pg_restore -U quant -d <db> --no-owner < quant-pipeline-db-<date>.dump`, and
`SELECT timescaledb_post_restore();` (what verify() below does). Untar the
files archive at the project root.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings  # noqa: E402

CHECK_DB = "quant_restore_check"
KINDS = (("db", ".dump"), ("files", ".tar.gz"))


def in_container(command: str, stdin=None, stdout=None) -> subprocess.CompletedProcess:
    """Run a shell command inside the database container as its own user."""
    return subprocess.run(
        ["docker", "exec", "-i", settings.BACKUP_DB_CONTAINER, "sh", "-c", command],
        stdin=stdin,
        stdout=stdout if stdout is not None else subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=1800,
    )


def psql(sql: str, db: str = '"$POSTGRES_DB"') -> str:
    r = in_container(f'psql -U "$POSTGRES_USER" -d {db} -At -v ON_ERROR_STOP=1 -c "{sql}"')
    if r.returncode:
        raise RuntimeError(f"psql failed: {r.stderr.decode()[-400:]}")
    return r.stdout.decode()


def table_counts(db: str) -> dict:
    tables = psql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1", db
    ).split()
    return {t: int(psql(f"SELECT count(*) FROM public.{t}", db)) for t in tables}


def dump(path: Path) -> None:
    with path.open("wb") as out:
        r = in_container('pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc', stdout=out)
    if r.returncode or path.stat().st_size == 0:
        raise RuntimeError(f"pg_dump failed (exit {r.returncode}): {r.stderr.decode()[-400:]}")


def verify(path: Path) -> str:
    """Restore into a scratch database and compare row counts with the live one."""
    live = table_counts('"$POSTGRES_DB"')
    psql(f"DROP DATABASE IF EXISTS {CHECK_DB}")
    psql(f"CREATE DATABASE {CHECK_DB}")
    try:
        psql(
            "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();",
            CHECK_DB,
        )
        with path.open("rb") as src:
            r = in_container(f'pg_restore -U "$POSTGRES_USER" -d {CHECK_DB} --no-owner', stdin=src)
        psql("SELECT timescaledb_post_restore();", CHECK_DB)
        restored = table_counts(CHECK_DB)
    finally:
        psql(f"DROP DATABASE IF EXISTS {CHECK_DB}")
    # pg_restore of a TimescaleDB dump reports harmless catalog warnings and
    # may exit 1 with the data intact, so the row counts are the test.
    wrong = {t: (n, restored.get(t)) for t, n in live.items() if restored.get(t) != n}
    if wrong:
        detail = ", ".join(f"{t} live {a} restored {b}" for t, (a, b) in sorted(wrong.items()))
        raise RuntimeError(
            f"restore check failed: {detail} (pg_restore exit {r.returncode}: "
            f"{r.stderr.decode()[-300:]})"
        )
    return f"{len(live)} tables, {sum(live.values()):,} rows match"


def archive_files(path: Path) -> int:
    count = 0
    with tarfile.open(path, "w:gz") as tar:
        for rel in settings.BACKUP_FILES:
            src = settings.ROOT_DIR / rel
            if src.exists():
                tar.add(src, arcname=rel)
                count += sum(1 for _ in src.rglob("*")) if src.is_dir() else 1
    return count


def prune(folder: Path, keep: int) -> list:
    removed = []
    for kind, ext in KINDS:
        files = sorted(folder.glob(f"quant-pipeline-{kind}-*{ext}"))
        for old in files[:-keep] if keep else files:
            old.unlink()
            removed.append(old.name)
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--no-verify", action="store_true")
    args = parser.parse_args()

    dest = settings.backup_dir()
    if dest is None:
        print("FAILED: QUANT_BACKUP_DIR is not set in .env; nothing was backed up.")
        return 1
    if not dest.parent.exists():
        print(f"FAILED: {dest.parent} does not exist (is the cloud drive mounted?).")
        return 1
    daily, monthly = dest / "daily", dest / "monthly"
    daily.mkdir(parents=True, exist_ok=True)
    monthly.mkdir(parents=True, exist_ok=True)

    today = date.today().isoformat()
    names = {kind: f"quant-pipeline-{kind}-{today}{ext}" for kind, ext in KINDS}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            staged = {kind: Path(tmp) / name for kind, name in names.items()}
            dump(staged["db"])
            print(f"dump: {staged['db'].stat().st_size / 1e6:.1f} MB")
            if not args.no_verify:
                print(f"restore check: {verify(staged['db'])}")
            print(f"files: {archive_files(staged['files'])} entries")
            # Copied out only after the check, so a bad dump never replaces
            # (or, through pruning, removes) a good one.
            for kind, src in staged.items():
                shutil.copy2(src, daily / names[kind])
                if not any(monthly.glob(f"quant-pipeline-{kind}-{today[:7]}-*")):
                    shutil.copy2(src, monthly / names[kind])
    except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print(f"FAILED: {exc}")
        return 1

    removed = prune(daily, settings.BACKUP_KEEP_DAILY) + prune(
        monthly, settings.BACKUP_KEEP_MONTHLY
    )
    if removed:
        print(f"pruned: {', '.join(removed)}")
    print(f"backup OK: {daily}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
