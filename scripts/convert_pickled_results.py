"""
scripts/convert_pickled_results.py

One-time migration: converts pickled results in `results/` to JSON so the API
never has to unpickle in a request.

    python scripts/convert_pickled_results.py --dry-run
    python scripts/convert_pickled_results.py

WHY THIS IS A SCRIPT AND NOT AN ENDPOINT
    `pickle.load` executes arbitrary code in the payload. An API route that
    unpickled a file named by the caller would be remote code execution as
    soon as anything could write into the results directory. Converting once,
    deliberately, from a terminal you already control keeps that out of the
    request path entirely.

    Run this ONLY on pickles you created. Do not point it at files from
    anywhere else — the same warning applies to the conversion itself.

Existing `.pkl` files are left in place; nothing is deleted. A `.json` that
already exists is skipped, so re-running is safe.
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import RESULTS_DIR  # noqa: E402
from core.results import encode  # noqa: E402


def convert(directory: Path, dry_run: bool) -> int:
    if not directory.is_dir():
        print(f"No directory at {directory} — nothing to convert.")
        return 0

    pickles = sorted(directory.glob("*.pkl"))
    if not pickles:
        print(f"No .pkl files in {directory}.")
        return 0

    converted = skipped = failed = 0
    for path in pickles:
        target = path.with_suffix(".json")
        if target.exists():
            print(f"  SKIP {path.name}: {target.name} already exists")
            skipped += 1
            continue

        try:
            with path.open("rb") as handle:
                payload = pickle.load(handle)
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {path.name}: {exc}")
            failed += 1
            continue

        encoded = encode(payload)
        described = _describe(encoded)
        print(f"  {path.name} -> {target.name} ({described})")

        if dry_run:
            continue

        target.write_text(json.dumps(encoded, indent=2))
        converted += 1

    verb = "Would convert" if dry_run else "Converted"
    print(f"\n{verb} {len(pickles) - skipped - failed} file(s); "
          f"skipped {skipped}, failed {failed}.")
    if not dry_run and converted:
        print("The original .pkl files are untouched; remove them when satisfied.")
    return 1 if failed else 0


def _describe(encoded) -> str:
    if isinstance(encoded, dict):
        if all(k in encoded for k in ("index", "columns", "data")):
            return f"DataFrame {len(encoded['index'])}x{len(encoded['columns'])}"
        return f"object with keys {sorted(encoded)[:6]}"
    if isinstance(encoded, list):
        return f"list of {len(encoded)}"
    return type(encoded).__name__


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=Path(RESULTS_DIR))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return convert(args.path, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
