"""
core/results.py
Saving and loading analysis results as JSON.

WHY NOT PICKLE. core/persistence/results_manager.py stores results with
`pickle.dump`. That is fine for a local script and wrong for an HTTP API in
two ways: unpickling executes arbitrary code, so an endpoint that loads a
pickle by name is remote code execution the moment anything can write into
the results directory; and a pickle is unreadable to the React frontend.

`dashboard_app/api_client.py` already assumed JSON — its `_deserialize_data`
reconstructs DataFrames from pandas' `orient="split"` layout — so JSON on the
wire was always the intended contract. This module makes it the on-disk
format too, and `scripts/convert_pickled_results.py` converts existing
pickles once, rather than leaving pickle in the request path.

DataFrames are encoded exactly as `orient="split"` ({"index", "columns",
"data"}) so the existing client deserialiser works unchanged.

Phase 5 — decommissioning Streamlit
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

SUFFIX = ".json"

#: Letters, digits, dash, underscore, dot — and nothing else. Applied to the
#: caller-supplied name BEFORE it touches the filesystem.
SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


class ResultNameError(ValueError):
    """The supplied name is unusable or unsafe."""


def safe_name(name: str) -> str:
    """
    Validate a result name and return it with a .json suffix.

    PATH TRAVERSAL IS THE POINT. ResultsManager did
    `os.path.join(self.results_dir, filename)` with whatever it was handed, so
    a name of "../../etc/passwd" resolves outside the directory. Over HTTP
    that is a file-read primitive, so separators are rejected outright rather
    than stripped — stripping invites "....//" style bypasses.
    """
    candidate = (name or "").strip()
    if not candidate:
        raise ResultNameError("A result needs a name.")
    if candidate.endswith(SUFFIX):
        candidate = candidate[: -len(SUFFIX)]
    if not SAFE_NAME.match(candidate):
        raise ResultNameError(
            f"Invalid result name {name!r}. Use letters, digits, dot, dash "
            "and underscore only — no path separators."
        )
    if candidate in (".", ".."):
        raise ResultNameError(f"Invalid result name {name!r}.")
    return candidate + SUFFIX


def encode(value: Any) -> Any:
    """
    Recursively make a result JSON-serialisable.

    DataFrames become pandas' `split` layout, which is what
    dashboard_app/api_client.py's `_deserialize_data` already reconstructs.
    """
    if isinstance(value, pd.DataFrame):
        frame = value.copy()
        # A DatetimeIndex must become strings or json.dump rejects the keys.
        if isinstance(frame.index, pd.DatetimeIndex):
            frame.index = frame.index.map(lambda t: t.isoformat())
        frame.columns = [str(c) for c in frame.columns]
        return {
            "index": [encode(i) for i in frame.index.tolist()],
            "columns": list(frame.columns),
            "data": [[encode(v) for v in row] for row in frame.to_numpy().tolist()],
        }
    if isinstance(value, pd.Series):
        # As a one-column frame, so it round-trips through the same reader.
        return encode(value.to_frame(name=value.name or "value"))
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return [encode(v) for v in value.tolist()]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        number = float(value)
        # NaN and Infinity are not JSON; null round-trips and reads correctly
        # as "missing" rather than becoming the string "NaN".
        return None if (number != number or number in (float("inf"), float("-inf"))) else number
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float):
        return None if (value != value or value in (float("inf"), float("-inf"))) else value
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [encode(v) for v in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    # Anything else (a strategy object, a matplotlib figure) is described
    # rather than dropped silently.
    return {"__unserialisable__": type(value).__name__, "repr": repr(value)[:200]}


class ResultStore:
    """JSON-backed result storage in one directory."""

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)

    def _path(self, name: str) -> Path:
        path = (self.directory / safe_name(name)).resolve()
        # Belt and braces: even with the name validated, confirm the resolved
        # path is inside the directory. A symlinked results dir would
        # otherwise still be escapable.
        root = self.directory.resolve()
        if root not in path.parents:
            raise ResultNameError("Resolved outside the results directory.")
        return path

    def list(self) -> List[Dict[str, Any]]:
        if not self.directory.is_dir():
            return []
        entries = []
        for path in sorted(self.directory.glob(f"*{SUFFIX}")):
            stat = path.stat()
            entries.append(
                {
                    "name": path.name,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).astimezone(),
                }
            )
        return entries

    def save(self, name: str, payload: Any) -> str:
        path = self._path(name)
        self.directory.mkdir(parents=True, exist_ok=True)
        # Written to a temporary file and moved, so a crash mid-write cannot
        # leave a truncated result that later fails to parse.
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(encode(payload), indent=2))
        temporary.replace(path)
        return path.name

    def load(self, name: str) -> Optional[Any]:
        path = self._path(name)
        if not path.is_file():
            return None
        return json.loads(path.read_text())

    def delete(self, name: str) -> bool:
        path = self._path(name)
        if not path.is_file():
            return False
        path.unlink()
        return True
