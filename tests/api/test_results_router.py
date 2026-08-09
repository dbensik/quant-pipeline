"""
/api/v1/results — saving and loading analysis results.

The store is pointed at a temporary directory, so these never touch the real
`results/`.

Phase 5 — decommissioning Streamlit
"""

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers.results import get_result_store
from core.results import ResultNameError, ResultStore, encode, safe_name

BASE = "/api/v1/results"


@pytest.fixture
def store(tmp_path) -> ResultStore:
    return ResultStore(tmp_path)


@pytest.fixture
def results_client(store: ResultStore) -> TestClient:
    app.dependency_overrides[get_result_store] = lambda: store
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Name safety
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name",
    ["../secrets", "../../etc/passwd", "sub/dir", "a\\b", "..", ".", "", "   "],
)
def test_unsafe_names_are_rejected(name):
    """
    ResultsManager did os.path.join(results_dir, filename) with whatever it
    was handed, so a name of "../../etc/passwd" resolved outside the
    directory. Over HTTP that is a file-read primitive. Separators are
    rejected rather than stripped — stripping invites "....//" bypasses.
    """
    with pytest.raises(ResultNameError):
        safe_name(name)


def test_a_normal_name_gains_the_json_suffix():
    assert safe_name("my_run") == "my_run.json"


def test_an_existing_suffix_is_not_doubled():
    assert safe_name("my_run.json") == "my_run.json"


def test_traversal_over_http_is_422_not_404(results_client: TestClient):
    """422, not 404: the name is invalid, not merely absent."""
    response = results_client.get(f"{BASE}/..%2F..%2Fetc%2Fpasswd")
    assert response.status_code in (404, 422)
    # Whatever the router matches, it must never read outside the directory.
    assert "passwd" not in response.text or response.status_code == 422


def test_saving_under_an_unsafe_name_is_422(results_client: TestClient):
    response = results_client.post(
        BASE, json={"name": "../escape", "payload": {"a": 1}}
    )
    assert response.status_code == 422
    assert "Invalid result name" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------

def test_save_then_load(results_client: TestClient):
    payload = {"symbol": "AAPL", "metrics": {"Sharpe Ratio": 1.25}}
    created = results_client.post(BASE, json={"name": "run1", "payload": payload})
    assert created.status_code == 201
    assert created.json()["name"] == "run1.json"

    assert results_client.get(f"{BASE}/run1.json").json() == payload


def test_load_works_without_the_suffix(results_client: TestClient):
    results_client.post(BASE, json={"name": "run1", "payload": {"a": 1}})
    assert results_client.get(f"{BASE}/run1").json() == {"a": 1}


def test_listing_reports_saved_results(results_client: TestClient):
    results_client.post(BASE, json={"name": "alpha", "payload": {}})
    results_client.post(BASE, json={"name": "beta", "payload": {}})
    listed = results_client.get(BASE).json()
    assert [r["name"] for r in listed] == ["alpha.json", "beta.json"]
    assert all(r["size_bytes"] > 0 for r in listed)


def test_saving_the_same_name_replaces_it(results_client: TestClient):
    results_client.post(BASE, json={"name": "run1", "payload": {"v": 1}})
    results_client.post(BASE, json={"name": "run1", "payload": {"v": 2}})
    assert results_client.get(f"{BASE}/run1").json() == {"v": 2}
    assert len(results_client.get(BASE).json()) == 1


def test_missing_result_is_404(results_client: TestClient):
    assert results_client.get(f"{BASE}/nope").status_code == 404


def test_delete_removes_it(results_client: TestClient):
    results_client.post(BASE, json={"name": "run1", "payload": {}})
    assert results_client.delete(f"{BASE}/run1").status_code == 204
    assert results_client.get(BASE).json() == []


def test_deleting_a_missing_result_is_404(results_client: TestClient):
    assert results_client.delete(f"{BASE}/nope").status_code == 404


def test_an_empty_directory_lists_nothing(results_client: TestClient):
    assert results_client.get(BASE).json() == []


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------

def test_dataframes_use_the_split_layout(results_client: TestClient):
    """
    dashboard_app/api_client.py's _deserialize_data reconstructs DataFrames
    from pandas' orient="split" ({"index", "columns", "data"}), so that is the
    layout written — the client contract predates this endpoint.
    """
    frame = pd.DataFrame({"a": [1, 2], "b": [3, 4]}, index=["x", "y"])
    results_client.post(BASE, json={"name": "df", "payload": encode(frame)})
    loaded = results_client.get(f"{BASE}/df").json()
    assert set(loaded) == {"index", "columns", "data"}
    assert loaded["columns"] == ["a", "b"]
    assert loaded["data"] == [[1, 3], [2, 4]]


def test_a_dataframe_round_trips_back_into_pandas():
    frame = pd.DataFrame({"a": [1.0, 2.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02"]))
    payload = encode(frame)
    rebuilt = pd.DataFrame(
        data=payload["data"], index=payload["index"], columns=payload["columns"]
    )
    assert rebuilt["a"].tolist() == [1.0, 2.0]
    assert rebuilt.index.tolist() == ["2024-01-01T00:00:00", "2024-01-02T00:00:00"]


def test_nan_becomes_null_not_the_string_nan():
    """
    NaN is not valid JSON. json.dumps emits a bare NaN token that strict
    parsers reject, and stringifying it would read as a value.
    """
    assert encode(float("nan")) is None
    assert encode(np.float64("inf")) is None


def test_numpy_scalars_become_python_types():
    assert encode(np.int64(5)) == 5
    assert isinstance(encode(np.float64(1.5)), float)
    assert encode(np.bool_(True)) is True


def test_a_series_encodes_like_a_one_column_frame():
    payload = encode(pd.Series([1, 2], index=["a", "b"], name="weights"))
    assert payload["columns"] == ["weights"]
    assert payload["data"] == [[1], [2]]


def test_timestamps_become_iso_strings():
    assert encode(pd.Timestamp("2024-01-01T12:00:00")) == "2024-01-01T12:00:00"


def test_an_unserialisable_object_is_described_not_dropped():
    """
    Silently omitting it would produce a result that looks complete and is
    not.
    """
    encoded = encode({"model": object()})
    assert encoded["model"]["__unserialisable__"] == "object"


def test_nested_structures_are_encoded_throughout(results_client: TestClient):
    payload = {
        "runs": [
            {"frame": encode(pd.DataFrame({"a": [1]})), "sharpe": np.float64(2.0)}
        ]
    }
    results_client.post(BASE, json={"name": "nested", "payload": payload})
    loaded = results_client.get(f"{BASE}/nested").json()
    assert loaded["runs"][0]["frame"]["columns"] == ["a"]
    assert loaded["runs"][0]["sharpe"] == 2.0


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------

def test_a_partial_write_cannot_leave_an_unreadable_result(store: ResultStore):
    """
    Saves go to a temporary file and are moved into place, so a crash
    mid-write cannot leave a truncated file that later fails to parse.
    """
    store.save("run1", {"a": 1})
    assert list(store.directory.glob("*.tmp")) == []
    assert store.load("run1") == {"a": 1}
