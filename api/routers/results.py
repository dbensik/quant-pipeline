"""
api/routers/results.py
Saving and loading analysis results.

Ports the Streamlit "Load/Save Results" controls. `dashboard_app/api_client.py`
has called `/results` and `/results/{filename}` since it was written — endpoints
api/main.py never implemented, so its "Remote API" data source has always
404'd. This implements them under the versioned prefix everything else uses.

JSON ON DISK, NOT PICKLE. ResultsManager stores results with pickle.dump;
serving that over HTTP would mean unpickling a file named by the caller, which
is arbitrary code execution as soon as anything can write into the results
directory. `scripts/convert_pickled_results.py` converts existing pickles once
so nothing has to unpickle in a request.

Names are validated, not sanitised: ResultsManager joined the caller's string
straight onto the directory, so "../../etc/passwd" escaped it.

Phase 5 — decommissioning Streamlit
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from config.settings import RESULTS_DIR
from core.results import ResultNameError, ResultStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/results", tags=["results"])


def get_result_store() -> ResultStore:
    """A dependency so tests can point it at a temporary directory."""
    return ResultStore(Path(RESULTS_DIR))


class ResultSummary(BaseModel):
    name: str
    size_bytes: int
    modified_at: datetime


class SaveResultRequest(BaseModel):
    name: str = Field(description="Letters, digits, dot, dash, underscore")
    payload: Any = Field(description="Any JSON-serialisable result")


class SaveResultResponse(BaseModel):
    name: str
    saved: bool = True


@router.get("", response_model=List[ResultSummary], summary="List saved results")
async def list_results(
    store: ResultStore = Depends(get_result_store),
) -> List[ResultSummary]:
    return [ResultSummary(**entry) for entry in store.list()]


@router.post(
    "",
    response_model=SaveResultResponse,
    status_code=201,
    summary="Save a result",
    responses={422: {"description": "Unusable or unsafe name"}},
)
async def save_result(
    request: SaveResultRequest,
    store: ResultStore = Depends(get_result_store),
) -> SaveResultResponse:
    """Saving over an existing name replaces it — the Streamlit form did too."""
    try:
        name = store.save(request.name, request.payload)
    except ResultNameError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not write the result: {exc}"
        ) from None
    return SaveResultResponse(name=name)


@router.get(
    "/{name}",
    summary="Load a saved result",
    responses={
        404: {"description": "No such result"},
        422: {"description": "Unusable or unsafe name"},
    },
)
async def load_result(
    name: str,
    store: ResultStore = Depends(get_result_store),
) -> Any:
    """
    Returns the payload as stored. DataFrames come back in pandas'
    `orient="split"` layout, which is what api_client's `_deserialize_data`
    already reconstructs.
    """
    try:
        payload = store.load(name)
    except ResultNameError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(
            status_code=500, detail=f"{name!r} is not readable JSON: {exc}"
        ) from None

    if payload is None:
        raise HTTPException(status_code=404, detail=f"No saved result named {name!r}.")
    return payload


@router.delete(
    "/{name}",
    status_code=204,
    summary="Delete a saved result",
    responses={404: {"description": "No such result"}},
)
async def delete_result(
    name: str,
    store: ResultStore = Depends(get_result_store),
) -> None:
    try:
        removed = store.delete(name)
    except ResultNameError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    if not removed:
        raise HTTPException(status_code=404, detail=f"No saved result named {name!r}.")
