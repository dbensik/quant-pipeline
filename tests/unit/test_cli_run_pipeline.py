"""
cli/run_pipeline.py — the CLI ingest entry point.

The behaviour worth testing is that it reaches core/ingest.py rather than a
second implementation. It used to build a PipelineOrchestrator over SQLite and
write a database nothing read, reporting success either way — the same defect
as the Streamlit button. A CLI that quietly diverges from the API is how that
happens again.

No network and no database: ingest_symbols is patched.

Phase 5 — retiring the SQLite pipeline
"""

import argparse
from unittest.mock import AsyncMock, patch

import pytest

from cli import run_pipeline


def args(**overrides) -> argparse.Namespace:
    defaults = {"symbols": None, "full_backfill": False, "dry_run": False}
    return argparse.Namespace(**{**defaults, **overrides})


class FakeReport:
    written = 42
    symbols = ["AAPL"]
    failed: list = []
    delisted: list = []


@pytest.mark.asyncio
async def test_the_cli_calls_core_ingest():
    """
    THE regression. The old CLI drove PipelineOrchestrator into SQLite; both
    callers now share one write path so they cannot diverge.
    """
    with (
        patch.object(run_pipeline, "ingest_symbols", new=AsyncMock(return_value=FakeReport())) as ingest,
        patch.object(run_pipeline, "get_session"),
    ):
        code = await run_pipeline.run(args(symbols=["AAPL"]))

    assert code == 0
    ingest.assert_awaited_once()
    assert ingest.await_args.kwargs["symbols"] == ["AAPL"]


@pytest.mark.asyncio
async def test_symbols_are_upper_cased_and_blanks_dropped():
    with (
        patch.object(run_pipeline, "ingest_symbols", new=AsyncMock(return_value=FakeReport())) as ingest,
        patch.object(run_pipeline, "get_session"),
    ):
        await run_pipeline.run(args(symbols=["aapl", "", "  ", "msft"]))

    assert ingest.await_args.kwargs["symbols"] == ["AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_full_backfill_is_passed_through():
    """
    It overwrites stored bars, so it must not be silently dropped — this is
    the flag that repairs split-adjustment drift.
    """
    with (
        patch.object(run_pipeline, "ingest_symbols", new=AsyncMock(return_value=FakeReport())) as ingest,
        patch.object(run_pipeline, "get_session"),
    ):
        await run_pipeline.run(args(symbols=["AAPL"], full_backfill=True))

    assert ingest.await_args.kwargs["full_backfill"] is True


@pytest.mark.asyncio
async def test_dry_run_writes_nothing():
    with (
        patch.object(run_pipeline, "ingest_symbols", new=AsyncMock()) as ingest,
        patch.object(run_pipeline, "get_session"),
    ):
        code = await run_pipeline.run(args(symbols=["AAPL"], dry_run=True))

    assert code == 0
    ingest.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_empty_registry_is_an_error_not_a_silent_success():
    """
    Reporting success on an empty run is exactly how the SQLite pipeline hid
    that it was writing nowhere.
    """
    with (
        patch.object(run_pipeline, "ingest_symbols", new=AsyncMock()) as ingest,
        patch.object(run_pipeline, "get_session"),
        patch.object(run_pipeline, "_registered_symbols", new=AsyncMock(return_value=[])),
    ):
        code = await run_pipeline.run(args())

    assert code == 1
    ingest.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_symbols_produce_a_nonzero_exit():
    class Failing(FakeReport):
        failed = ["NOPE"]

    with (
        patch.object(run_pipeline, "ingest_symbols", new=AsyncMock(return_value=Failing())),
        patch.object(run_pipeline, "get_session"),
    ):
        code = await run_pipeline.run(args(symbols=["NOPE"]))

    assert code == 1
