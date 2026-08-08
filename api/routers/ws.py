"""
api/routers/ws.py
WebSocket endpoint streaming backtest progress.

WHY THIS AND NOT A PRICE FEED:
    There is no live market data in this system. price_data_daily ends
    2025-07-15 and bars are daily, so a "live tick" websocket would be
    replaying stale history dressed up as real time. Backtest progress is
    genuinely real-time, needs no market feed, and solves an actual problem:
    a multi-year run over 1,300+ bars otherwise leaves the UI blank with no
    indication of progress or failure.

PROTOCOL
    Client connects, then sends one JSON message matching BacktestRequest:

        {"symbol": "AAPL", "strategy_id": "ma_crossover",
         "start": "2020-01-01T00:00:00Z", "end": "2024-01-01T00:00:00Z",
         "params": {"short_window": 20, "long_window": 50}}

    Server replies with a sequence of JSON messages, each having a "type":

        {"type": "accepted", "symbol": ..., "strategy_id": ...}
        {"type": "progress", "stage": "fetching"|"running"|"summarising",
         "pct": 0-100, "detail": "..."}
        {"type": "result",  "metrics": {...}, "bars": N, ...}
        {"type": "error",   "detail": "...", "code": 404|422|500}

    The socket closes after "result" or "error". One backtest per connection —
    reconnect for another.

Phase 3 — FastAPI routers for the React UI
"""

import logging
from datetime import timezone
from typing import Any, Dict

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from alpha_models import registry
from db.repositories.market_data import TimescaleMarketDataRepo
from db.session import get_session

from api.frames import records_to_frame
from api.routers.backtest import BacktestRequest, _json_safe, _run_backtest_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ws", tags=["ws"])


async def _send_error(websocket: WebSocket, detail: str, code: int) -> None:
    await websocket.send_json({"type": "error", "detail": detail, "code": code})


@router.websocket("/backtest")
async def backtest_ws(websocket: WebSocket) -> None:
    """
    Run one backtest, streaming progress.

    NOTE: dependency injection is not used here. FastAPI's Depends() with a
    yielding dependency ties the session's lifetime to the request scope, which
    for a websocket is the whole connection — holding a DB connection open for
    the entire backtest. The session is opened and closed around the fetch only.
    """
    await websocket.accept()

    try:
        raw = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    except Exception:
        await _send_error(websocket, "Expected a JSON backtest request.", 422)
        await websocket.close()
        return

    try:
        request = BacktestRequest.model_validate(raw)
    except ValidationError as exc:
        await _send_error(websocket, f"Invalid request: {exc.errors()}", 422)
        await websocket.close()
        return

    try:
        if request.start > request.end:
            await _send_error(websocket, "`start` must not be after `end`.", 422)
            return

        try:
            spec = registry.get(request.strategy_id)
        except KeyError as exc:
            await _send_error(websocket, str(exc), 404)
            return

        if spec.input_contract == "multi":
            await _send_error(
                websocket,
                f"Strategy '{spec.id}' takes a multi-symbol frame and cannot be "
                "backtested against a single symbol.",
                422,
            )
            return

        await websocket.send_json(
            {
                "type": "accepted",
                "symbol": request.symbol,
                "strategy_id": spec.id,
                "strategy_name": spec.display_name,
            }
        )

        # -- fetch ------------------------------------------------------
        await websocket.send_json(
            {"type": "progress", "stage": "fetching", "pct": 10,
             "detail": f"Loading {request.symbol} history"}
        )

        start = (
            request.start.replace(tzinfo=timezone.utc)
            if request.start.tzinfo is None else request.start
        )
        end = (
            request.end.replace(tzinfo=timezone.utc)
            if request.end.tzinfo is None else request.end
        )

        async with get_session() as session:
            repo = TimescaleMarketDataRepo(session)
            asset = await repo.find_asset(request.symbol)
            if asset is None:
                await _send_error(
                    websocket, f"Unknown symbol: {request.symbol!r}", 404
                )
                return
            records = await repo.fetch_range(
                symbol=request.symbol, asset_class=None, start=start, end=end
            )

        frame = records_to_frame(records)
        if frame.empty:
            await _send_error(
                websocket,
                f"No bars stored for {request.symbol!r} between "
                f"{start.date()} and {end.date()}.",
                422,
            )
            return

        await websocket.send_json(
            {"type": "progress", "stage": "running", "pct": 40,
             "detail": f"Running {spec.display_name} over {len(frame)} bars"}
        )

        # -- run --------------------------------------------------------
        try:
            results, metrics, trades = await run_in_threadpool(
                _run_backtest_sync,
                frame,
                spec,
                request.params,
                request.initial_capital,
                request.transaction_cost,
                request.seed,
            )
        except ValueError as exc:
            await _send_error(websocket, str(exc), 422)
            return

        await websocket.send_json(
            {"type": "progress", "stage": "summarising", "pct": 90,
             "detail": "Computing performance metrics"}
        )

        payload: Dict[str, Any] = {
            "type": "result",
            "symbol": asset.symbol,
            "strategy_id": spec.id,
            "strategy_name": spec.display_name,
            "bars": len(frame),
            # A count, not the log itself: the trade list can run to thousands
            # of rows and the UI only shows how many there were.
            "trades": 0 if trades is None or trades.empty else len(trades),
            "metrics": {k: _json_safe(v) for k, v in (metrics or {}).items()},
            "caveat": spec.caveat,
            # Echoed for the same reason the REST response echoes them: a result
            # that does not say what produced it cannot be reproduced or
            # compared. `initial_capital` in particular is what a client needs
            # to draw a break-even line on the equity curve.
            "params": {
                p.name: request.params.get(p.name, p.default) for p in spec.params
            },
            "seed": request.seed,
            "initial_capital": request.initial_capital,
        }
        if request.include_equity_curve and results is not None and not results.empty:
            payload["equity_curve"] = [
                {"time": idx.isoformat(), "total": float(row["total"])}
                for idx, row in results.iterrows()
            ]

        await websocket.send_json(payload)

    except WebSocketDisconnect:
        # Client hung up mid-run; nothing to clean up beyond the session, which
        # its context manager already closed.
        logger.info("Websocket client disconnected during backtest.")
        return
    except Exception as exc:  # noqa: BLE001 — the socket must not die silently
        logger.exception("Unhandled error in backtest websocket: %s", exc)
        try:
            await _send_error(websocket, "Internal server error", 500)
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
