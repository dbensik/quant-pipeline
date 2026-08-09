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

import asyncio
import logging
from datetime import timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from alpha_models import registry
from db.repositories.market_data import TimescaleMarketDataRepo
from db.session import get_session

from api.frames import frames_to_wide_close, records_to_frame
from api.routers.backtest import BacktestRequest, _json_safe, _run_backtest_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ws", tags=["ws"])


async def _send_error(websocket: WebSocket, detail: str, code: int) -> None:
    await websocket.send_json({"type": "error", "detail": detail, "code": code})


class _ProgressBridge:
    """
    Carries progress out of a worker thread and onto the socket.

    The backtest endpoint below can send progress directly, because it only
    reports between stages — it is on the event loop at those points. Grid
    search and Monte Carlo report from INSIDE the threadpool worker, where
    `await websocket.send_json(...)` is not available and calling it would
    touch the loop from the wrong thread.

    So the worker only ever does a `call_soon_threadsafe` put, and an async
    drain task owns the socket. Progress is coalesced rather than queued
    without bound: a 1,000-combination grid emits 1,000 updates, and a slow
    client must not be able to make the queue grow without limit.
    """

    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._loop = asyncio.get_running_loop()
        self._latest: Optional[Dict[str, Any]] = None
        self._event = asyncio.Event()
        self._done = False
        self._task: Optional[asyncio.Task] = None

    def publish(self, message: Dict[str, Any]) -> None:
        """Called FROM THE WORKER THREAD. Must not touch the loop directly."""
        self._loop.call_soon_threadsafe(self._set, message)

    def _set(self, message: Dict[str, Any]) -> None:
        self._latest = message
        self._event.set()

    async def _drain(self) -> None:
        while True:
            await self._event.wait()
            self._event.clear()
            message, self._latest = self._latest, None
            if message is not None:
                try:
                    await self._websocket.send_json(message)
                except Exception:
                    return
            if self._done:
                return

    async def __aenter__(self) -> "_ProgressBridge":
        self._task = asyncio.create_task(self._drain())
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        self._done = True
        self._event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                self._task.cancel()


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


# ---------------------------------------------------------------------------
# Optimization
# ---------------------------------------------------------------------------
# Same protocol as /backtest above — accepted / progress / result / error —
# but progress here is per-combination and originates in the worker thread,
# so it goes through _ProgressBridge.


@router.websocket("/optimize/strategy")
async def optimize_strategy_ws(websocket: WebSocket) -> None:
    """
    Grid-search one strategy's parameters, streaming per-combination progress.

    Client sends one JSON message matching StrategyOptimizeRequest. This is
    the endpoint the grid search is really meant for: a few hundred
    combinations is a few hundred backtests, which takes minutes.
    """
    from api.routers.optimize import (
        StrategyOptimizeRequest,
        _optimize_sync,
        build_optimize_payload,
        validate_strategy_request,
    )

    await websocket.accept()

    try:
        raw = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    except Exception:
        await _send_error(websocket, "Expected a JSON optimization request.", 422)
        await websocket.close()
        return

    try:
        request = StrategyOptimizeRequest.model_validate(raw)
    except ValidationError as exc:
        await _send_error(websocket, f"Invalid request: {exc.errors()}", 422)
        await websocket.close()
        return

    try:
        try:
            spec, combos = validate_strategy_request(request)
        except HTTPException as exc:
            await _send_error(websocket, str(exc.detail), exc.status_code)
            return

        await websocket.send_json(
            {
                "type": "accepted",
                "symbol": request.symbol,
                "strategy_id": spec.id,
                "strategy_name": spec.display_name,
                "combinations": len(combos),
                "metric": request.metric,
            }
        )

        # -- fetch ------------------------------------------------------
        await websocket.send_json(
            {"type": "progress", "stage": "fetching", "pct": 5,
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
                await _send_error(websocket, f"Unknown symbol: {request.symbol!r}", 404)
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

        # -- search -----------------------------------------------------
        async with _ProgressBridge(websocket) as bridge:
            def on_progress(completed: int, total: int) -> None:
                # Runs in the threadpool worker. 5% is fetching, 95% the
                # search, so the bar never jumps backwards.
                bridge.publish(
                    {
                        "type": "progress",
                        "stage": "running",
                        "pct": 5 + int(90 * completed / max(total, 1)),
                        "detail": f"Combination {completed} of {total}",
                        "completed": completed,
                        "total": total,
                    }
                )

            optimizer = await run_in_threadpool(
                _optimize_sync, frame, spec.id, combos, request, on_progress
            )

        if optimizer.results_df is None or optimizer.results_df.empty:
            reasons = {entry["reason"] for entry in optimizer.skipped}
            await _send_error(
                websocket,
                f"All {len(combos)} combinations were rejected by '{spec.id}': "
                f"{'; '.join(sorted(reasons)) or 'no results'}",
                422,
            )
            return

        await websocket.send_json(
            {"type": "progress", "stage": "summarising", "pct": 97,
             "detail": "Ranking results"}
        )

        payload = build_optimize_payload(
            optimizer, spec, frame, combos, request, start, end
        )
        payload["type"] = "result"
        payload["start"] = start.isoformat()
        payload["end"] = end.isoformat()
        await websocket.send_json(payload)

    except WebSocketDisconnect:
        logger.info("Websocket client disconnected during optimization.")
        return
    except Exception as exc:  # noqa: BLE001 — the socket must not die silently
        logger.exception("Unhandled error in optimization websocket: %s", exc)
        try:
            await _send_error(websocket, "Internal server error", 500)
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


@router.websocket("/optimize/portfolio")
async def optimize_portfolio_ws(websocket: WebSocket) -> None:
    """
    Monte Carlo weight search, streaming progress.

    PortfolioOptimizer.simulate_random_portfolios already takes a callback and
    invokes it every 100 trials — from inside the worker thread, so it goes
    through the same bridge.
    """
    from api.routers.optimize import (
        MAX_SYMBOLS,
        PortfolioOptimizeRequest,
        _simulate_sync,
        build_portfolio_payload,
    )

    await websocket.accept()

    try:
        raw = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    except Exception:
        await _send_error(websocket, "Expected a JSON optimization request.", 422)
        await websocket.close()
        return

    try:
        request = PortfolioOptimizeRequest.model_validate(raw)
    except ValidationError as exc:
        await _send_error(websocket, f"Invalid request: {exc.errors()}", 422)
        await websocket.close()
        return

    try:
        if request.start > request.end:
            await _send_error(websocket, "`start` must not be after `end`.", 422)
            return
        if len(request.symbols) < 2:
            await _send_error(
                websocket,
                "Weight optimization needs at least 2 symbols; "
                f"{len(request.symbols)} given.",
                422,
            )
            return
        if len(request.symbols) > MAX_SYMBOLS:
            await _send_error(
                websocket,
                f"{len(request.symbols)} symbols requested; the limit is {MAX_SYMBOLS}.",
                422,
            )
            return

        await websocket.send_json(
            {
                "type": "accepted",
                "symbols": list(request.symbols),
                "num_portfolios": request.num_portfolios,
            }
        )

        start = (
            request.start.replace(tzinfo=timezone.utc)
            if request.start.tzinfo is None else request.start
        )
        end = (
            request.end.replace(tzinfo=timezone.utc)
            if request.end.tzinfo is None else request.end
        )

        await websocket.send_json(
            {"type": "progress", "stage": "fetching", "pct": 5,
             "detail": f"Loading {len(request.symbols)} price histories"}
        )

        price_data: Dict[str, Any] = {}
        async with get_session() as session:
            repo = TimescaleMarketDataRepo(session)
            for symbol in request.symbols:
                asset = await repo.find_asset(symbol)
                if asset is None:
                    await _send_error(websocket, f"Unknown symbol: {symbol!r}", 404)
                    return
                records = await repo.fetch_range(
                    symbol=symbol, asset_class=None, start=start, end=end
                )
                frame = records_to_frame(records)
                if frame.empty:
                    await _send_error(
                        websocket,
                        f"No bars stored for {symbol!r} between "
                        f"{start.date()} and {end.date()}.",
                        422,
                    )
                    return
                price_data[symbol] = frame

        prices = frames_to_wide_close(price_data).dropna()
        if len(prices) < 3:
            await _send_error(
                websocket,
                f"Only {len(prices)} dates are common to all symbols; at least "
                "3 are needed to estimate a covariance matrix.",
                422,
            )
            return

        async with _ProgressBridge(websocket) as bridge:
            def on_progress(fraction: float) -> None:
                bridge.publish(
                    {
                        "type": "progress",
                        "stage": "running",
                        "pct": 5 + int(90 * fraction),
                        "detail": (
                            f"{int(fraction * request.num_portfolios):,} of "
                            f"{request.num_portfolios:,} portfolios"
                        ),
                    }
                )

            results_df = await run_in_threadpool(
                _simulate_sync, prices, request, on_progress
            )

        payload = build_portfolio_payload(results_df, prices, request, start, end)
        payload = {
            "type": "result",
            **{
                key: (
                    value.model_dump()
                    if hasattr(value, "model_dump")
                    else [v.model_dump() for v in value]
                    if isinstance(value, list) and value and hasattr(value[0], "model_dump")
                    else value
                )
                for key, value in payload.items()
            },
        }
        payload["start"] = start.isoformat()
        payload["end"] = end.isoformat()
        await websocket.send_json(payload)

    except WebSocketDisconnect:
        logger.info("Websocket client disconnected during portfolio optimization.")
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled error in portfolio optimization websocket: %s", exc)
        try:
            await _send_error(websocket, "Internal server error", 500)
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
