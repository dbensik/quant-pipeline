"""
Router test harness.

These tests exercise the FastAPI routers WITHOUT a database. `pytest tests/`
must stay runnable with no Docker and no TimescaleDB — it has been all along,
and a router suite that needs a live database would be skipped exactly when it
matters.

The seam is the repository Protocol. db/repositories/market_data.py says it
outright: "Swap TimescaleMarketDataRepo for an in-memory stub in unit tests by
satisfying the same Protocol — no monkey-patching or mocking needed." FakeRepo
below is that stub, and FastAPI's dependency_overrides installs it.

TestClient is used rather than httpx + ASGITransport for two reasons: it is
synchronous, so none of this needs pytest-asyncio or a change to asyncio_mode
(which would alter collection for every existing test), and it is the only
clean way to drive the websocket route.

NOTE: the client is deliberately NOT used as a context manager. That would run
the app's lifespan, which assigns an async engine to app.state — harmless in
itself, but it is the path toward tests that quietly depend on a real database.

Phase 3/4 — API router tests
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_market_data_repo
from api.main import app
from core.models import OHLCV, Asset, MarketDataRecord, Timestamp

# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

START = datetime(2024, 1, 1, tzinfo=timezone.utc)
N_BARS = 400

KNOWN_ASSETS: Dict[str, Asset] = {
    "AAPL": Asset(symbol="AAPL", asset_class="equity", source="yfinance",
                  metadata={"sector": "Information Technology"}),
    "BTC-USD": Asset(symbol="BTC-USD", asset_class="crypto", source="yfinance"),
    # Decorrelated from the other two, which share one price formula and
    # differ only in `base` — so their RETURNS are identical and every
    # weighting of them has the same volatility. That makes portfolio-weight
    # tests vacuous: a frontier over AAPL + BTC-USD is a single point. MSFT
    # follows an independent seeded path so diversification is expressible.
    "MSFT": Asset(symbol="MSFT", asset_class="equity", source="yfinance",
                  metadata={"sector": "Information Technology"}),
    # Registered but with zero bars — mirrors the five padding-only crypto
    # tickers the migration found, and makes "unknown symbol" (404) vs "known
    # symbol, no data" (200 + empty) a testable distinction.
    "EMPTY-USD": Asset(symbol="EMPTY-USD", asset_class="crypto", source="yfinance"),
}


def _decorrelated_closes() -> List[float]:
    """
    A seeded random walk, independent of the shared sawtooth path.

    Seeded rather than random: these fixtures must produce the same bars on
    every run, or the determinism tests they feed would be testing the
    fixture instead of the code.
    """
    import numpy as np

    rng = np.random.default_rng(20260809)
    steps = rng.normal(0.0006, 0.011, N_BARS)
    return [float(v) for v in 150.0 * np.cumprod(1 + steps)]


def _series(symbol: str) -> List[MarketDataRecord]:
    """
    Deterministic trending-then-oscillating closes.

    Shaped so real strategies actually produce signal changes: a pure ramp
    makes mean reversion emit nothing and every "did the parameters change the
    output?" assertion vacuously true.
    """
    if symbol == "EMPTY-USD":
        return []

    if symbol == "MSFT":
        closes = _decorrelated_closes()
    else:
        base = 100.0 if symbol == "AAPL" else 30_000.0
        closes = [
            base * (1 + 0.0008 * i) + base * 0.02 * ((i % 20) - 10) / 10
            for i in range(N_BARS)
        ]

    records: List[MarketDataRecord] = []
    for i in range(N_BARS):
        close = closes[i]
        records.append(
            MarketDataRecord(
                asset=KNOWN_ASSETS[symbol],
                ohlcv=OHLCV(
                    open=close * 0.995,
                    high=close * 1.01,
                    low=close * 0.99,
                    close=close,
                    volume=1_000_000.0,
                    timestamp=Timestamp(utc=START + timedelta(days=i)),
                ),
            )
        )
    return records


# ---------------------------------------------------------------------------
# Fake repository
# ---------------------------------------------------------------------------

class FakeRepo:
    """
    In-memory MarketDataRepository.

    It genuinely filters by symbol, date range, asset_class and source. A stub
    that returned a canned list regardless of arguments would make the
    "no bars in this range" test pass for the wrong reason.
    """

    def __init__(self) -> None:
        self.data = {symbol: _series(symbol) for symbol in KNOWN_ASSETS}

    async def write(self, records: List[MarketDataRecord]) -> None:  # pragma: no cover
        raise NotImplementedError("Router tests never write.")

    async def find_asset(
        self, symbol: str, asset_class: Optional[str] = None
    ) -> Optional[Asset]:
        asset = KNOWN_ASSETS.get(symbol)
        if asset is None:
            return None
        if asset_class and asset.asset_class != asset_class:
            return None
        return asset

    async def fetch_range(
        self,
        symbol: str,
        asset_class: Optional[str],
        start: datetime,
        end: datetime,
        source: Optional[str] = None,
    ) -> List[MarketDataRecord]:
        records = self.data.get(symbol, [])
        out = []
        for record in records:
            # asset_class=None means "any" — the semantics the API relies on so
            # callers need not know an asset's class to request its bars.
            if asset_class and record.asset.asset_class != asset_class:
                continue
            if source and record.asset.source != source:
                continue
            if start <= record.ohlcv.timestamp.utc <= end:
                out.append(record)
        return out


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def repo() -> FakeRepo:
    return FakeRepo()


@pytest.fixture
def client(repo: FakeRepo) -> TestClient:
    """TestClient with the market-data repository replaced by the fake."""
    app.dependency_overrides[get_market_data_repo] = lambda: repo
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def ws_client(repo: FakeRepo, monkeypatch) -> TestClient:
    """
    TestClient for the websocket route.

    ws.py does not use Depends() — it opens a session itself — so
    dependency_overrides cannot reach it.

    Both patches target `api.routers.ws.*`, not `db.session.*`: ws.py does
    `from db.session import get_session` at module level, which binds the name
    into ws.py's own namespace, so patching the source module has no effect.
    The get_session replacement must be an async context manager, because the
    router uses `async with get_session() as session`.

    Of the two, the repository patch is what makes these tests hermetic — the
    session it is handed is never used. get_session is patched as well so the
    router cannot open a real session at all; without it the call succeeds but
    stays lazy (no query, no connection), which would leave these tests one
    refactor away from silently needing a database.

    Verified hermetic: the whole tests/api suite passes with the container
    stopped and with DATABASE_URL pointed at a dead port.
    """

    @asynccontextmanager
    async def fake_session():
        yield object()  # never used: TimescaleMarketDataRepo is patched too

    monkeypatch.setattr("api.routers.ws.get_session", fake_session)
    monkeypatch.setattr(
        "api.routers.ws.TimescaleMarketDataRepo", lambda _session: repo
    )
    return TestClient(app)
