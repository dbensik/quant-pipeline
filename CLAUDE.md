# CLAUDE.md — Quant Pipeline

End-to-end modular framework for systematic trading: data ingestion, backtesting, ML models, a FastAPI backend and a React dashboard.

Streamlit (`dashboard_app/`) was deleted on 2026-08-09 after every one of its
features was ported to a router and a React page. Do not reintroduce it.

## Run

```bash
./run_pipeline.sh all          # gRPC + GraphQL + FastAPI + React + verification
./run_pipeline.sh rest         # FastAPI only (REST + websockets, port 8001)
./run_pipeline.sh dashboard    # React dev server only (port 5174)
./run_pipeline.sh api          # GraphQL gateway only (port 8002) — NOT FastAPI
./run_pipeline.sh grpc         # gRPC service only
python -m cli.run_pipeline     # ingest bars into TimescaleDB (same path as the API)
```

`api`/`gateway` means the GraphQL gateway, not the REST API — a naming wart
that predates FastAPI. Use `rest` for FastAPI.

## Ports

**This project deliberately avoids the framework defaults.** uvicorn wants
8000, Vite wants 5173, Postgres wants 5432 — and so does every other
Python/React project on this machine, which makes collisions certain rather
than unlucky. On 2026-08-11 `siting-platform`'s uvicorn held 8000, so the
GraphQL gateway never started and `verify` failed with a 404 that read as a
code fault.

| port | service | default it avoids | override |
|---|---|---|---|
| 8002 | GraphQL gateway | 8000 (uvicorn) | `QUANT_GRAPHQL_PORT` |
| 8001 | FastAPI REST + WS | — | `QUANT_REST_PORT` |
| 5174 | Vite dev server | 5173 (Vite) | `QUANT_VITE_PORT` |
| 15432 | TimescaleDB, **host side only** | 5432 (Postgres) | edit `docker-compose.yml` |
| 50051 | gRPC signal service | — | `QUANT_GRPC_PORT` |

`run_pipeline.sh` checks each port is free before starting anything and names
the process holding it. Without that, a bound port surfaced minutes later as a
connection error against a service that had silently never started.

Three places must agree on the Vite port or the browser gets a CORS failure
that reads like an API bug: `frontend/vite.config.ts`, `QUANT_VITE_PORT`, and
the allow-list in `app/core/config.py`.

Only the DB's **host** port moved. Inside the compose network services still
address `timescaledb:5432`, so container-to-container config is unchanged —
but anything connecting from the host needs `localhost:15432`.

Ingestion goes through `core/ingest.py`, reached by both `POST /api/v1/ingest`
and `cli.run_pipeline`. The SQLite `PipelineOrchestrator` was deleted on
2026-08-09 — it wrote a database nothing read while reporting success.

`--full-backfill` OVERWRITES stored bars. It exists because yfinance
re-adjusts a series for splits as of the fetch date; `GET /api/v1/ingest/health`
says which symbols have drifted.

## Scheduled

```bash
crontab -l                                  # 06:00 daily
scripts/cron/daily_maintenance.sh           # ingest, then snapshot indexes
tail -f logs/daily_maintenance.log
```

Ingest runs before the snapshot so a name that joined an index today already
has bars. The job is single-instance (lock file) and aborts with one clear
line if TimescaleDB is unreachable, rather than two stack traces.

**Universe snapshots cannot be backdated.** A missed day is a permanent gap in
point-in-time membership, and membership is what makes survivorship-free
screening possible — so this job matters more than its size suggests.

It needs TimescaleDB up at 06:00, which takes two settings, both applied
2026-08-09:

- Docker Desktop starts at login (`AutoStart: true` in
  `~/Library/Group Containers/group.com.docker/settings-store.json`).
- `docker-compose.yml` uses **`restart: always`**, not `unless-stopped`.
  Measured on macOS 26.6: Docker Desktop's shutdown stops containers in a way
  the daemon records as an explicit stop, so `unless-stopped` left the
  container Exited across a restart. Verified both ways. Consequence:
  `docker compose stop` is undone by a Docker restart — use
  `docker compose down` for maintenance.

## Test

```bash
python -m pytest tests/        # needs NO Docker — keep it that way
python -m pytest -m integration  # +19 tests, needs TimescaleDB running
./run_pipeline.sh verify       # verify 3-layer architecture integrity
```

Integration tests that drive the async engine directly must mark
`loop_scope="session"`. `db/session.py` builds one module-level engine and its
pooled asyncpg connections bind to the first event loop, so pytest-asyncio's
default per-test loop fails in teardown with "Event loop is closed".

`tests/api/` covers every router without a database, via the repository
Protocol (`db/repositories/market_data.py` documents this as its purpose).
Integration tests are deselected by default and skip if the DB is down.

```bash
cd frontend && npm test        # 186 tests — also needs NO API/Docker
```

When adding tests: assert on the *output*, not on values the response merely
echoes back; check the fixture actually exercises the behaviour; and verify the
test FAILS against the bug it covers. All three mistakes have produced tests
here that passed against the very bug they were written to catch.

Know the `tests/api/conftest.py` fixture before asserting on relationships
*between* symbols. AAPL and BTC-USD come from one price formula differing only
in scale, so their **returns are identical** — any test about correlation,
diversification or portfolio weights is vacuous over that pair. Use MSFT, which
follows an independent seeded random walk, as the decorrelated counterpart.

Determinism tests need the same scrutiny. Slippage is seeded, and asserting
"the same winner" is often weaker than it looks — a dominant candidate wins
whether or not the seed is threaded. Assert on the metrics, and confirm the
unseeded version actually differs before committing the test.

Frontend specifics: charts render nothing under jsdom (Recharts measures a 0x0
parent; Plotly needs canvas APIs jsdom lacks), so never assert on chart output —
mock the chart child and assert on its props. Chart computation lives in pure
modules beside the components (`chartRows.ts`, `candlestickData.ts`) for the
same reason.

Plotly is loaded lazily and must stay that way — it is ~1.2 MB, larger than the
rest of the bundle. `npm run build` must show a separate CandlestickChart chunk
with no "plotly" in the main chunk.

## Environment

Poetry is authoritative (decided 2026-07-31; `package-mode = false` is set): `poetry install`.
`run_pipeline.sh` activates the Poetry venv (Poetry-only; the conda fallback and `environment.yml` were removed 2026-07-31 — do not recreate them or add conda-based setup instructions).
For the TimescaleDB layer, copy `.env.example` → `.env`; without it, `db/session.py` defaults to the local docker-compose database.

## Architecture

- `data_pipeline/` — `EquityPipeline`, `CryptoPipeline`, `FundamentalPipeline`, `DynamicUniverse`, `DataEnricher`; legacy fetchers still used for universe listings
- `core/ingest.py` — THE write path: fetch via adapters, persist via the repository into TimescaleDB
- `core/corporate_actions.py` — split-adjustment drift and delisting detection
- `alpha_models/` — Strategy classes (Moving Average Crossover, Mean Reversion, Trend Following, Pairs Trading, etc.) all inherit from `base_model.py`
- `backtesting/backtester.py` — Simulates strategy on historical data; produces equity curves and KPIs
- `screeners/` — Filter universe by criteria (momentum, low volatility); output feeds into watchlists
- `api/routers/` — the REST/WS surface: ohlcv, assets, strategies, backtest,
  compare, optimize, screeners, statistics, portfolios, watchlists, research,
  ingest, results, signals, ws
- `frontend/` — React dashboard; `routes.tsx` declares pages once for both the
  router and the nav bar; TanStack Query owns server state, Zustand owns UI
  selections only
- `ml_models/` — EDA, model training (scikit-learn), signal generation
- `services/` — 3-layer architecture: gRPC signal service → GraphQL gateway → Ed25519/SHA256 crypto audit log (`audit_log.json`)
- `core/portfolio.py` — portfolio accounting; the trade log is the only stored
  state and cash/positions/P&L are derived from it (`db/models.py` has no
  `cash` or `positions` column, deliberately)
- `config/settings.py` — Centralized settings

## Invariants

- New strategies inherit from `alpha_models/base_model.py`.
- Pages read server state through TanStack Query hooks in `api/queries.ts`; Zustand holds UI selections only, never fetched data.
- A mutation must invalidate every query its write affects — for portfolios that includes the DERIVED state, not just the trade list.
- Two Pydantic models must never share a class name across routers: FastAPI qualifies the collision by module, silently renaming the generated TypeScript type. `tests/api/test_openapi_contract.py` enforces this.
- Run `./run_pipeline.sh verify` after any change touching `services/`.
- Settings changes go through `config/settings.py` — no hardcoded parameters in pipelines or strategies.
