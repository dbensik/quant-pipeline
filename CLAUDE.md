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
launchctl print gui/$(id -u)/com.dbensik.quant-pipeline.daily-maintenance
scripts/launchd/install.sh                  # (re)install after editing the plist
scripts/cron/daily_maintenance.sh           # ingest, reassigned + missing-day checks, snapshot
scripts/check_reassigned.py                 # the check alone; read-only, exit 1 = flagged
scripts/check_missing_days.py               # holes inside series; read-only, exit 1 = lost bars
tail -f logs/daily_maintenance.log
```

**launchd, not cron, since 2026-08-25.** cron only fires if the machine is
awake at the scheduled minute and never catches up. Measured: the job did not
run at all from 2026-08-18 to 2026-08-24 — seven consecutive days, no log lines
at all, because the Mac was asleep at 06:00. launchd's `StartCalendarInterval`
runs a missed job on next wake. `install.sh` removes the crontab entry, because
two schedules would race for the lock file and the loser logs "SKIPPED: a run is
already in progress", which reads like a bug.

Ingest runs before the snapshot so a name that joined an index today already
has bars. The job is single-instance (lock file) and aborts with one clear
line if TimescaleDB is unreachable, rather than two stack traces.

**A partial snapshot exits non-zero and posts a notification.** Until
2026-08-25 `scripts/snapshot_universes.py` returned 0 whenever *any* index
succeeded, on the reasoning that one moved page should not page anyone. That is
wrong for un-backdatable data: Wikipedia dropped the constituents table from the
DJIA page on 2026-08-16, `dow_jones` returned empty every morning, the job
logged "Snapshotted 2 of 3" and exited 0, and ten index-days were lost before
anyone looked. Any failure is now non-zero, and the wrapper raises a macOS
notification when it is not attached to a terminal.

**`dow_jones` does not come from Wikipedia** (fixed 2026-08-25). That page
carried a components table until roughly 2026-08-16 and then became a navbox
with no table at all — the source moved, so no amount of parsing would have
helped. Constituents now come from `URL_DOWJONES_CONSTITUENTS` in
`config/settings.py`.

**Every scrape is checked against a plausible count** (`_implausible()` in
`data_pipeline/dynamic_universe.py`) — `DOWJONES_EXPECTED_COUNT = 30` exactly,
`SP500_EXPECTED_RANGE = (480, 520)`. The Dow is thirty stocks by definition; the
S&P needs a range because it targets 500 *companies* but lists more
*securities* (GOOG/GOOGL, FOX/FOXA, NWS/NWSA), so 503 today.

A SHORT read is the case worth guarding. An empty one is already safe — the
caller refuses to record it, since claiming an index has no members is worse
than recording nothing. But 28 Dow tickers or 250 S&P names looks exactly like a
healthy snapshot, gets written, and quietly corrupts point-in-time membership.
Discarding costs one visible index-day; recording a wrong list corrupts history
that cannot be backdated. DIA ETF holdings pages were rejected as a Dow source
for exactly this reason: they paginate at 25 rows.

Bounds are loose on purpose — they catch a changed source shape, not index
turnover. A check that fires on legitimate reconstitution gets ignored, then
deleted.

**After ingest, `scripts/check_reassigned.py` flags any equity or ETF whose
dollar volume collapses 20x or more** (added 2026-09-24). This is the guard for
PARA's failure: a delisted ticker's key re-issued to another company, whose bars
the daily job then appended. A flag exits 1 and posts its own notification; it
writes nothing. The signal is dollar volume, not a hole in the dates. A split
leaves dollar volume unchanged, and a reassignment can arrive with no hole.
Clear a reviewed break by adding its date to `metadata.reassignment_cleared` (a
list) — details in the script's docstring. Crypto is excluded: it has its own
guard, and stETH/DAI-style volume shifts would fire daily.

**Ingest re-requests the last 14 days (`INGEST_OVERLAP_DAYS`), inserting only
missing bars** (since 2026-09-25). Before that it resumed the day after the
newest bar, so a day lost while a later one landed was never requested again:
463 of 527 equities silently lost 2026-08-28 for a month. Existing bars are
never rewritten by the overlap. `scripts/check_missing_days.py` then reports any
hole older than the overlap; fill one with
`python -m cli.run_pipeline --symbols X --start YYYY-MM-DD` (insert-only,
refused alongside `--full-backfill`), or accept a day Yahoo no longer serves via
`metadata.missing_days_accepted`.

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
- `core/corporate_actions.py` — split-adjustment drift, delisting detection,
  and rename/successor detection (`scripts/find_successors.py` proposes, never
  writes). A rename is ONE instrument re-keyed and shows a constant old/new
  ratio; a merger or acquisition genuinely ends the target and correctly finds
  nothing. Threshold is `1e-4`, not `1e-2` — merger arbitrage pins a target to
  its acquirer at ~4.6e-03 before a deal closes, which a loose threshold calls
  a rename. See `research/corporate-actions-findings-2026-09-15.md`.
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
