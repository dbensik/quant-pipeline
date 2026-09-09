# Quant Pipeline

An end-to-end framework for systematic trading research: ingest market data into TimescaleDB, run strategies through one backtesting contract, screen and analyze a universe, and drive all of it from a FastAPI backend and a React dashboard. Signals can be served over gRPC and GraphQL with Ed25519 signatures and an append-only audit log.

Built and maintained by one person as a working research tool, not a product. It runs every day on a schedule; the parts that don't work yet are listed under [Status](#status) rather than hidden.

## What it does

| Area | What's there |
|---|---|
| **Data** | Daily bars for the S&P 500, Dow, Nasdaq 100, and top-100 crypto universes via yfinance, written to TimescaleDB through one ingestion path (`core/ingest.py`) shared by the API and the CLI. Incremental updates by default; `--full-backfill` rewrites a series when yfinance re-adjusts it for splits, and `GET /api/v1/ingest/health` reports which symbols have drifted. |
| **Strategies** | 18 modules in `alpha_models/` behind a single `BaseAlphaModel` contract and a registry: moving-average crossover, mean reversion, cointegrated mean reversion, pairs trading, paired switching, trend following, ATR breakout, RSI, momentum allocation, basket trading, index rebalancing, asset-class trend, buy-and-hold, a random-forest model, and others. |
| **Backtesting** | Equity curve against buy-and-hold, CAGR, Sharpe, max drawdown, Calmar, trade log; parameter grid search and portfolio-weight optimization; strategy comparison on one symbol. Live progress over a websocket. |
| **Screening and statistics** | Momentum and low-volatility screeners over a universe; ADF, cointegration, and PCA on the Statistics page. |
| **Portfolios and watchlists** | Saved in the database, with a trade log and derived P&L. |
| **Research** | Company profiles, financials, and news per symbol. |
| **Serving** | gRPC `SignalService`, a GraphQL gateway in front of it, SHA-256 payload hashing and Ed25519 signing, and a hash-chained `audit_log.json`. `./run_pipeline.sh verify` checks the chain. |
| **Testing** | `tests/test_strategy_contract.py` runs every registered strategy over synthetic trend, mean-reverting, flat, and gap fixtures and asserts output shape, valid signal values, and no look-ahead: signals at *t* must not change when future bars are removed. 116 checks, and new strategies are covered automatically. |
| **Operations** | A launchd job (`scripts/launchd/`) ingests and snapshots index constituents daily, is single-instance, catches up after the Mac was asleep, and posts a notification on a partial run instead of a stack trace. |

## Quick start

Requires Python 3.11, [Poetry](https://python-poetry.org/), Node for the dashboard, and Docker for TimescaleDB.

```bash
git clone git@github.com:dbensik/quant-pipeline.git
cd quant-pipeline
poetry install
cp .env.example .env          # TimescaleDB URLs; the defaults match docker-compose
docker compose up -d timescaledb
./run_pipeline.sh all         # gRPC + GraphQL + FastAPI + React + verification
```

Then open the dashboard at <http://localhost:5174> and the REST docs at <http://127.0.0.1:8001/api/v1/docs>. On the **Data** page, run an ingest (or `POST /api/v1/ingest`) to load bars.

Individual services:

```bash
./run_pipeline.sh rest         # FastAPI only (REST + websockets, port 8001)
./run_pipeline.sh dashboard    # React dev server only (port 5174)
./run_pipeline.sh api          # GraphQL gateway only (port 8002) — not FastAPI
./run_pipeline.sh grpc         # gRPC signal service (port 50051)
./run_pipeline.sh verify       # audit-chain and integration checks
python -m cli.run_pipeline     # ingest from the command line (same path as the API)
```

`run_pipeline.sh` activates the Poetry environment itself, checks that every port is free before starting anything, and names the process holding a port that isn't. The ports deliberately avoid the framework defaults (8000, 5173, 5432) because every other project on a developer's machine uses those; see the table in `CLAUDE.md` for the overrides.

## A typical session

1. **Ingest** on the Data page. Incremental by default; the health endpoint tells you when a full backfill is warranted.
2. **Chart and backtest** a symbol: pick a strategy, set parameters and a date range, run, and read the equity curve, KPIs, and trade log.
3. **Compare** several strategies on the same symbol, or **Optimize** a parameter grid or portfolio weights.
4. **Screen** the universe (momentum, low volatility) and save the result as a watchlist to backtest against.
5. **Save** the run and reload it later without re-simulating.

## Layout

```
alpha_models/      strategies (one class each), base_model.py, registry.py
backtesting/       backtester and parameter generator
core/              ingestion path shared by the API and CLI
data_pipeline/     universe fetchers, equity/crypto/fundamental pipelines, normalizer
screeners/         momentum and low-volatility screeners over a universe
ml_models/         EDA, training, signal generation
api/               FastAPI backend: one router per feature; upstream.py is the only module that touches the network
frontend/          React dashboard (Vite, TanStack Query, Tailwind/shadcn); api/schema.d.ts is generated from OpenAPI
services/          gRPC signal service, GraphQL gateway, protobufs, signing and audit log
cli/               run_pipeline.py
scripts/launchd/   daily maintenance job and installer
tests/             strategy contract harness, unit tests, API tests
alembic.ini, db/   TimescaleDB schema and migrations
run_pipeline.sh    orchestration: activates Poetry, checks ports, starts services
```

## Status

Honest accounting, kept current in `CHANGELOG.md`:

- `ml_random_forest` trains on full history and then predicts historically, so its backtests are invalid until it is rewritten walk-forward. The contract harness pins this as an expected failure so it cannot be mistaken for a working model.
- `PairsTradingStrategy` returns per-leg position columns instead of a `signal` column. It works with the portfolio backtester, is pinned by a test, and should be unified with the contract.
- `api_layer/` is legacy. Its `DataSerializer` is still imported; the REST API it once held was replaced by `api/` (FastAPI) and the GraphQL gateway.
- The Streamlit dashboard was removed in August 2026 after every feature was ported to a React page. Do not look for it.
- The SQLite database (`quant_pipeline.db`) is the legacy store from the 0.1.0 release. Nothing reads it any more; TimescaleDB is the source of truth.

## Contributing

Fork and open a pull request. New strategies should subclass `BaseAlphaModel` and register themselves; the contract harness will pick them up and tell you if they peek at the future.

## License

[MIT](https://opensource.org/license/mit)
