# CLAUDE.md — Quant Pipeline

End-to-end modular framework for systematic trading: data ingestion, backtesting, ML models, and a Streamlit dashboard.

## Run

```bash
./run_pipeline.sh all          # start dashboard + API + gRPC + verification
./run_pipeline.sh dashboard    # Streamlit dashboard only
./run_pipeline.sh api          # FastAPI only
./run_pipeline.sh grpc         # gRPC service only
python -m cli.run_pipeline     # data pipeline only
```

## Test

```bash
python -m pytest tests/        # needs NO Docker — keep it that way
python -m pytest -m integration  # +9 tests, needs TimescaleDB running
./run_pipeline.sh verify       # verify 3-layer architecture integrity
```

`tests/api/` covers every router without a database, via the repository
Protocol (`db/repositories/market_data.py` documents this as its purpose).
Integration tests are deselected by default and skip if the DB is down.

```bash
cd frontend && npm test        # 73 tests — also needs NO API/Docker
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

- `data_pipeline/` — `EquityPipeline`, `CryptoPipeline`, `FundamentalPipeline`, `DynamicUniverse`, `DataEnricher`; fetches via `yfinance`; stores to SQLite (`quant_pipeline.db`)
- `alpha_models/` — Strategy classes (Moving Average Crossover, Mean Reversion, Trend Following, Pairs Trading, etc.) all inherit from `base_model.py`
- `backtesting/backtester.py` — Simulates strategy on historical data; produces equity curves and KPIs
- `screeners/` — Filter universe by criteria (momentum, low volatility); output feeds into watchlists
- `dashboard_app/` — Streamlit UI; `controllers/` hold business logic; `ui_components/` hold rendering
- `ml_models/` — EDA, model training (scikit-learn), signal generation
- `services/` — 3-layer architecture: gRPC signal service → GraphQL gateway → Ed25519/SHA256 crypto audit log (`audit_log.json`)
- `config/settings.py` — Centralized settings

## Invariants

- New strategies inherit from `alpha_models/base_model.py`.
- Dashboard business logic lives in `controllers/`, never in `ui_components/`.
- Run `./run_pipeline.sh verify` after any change touching `services/`.
- Settings changes go through `config/settings.py` — no hardcoded parameters in pipelines or strategies.
