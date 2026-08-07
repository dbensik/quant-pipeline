# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **docker-compose backend command (P0):** `uvicorn main:app` → `uvicorn api.main:app` (no top-level `main.py` exists; broke containerized deploy).
- **docker-compose dashboard command:** `dashboard_app/main.py` → `dashboard_app/dashboard.py` (correct Streamlit entry point).
- **`db/models.py`:** replaced deprecated `lazy="dynamic"` on `AssetORM.market_data` with `lazy="selectin"` (removed in SQLAlchemy 2.1; incompatible with `AsyncSession`).
- **`db/session.py`:** `DATABASE_URL`/`SYNC_DATABASE_URL` now default to the local docker-compose TimescaleDB, so importing without a `.env` no longer raises `ValidationError`.
- **`.gitignore`:** added `!.env.example` negation (the `.env.*` pattern was ignoring the template).

### Added
- **`.env.example`:** documents both database URLs (async app / sync Alembic) and the in-network compose variant.
- **Strategy contract test harness (`tests/test_strategy_contract.py`):** every `BaseAlphaModel` strategy is run over synthetic fixtures (trend / mean-reverting / flat / gap) asserting output shape, valid signal values, and — critically — **no look-ahead** (signals at t must not change when future bars are removed). 116 checks; auto-covers future strategies added to the registry.

### Known Issues
- **`ml_random_forest` has look-ahead bias** (caught by the new harness; the code comment admits it): it trains on the full history then predicts historically, so its backtests are invalid until rewritten walk-forward. Pinned as `xfail(strict=True)` in the harness.
- **`PairsTradingStrategy` output contract diverges:** returns per-leg position columns rather than a `signal` column; consumed by the portfolio backtester. Pinned by test; worth unifying.

### Changed
- **`run_pipeline.sh`:** environment activation migrated to Poetry-only — activates the venv from `poetry env info --path`, exits with instructions if missing. (Briefly shipped with a conda fallback; removed same day after the Poetry flow was verified on the primary machine.)
- **README:** installation instructions rewritten for Poetry.

### Removed
- **`environment.yml`:** conda environment spec retired per the Poetry decision; a pre-existing `quant-pipeline-env` still works via the launcher's fallback, but conda setup is no longer documented.

### Security
- **Loopback-only binding for local services.** The Streamlit dashboard (no auth, paper trading + DB writes) bound `0.0.0.0` by default, exposing it to the LAN; added `.streamlit/config.toml` with `server.address = "127.0.0.1"`. The gRPC signal service default bind changed from `[::]` to `127.0.0.1` in `services/config.py` (override with `QUANT_GRPC_BIND_ADDRESS=0.0.0.0` for containerized deployment). The GraphQL gateway was already loopback-bound. Note: Streamlit's "External URL" startup line was only a detected public IP, not actual internet exposure — the real issue was LAN reachability.

### Decided
- **API architecture:** FastAPI (`api/main.py`) is the web-facing API for the planned React dashboard; the gRPC → GraphQL → Ed25519/SHA256 audit-log stack remains the signal-serving layer. Supersedes the "delete `api/main.py`" action item.
- **Environment manager:** Poetry is authoritative (`package-mode = false`); conda flow is legacy.

---

## [0.2.0] - 2026-01-20

### Added
- **API Layer**: Implemented a FastAPI application to expose system status and backtest results.
- **Index Rebalancing Strategy**: Added a new strategy supporting monthly, weekly, and quarterly rebalancing.
- **Run Script**: Updated `run_pipeline.sh` to support starting the API and fixing execution path issues.

### Changed
- **Dashboard Refactor**: Extracted business logic from `dashboard.py` into dedicated controllers (`AnalysisController`, `OptimizationController`, `StatisticsController`).
- **Performance Optimization**: Vectorized `time_series_normalizer.py` for significant speed improvements.

---

## [0.1.0] - 2025-07-06

This is the initial public release of the Quant Pipeline project.

### Added
- **Data Pipeline:** Core functionality to fetch daily price data for stocks and cryptocurrencies using `yfinance` and store it in a SQLite database.
- **Constituents Fetcher:** Script to dynamically fetch and cache the constituents of major indices (S&P 500, Dow Jones, Nasdaq 100) and the top 100 cryptocurrencies.
- **Streamlit Dashboard:** Interactive user interface for visualizing price data, running backtests, and managing watchlists.
- **Backtesting Engine:** Initial implementation of a moving average crossover strategy backtester with performance metrics (Sharpe Ratio, Max Drawdown, etc.).
- **Watchlist Management:** Functionality within the dashboard to create, save, and load custom asset watchlists.
- **Database Storage:** Centralized data persistence using a SQLite database (`quant_pipeline.db`).
- **CLI Entry Point:** A command-line interface (`run-quant-pipeline`) to execute the data pipeline.
- **Project Structure:** Established a modern Python project structure with `pyproject.toml`, a clean `environment.yml`, and a dedicated `tests` package.

### Changed
- **Refactored Watchlists:** Migrated watchlist storage from a `watchlists.json` file to dedicated tables in the SQLite database for improved data integrity and scalability.
- **Centralized Configuration:** All file paths, URLs, and key settings are now managed in `config/settings.py` for easier maintenance.

### Removed
- **Redundant Scripts:** Deleted legacy scripts (`init_db.py`, `crypto_meta.py`) whose functionality was absorbed into the main pipeline.
- **Legacy Directories:** Removed the confusing `backtest_results/` directory in favor of the managed `results/` directory.