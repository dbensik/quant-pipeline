# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New backtesting strategies (e.g., RSI, Bollinger Bands).
- API endpoint for retrieving backtest results.

### Changed
- Improved performance of the data normalization step.

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