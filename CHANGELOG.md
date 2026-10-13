# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Entries from 2026-08-07 on. The older entries further down this section
describe the Streamlit dashboard and `docker-compose` services as they were
before the migration below; they are kept as history.

### Changed
- **The Research page's financial statements come from SEC filings** (2026-10-12): ten fiscal years or eight quarters of the same point-in-time figures the Valuation page uses, each with its filing date and XBRL concept on hover, instead of yfinance's four restated years with no filing dates. Quarterly cash flows are single quarters derived from the year-to-date figures 10-Qs file, shown in italics. Banks get their statements even though the DCF refuses them. yfinance is kept, and says why, only where the filings have no recent standard figures: ETFs, crypto, AZN, CCEP and GFS, and ASML's quarters.
- **The portfolio optimizer scores weights as a backtest would** (2026-10-12): `/optimize/portfolio` (REST and websocket) now reports each weighting's compound annual growth, volatility and cash-account Sharpe, as a daily-rebalanced, cost-free backtest of it would, through the same Sharpe function every backtest uses. It used mean daily return × 252 minus the window's mean rate, so the Optimize page and a backtest showed different Sharpes for the same basket. SPY/AGG/GLD 2023–2025: max Sharpe 1.82 → 1.94 at the same 42/0/58 weights, return 26.3% → 29.0%. The seeded weights are unchanged; scoring 5,000 takes 14–26 ms instead of ~140 ms.
- **Idle backtest cash earns the risk-free rate, and Sharpe is measured against it** (2026-10-12): every backtester (single-asset, portfolio, grid search, simulate's synthetic paths) credits uninvested cash with the `^IRX` bond-equivalent rate bar by bar, using the rate known at the start of each period over its calendar days. Sharpe is now the geometric excess of equity over that cash account divided by the volatility of the excess return, so a strategy that never trades scores 0 instead of being charged interest it was never paid. SPY 2023–2025: `ma_crossover` returns 5.77% a year instead of 1.25% and its Sharpe is 0.77 (0.95 against zero, −2.71 with the rate subtracted alone); buy and hold 1.13. With zero interest, 15 real runs are bit-identical to before.
- **Every strategy Sharpe is in excess of the stored risk-free rate** (2026-10-12): backtest, compare (strategies and benchmark on one rate), strategy optimize, portfolio backtest and simulate, REST and websocket, all subtract the mean `^IRX` bond-equivalent rate over the window; it used to be 0. Each response carries `risk_free_rate` and `risk_free_source`, and the Backtest, Compare and Optimize pages show them. Sortino and Calmar are unchanged. Backtester runs are unaffected: the rate enters the metrics only. A mostly-in-cash strategy moves most: SPY `ma_crossover` 2023–2025 went from 0.95 to −2.71, since idle cash earns nothing in the backtester.
- **Portfolio optimizer uses the stored risk-free rate** (2026-10-12): the Sharpe ratio in `/optimize/portfolio` (REST and websocket) subtracts the mean `^IRX` bond-equivalent rate over the window's sessions instead of a constant 0.02. The response carries `risk_free_source`, and the Optimize page shows it. A window the stored series does not cover is 422; `risk_free_rate` in the request still overrides. Over 2023–2025 the constant was 2.8 points low: SPY/AGG/GLD max Sharpe 2.05 → 1.82.
- **Backtester runs 50x faster** (2026-10-07): per-bar state is recorded in numpy arrays instead of three pandas cell writes; 291 ms → 5 ms a run, bit-identical on 56 real runs. Grid search and the Monte Carlo re-run mode inherit it.
- **TimescaleDB is the store** (2026-08-07 to 08-09). Bars moved from SQLite to a TimescaleDB hypertable; the SQLite pipeline and its orchestrator were retired, and the CLI and `POST /api/v1/ingest` share one write path, `core/ingest.py`.
- **React replaces Streamlit** (2026-08-09). Every dashboard feature was ported to a FastAPI router and a React page, then `dashboard_app/` and the Streamlit dependency were deleted.
- **Prices are adjusted at read time** (2026-09-28). Ingest stores prices as Yahoo served them, with the fetch time and corporate actions; `fetch_range` derives adjusted prices on read. Stored bars are never rewritten and `--full-backfill` is removed (the API answers 422 to it).
- **launchd replaces cron** for the daily job (2026-08-25), so a run missed while the Mac slept happens on wake.
- **Ports moved off the framework defaults** (2026-08-11): 8001 REST, 8002 GraphQL, 5174 Vite, 15432 TimescaleDB on the host.

### Added
- **A daily check that the DCF still reads the right line from each filing** (2026-10-13): `scripts/check_line_concepts.py`, in the 06:00 job after the snapshot, re-values every valued company with each other concept that reports its newest fiscal year differently (revenue, capex, operating cash flow, interest) and flags any that moves value/price by 0.05 and 5%, plus any per-filer map naming concepts the filer stopped using. Flags only, about 45 seconds. Clean today; with CHTR's revenue map removed it flags CHTR 3.63 -> 5.81. Eight readings that found the chosen line right are recorded in `LINE_CONCEPTS_REVIEWED` with what the statement shows.
- **Daily backups, proven by a restore** (2026-10-12): the database (a 90 MB dump from the container's own `pg_dump`) and the option-chain archive go to a Google Drive folder set in `.env` as the 06:00 job's last step, 14 daily and 12 monthly kept. Each dump is restored into a scratch database and every table's row count checked before it is copied; a failure fails the job and notifies. Nothing was backed up before, and most of what the project stores — option chains, universe snapshots, prices as served, legacy histories — cannot be re-fetched.
- **22 fee businesses in Financials and Real Estate get a DCF** (2026-10-12): exchanges and data providers (SPGI, MCO, MSCI, FDS, ICE, CME, NDAQ, CBOE), insurance brokers (AJG, AON, BRO, MRSH, WTW), payment processors (MA, GPN, CPAY, JKHY, PYPL, FIS, FISV) and real-estate services (CBRE, CSGP). Their sectors stay refused for banks, insurers and REITs; these are admitted by name, each measured first (operating cash flow a stable 0.17–0.59 of revenue), and the page says so. 359 → 381 valued.
- **Valuation page and API** (2026-10-09..11): a DCF for every S&P 500 filer from SEC XBRL company facts stored raw and insert-only (`fundamental_facts`, 1.6M+ facts) and standardised at read time, strictly as filed before the valuation date. Each input shows its source; the page has the bridge, a sensitivity grid, the price-implied growth and value vs price at every filing. Beta is weekly 5-year Blume-adjusted; WACC is floored at 7% and labelled where it binds; a company paying interest with no debt balance on file is refused rather than valued debt-free. The 06:00 job reloads only companies that filed a 10-K or 10-Q that week, from EDGAR's daily form index. `/api/v1/valuation`, `modeling/`, `research/dcf-plan-2026-10-08.md`.
- **Options page and API** (2026-10-08..09): implied-volatility smiles, the ATM term structure over the realised-vol cone, and a side-by-side pricer (Black–Scholes, CRR tree, Monte Carlo) for the archived option chains of SPY, QQQ, IWM, TQQQ, AAPL and NVDA. IV is computed from mids, not taken from Yahoo; each expiry is priced off its own parity forward; American exercise and projected discrete dividends are taken out before inversion. Surfaces are cached on disk with a key that changes when any input or the pricing code does. `/api/v1/options`, `pricing/`, `research/option-pricing-plan-2026-10-08.md`.
- **Risk-free rate series** (2026-10-08): `^IRX` (13-week T-bill, bank-discount basis) ingested daily into its own insert-only table, `rate_observations`, from 2015. `core/rates.py` converts it on read to continuous and bond-equivalent rates for a date, using the observation on or before it and flagging a stale one. Tier 2 phase 0 of `research/option-pricing-plan-2026-10-08.md`.
- **TQQQ** registered as a served ETF (2026-10-08), so the option-chain capture has spot history, splits and dividends behind it.
- **Monte Carlo over a backtest** (2026-10-07): `POST /api/v1/simulate` and `ws /simulate`, with a Simulate panel under the backtest result. Fan bands, terminal-wealth, drawdown and VaR/CVaR distributions from a stationary block bootstrap (iid and GBM beside it), either over the strategy's realised returns or by re-running the strategy on resampled price paths. Served assets only unless overridden. Plan and measurements in `research/monte-carlo-plan-2026-10-07.md`.
- **Point-in-time index membership** (2026-08-09): a daily snapshot of the S&P 500, Dow and top-100 crypto lists. A partial snapshot is a failed run.
- **Reconstructed S&P 500 membership** (2026-10-04): month-end lists for 2014-12 to 2026-07 from Wikipedia revisions, in `universe_membership_reconstructed`. Nothing reads it yet.
- **ETF asset class and three allocation strategies** (2026-08-10): 11 ETFs; paired switching, asset-class trend and momentum allocation; each strategy declares its `signal_shape`.
- **Daily data checks** after ingest: reassigned tickers (2026-09-24), missing days (2026-09-25), returns against a fresh Yahoo fetch (2026-09-28).
- **Provider tickers for crypto** (2026-10-04): 21 assets whose bare Yahoo ticker is a different token are fetched under the ticker of the right coin and verified before any bar is stored; 21,766 bars loaded.
- **PSKY** registered as the continuation of PARA (2026-10-04), as its own asset; PARA's row and bars are unchanged.
- **Crypto identity and bounds checks** (2026-09-19 to 09-22): each crypto series is verified against its CoinGecko id and the range the coin has traded in.
- **Rename and successor detection** for equities (2026-09-19), proposing only.
- **Daily option-chain capture** to dated Parquet (2026-09-14) and a data-freshness badge on every page (2026-09-14).
- **Test suites that need no Docker**: API routers through the repository Protocol, and a frontend suite.

### Fixed
- **DCF capex includes capitalised software and other intangible capital spending** (2026-10-13): capex was property, plant and equipment only, so companies that capitalise software had their free cash flow overstated. Software development and purchases are now added wherever filed (17 valued companies, each checked to report it outside capex: FIS 2.34 -> 1.33, JKHY 1.60 -> 1.16, AKAM 0.58 -> 0.04), and intangible purchases by name where the statement shows recurring spending (CMCSA 5.50 -> 4.73, ADP, SLB). Spectrum, acquisitions and emission allowances are left out on purpose. The capex source names the addition. 20 values changed; still 381 valued.
- **DCF growth, revenue and operating cash flow read the lines the filings report** (2026-10-13): the other DCF inputs were checked like capex. Seven companies' growth was measured on a revenue figure that is not the income statement's total (CHTR's $0.89B, not $54.8B: growth -6.9% for flat revenue, value 3.63 -> 5.81x; ADM, BG, KMI, SRE, DVN, WMB likewise) and now read the total by name. A growth rate's start year is found by date and on the latest year's basis: ADM's 5-year rate had compared a 2025 part with a 2020 total, EQT's gross gas sales with total revenues, EMR an off-cycle $1.66B period with its fiscal year. Operating cash flow is the continuing-operations figure where one is filed, matching capex: CARR's free cash flow had included $424M from businesses being sold (0.58 -> 0.40), HON's $333M, OXY's $926M. 26 values changed; still 381 valued. Interest, D&A, cash and debt alternatives changed nothing material or were already right.
- **AEP, LNT and EOG are valued on their real capex** (2026-10-12): 27 companies tag capex under more than one concept; the 11 where it matters were read against their 10-K cash-flow statements. AEP had been valued at 0.86x its price on $3.45B of power-plant acquisitions instead of $8.45B of construction spending; it is now -0.82, like the other utilities. LNT and EOG report capex in two lines that are now summed. The other eight were already right — including Delta, whose capex the earlier note wrongly called understated.
- **DCF valuations no longer depend on what ran before them, and six debt refusals are valued** (2026-10-12): a valuation read a 10-Q's quarter and year-to-date figures in whatever order the database returned them and kept one, so DAL valued at 1.28 alone and 2.28 inside a sweep; and the trailing year mixed two capex lines DAL files side by side ($1.534B a year instead of $4.475B). Both fixed; sweeps in any order now agree, 17 values moved (DAL 2.28 → 1.28, the rest by 0.22 or less). Convertible, senior and unsecured note concepts were added, so AKAM, CDNS, DDOG, DXCM, VRSN and ZS are valued (353 → 359) and DASH carries its $2.73B of convertibles instead of none. The remaining refusals are documented as permanent with SEC company facts (extension-only capex, per-class share counts); a proposal to admit fee businesses in Financials and Real Estate is in `research/dcf-plan-2026-10-08.md`, decision 16.
- **Crypto backtests trade fractions of a coin** (2026-10-12): every backtest traded whole units, so with less cash than one coin a BTC backtest bought nothing and showed a flat line, and a portfolio's 2% per-trade cap rounded its BTC order to 0. Crypto now trades in lots of 1e-8 and equities and ETFs stay in whole shares, in every backtest endpoint (backtest, compare, optimize, portfolio backtest, simulate, and their websockets). Equity and ETF results are unchanged bit for bit. BTC-USD 2016–2025: 14 buy signals could not fill across the default strategies (13 of them `mean_reversion`; now 0), and buy and hold ends at $1.21M instead of $1.14M, since whole coins had left the remainder in cash.
- **Backtests charge `transaction_cost`, and slippage is always adverse** (2026-10-12): the per-fill cost every endpoint accepted (default 10 bps) was read by nothing, and slippage was symmetric, so it averaged to zero — a backtest paid $1 a trade and no more. Both are now charged; portfolio backtests gained the `transaction_cost` input. Buy and hold barely moves; SPY `mean_reversion` 2016–2025 ends at $94.9k instead of $117.5k, and the SPY `ma_crossover` grid winner moves from 10/50 (58 trades) to 10/200 (16).
- **Portfolio backtests never overdraw cash, and a rebalance sells before it buys** (2026-10-12): `PortfolioBacktester` had no cash check, so slippage and commissions left real AAPL+TQQQ baskets up to $70 below zero for 60 bars, and orders ran in ticker order, so a purchase could precede the sale funding it. Each bar now executes sales first, and a purchase the cash cannot cover is cut to what it covers at its fill. Six real baskets (2016–2025) now stay at or above zero and end within 0.05% of their old values.
- **A buy the cash could not cover is resized, not dropped** (2026-10-12): an all-cash entry whose slippage and commission pushed the fill above the cash was skipped and retried next bar — 80 unfilled buy bars over 15 real runs (TQQQ `mean_reversion` alone 42; buy and hold on AAPL and TQQQ started a day late). It now buys what the cash covers at the same fill, so runs that never hit it are unchanged.
- **Backtest metrics used the requested starting capital** (2026-10-12): every caller passed `PerformanceAnalyzer(history, initial_capital)`, which bound the capital to `benchmark_history`, so metrics were measured against 100,000 whatever the run started with — a 50,000 run that gained 1.6% reported −49.2%. Only API callers setting `initial_capital` were affected; the UI always sends the default. The analyzer's options are now keyword-only.
- **Split-adjustment drift** silently corrupted prices after a split (2026-08-09); superseded by read-time adjustment.
- **An empty fetch no longer marks a symbol delisted** (2026-08-10): it means the provider lost the key.
- **PARA** ingested a reassigned ticker's bars; they were removed and 1256 real bars restored (2026-09-24).
- **Ingest re-requests the last 14 days**, inserting only missing bars, after 463 equities silently lost 2026-08-28 (2026-09-25).
- **Wrong-token crypto histories** removed or trimmed for the affected assets (2026-09-19 to 09-24).
- **Basket and index rebalancing** skipped a third of their rebalances (2026-08-11).
- **ADF test** reported every series as stationary (2026-08-09).
- **Dow Jones constituents** moved off Wikipedia, and implausible constituent counts are rejected (2026-08-25).

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
- **The SQLite-era pipelines** (2026-10-12): `data_pipeline/` `EquityPipeline`, `CryptoPipeline`, `pipeline.py`, `new.py` and `DataEnricher` — 719 lines that only imported each other, and the last hardcoded 0.02 risk-free rate. Ingest has gone through `core/ingest.py` since 2026-08-09.
- `ml_models/option_pricing.py` (2026-10-08), a placeholder barrier-option payoff nothing imported; replaced by the tier 2 pricing package as it is built.
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