# Quant Pipeline
Quant Pipeline is an end-to-end modular framework for developing, backtesting, and deploying systematic trading strategies using both traditional and machine learning approaches.

## Overview

**Quant Pipeline** 
The project is organized into several key components:

- **Alpha Models (alpha_models/):**  
  Contains traditional strategy modules. This folder includes:
  - `index_rebalancing.py`: Placeholder for an Index Rebalancing Strategy.
  - `pairs_trading.py`: Placeholder for a Pairs Trading Strategy.
  - `basket_trading.py`: Placeholder for a Basket Trading Strategy.

- **API Layer (api_layer/):**  
  *Legacy/Deprecated*. Contains serialization helpers (`DataSerializer`). The REST API has been superseded by the `services/graphql_gateway`.

- **Backtesting (backtesting/):**  
  Contains the backtesting framework (e.g., `backtester.py`) to simulate historical performance of the strategies.

- **CLI (cli/):**  
  Contains the CLI runner (`run_pipeline.py`) to execute the complete pipeline from the command line.

- **Dashboard App (dashboard_app/):**  
  Contains the Streamlit dashboard code.
  - `controllers/`: Contains logic for Analysis, Optimization, and Statistics.
  - `ui_components/`: Contains UI rendering modules.
  - `dashboard.py`: Main entry point.

- **Data Pipeline (data_pipeline/):**  
  Contains the core functionality for fetching and cleaning market data.
  - `__init__.py`: Package initializer.
  - `data_pipeline.py`: Defines the `DataPipeline` class for data fetching, cleaning, and saving.

- **Data (data/):**  
  An optional folder for storing additional data files if needed.

- **Machine Learning Models (ml_models/):**  
  Contains machine learning components for predictive modeling:
  - `eda.py`: Updated module to load data from CSV or the database and perform EDA.
  - `model_training.py`: Contains routines for training and evaluating ML models.
  - `signal_generation.py`: Converts model outputs into actionable trading signals.

- **Notebooks (notebooks/):**  
  Contains Jupyter notebooks for interactive work and research:
  - `01_data_collection_and_cleaning.ipynb`: Notebook for data fetching and cleaning.
  - `02_alpha_research.ipynb`: Notebook for alpha research and signal generation.

- **Project Setup Files:**
  - `environment.yml`: Conda environment specification.
  - `pyproject.toml`: PEP 517 configuration file for modern build systems.
  - `setup.py`: Setup script for packaging the project.
  - `.gitignore`: Specifies files and directories to exclude from version control.

- **Database:**
  - `quant_pipeline.db`: The SQLite database file (created at runtime).

- **Services (services/):**
  New 3-layer architecture components:
  - `proto/`: Protobuf definitions for gRPC.
  - `grpc_service/`: High-performance signal generation service.
  - `graphql_gateway/`: GraphQL API for flexible querying.
  - `crypto/`: Cryptographic signing and audit logging.

- **Tests (tests/):**  
  An optional folder for unit tests to ensure your code behaves as expected.


## Folder Structure
```
quant-pipeline/
├── alpha_models/                               # Traditional strategy modules
│   ├── __init__.py                             # Package initializer
│   ├── base_model.py                           # Base class for all models
│   ├── basket_trading.py                       # Basket Trading Strategy
│   ├── buy_and_hold.py                         # Buy and Hold Strategy
│   ├── index_rebalancing.py                    # Index Rebalancing Strategy
│   ├── mean_reversion.py                       # Mean Reversion Strategy
│   ├── moving_average_crossover.py             # Moving Average Crossover Strategy
│   ├── pairs_trading.py                        # New file for Pairs Trading Strategy
│   └── trend_following.py                      # Trend Following Strategy
├── api_layer/                                  # API Layer
│   ├── main.py                                 # FastAPI Application
├── backtesting/                                # Backtesting framework
│   ├── __init__.py                             # Package initializer
│   ├── backtester.py                           # Backtester class
│   └── parameter_generator.py                  # Parameter generator for backtesting
├── cli/                                        # Command-line interface
│   ├── __init__.py                             # Package initializer
│   └── run_pipeline.py                         # CLI runner to execute the pipeline
├── config/                                     # Configuration files
│   ├── __init__.py                             # Package initializer
│   └── settings.py                             # Centralized settings
├── dashboard_app/                              # Dashboard interface
│   ├── controllers/                            # Business Logic Controllers
│   │    ├── analysis_controller.py
│   │    ├── optimization_controller.py
│   │    └── statistics_controller.py
│   ├── ui_components/                          # UI components
│   │    ├── __init__.py                        # Package initializer
│   │    ├── analysis_tab.py                    # Analysis Tab Components
│   │    ├── optimization_tab.py                # Optimization Tab Components
│   │    ├── portfolio_tab.py                   # Portfolio Tab Components
│   │    ├── sidebar.py                         # Sidebar Components
│   │    └── stats_tab.py                       # Stats Tab Components
│   ├── dashboard.py                            # Main Dashboard Application
│   ├── database_manager.py                     # Database Manager
│   ├── portfolio_manager.py                    # Portfolio Manager
│   ├── results_manager.py                      # Results Manager
│   ├── universe_manager.py                     # Universe Manager
│   ├── utils.py                                # Utility functions
│   └── watchlist_manager.py                    # Watchlist Manager
├── data/                                       # Folder for storing additional data files
│   └── universe.csv                            # Universe data
├── data_pipeline/                              # Core data pipeline functionality
│   ├── __init__.py                             # Package initializer
│   ├── crypto_pipeline.py                      # CryptoPipline class
│   ├── data_enricher.py                        # DataEnricher class
│   ├── dynamic_universe.py                     # DynamicUniverse class
│   ├── equity_pipeline.py                      # EquityPipeline class
│   ├── fundamental_pipeline.py                 # FundamentalPipeline class
│   ├── time_series_normalizer.py               # TimeSeriesNormalizer class
│   └── universe_fetcher.py                     # UniverseFetcher class
├── ml_models/                                  # Machine learning components
│   ├── __init__.py                             # Package initializer
│   ├── eda.py                                  # Updated to load data from CSV or DB
│   ├── model_training.py                       # Contains model training routines
│   └── signal_generation.py                    # Converts model outputs to trading signals
├── notebooks/                                  # Jupyter notebooks for interactive work
│   ├── 01_data_collection_and_cleaning.ipynb   # Notebook for data fetching and cleaning
│   └── 02_alpha_research.ipynp                 # Notebook for alpha and signal generation research
├── screeners/                                  # Folder for screeners
│   ├── init.py                                 # Package initializer
│   ├── base_screener.py                        # Base class for all screeners
│   ├── low_volatility_screener.py              # Low Volatility Screener
│   ├── momentum_screener.py                    # Momentum Screener
│   └── screener_pipeline.py                    # Screnner Pipeline
├── services/                                   # 3-Layer Architecture Services
│   ├── crypto/                                 # Cryptography & Audit Log
│   ├── graphql_gateway/                        # GraphQL API Gateway
│   ├── grpc_service/                           # gRPC Signal Service
│   └── proto/                                  # Protobuf definitions
│── tests/                                      # Unit tests for your project
│   ├── __init__.py                             # Package initializer
│   ├── __main__.py                             # Entry point for unit tests
│   ├── test_dynamic_universe.py                # Unit tests for DynamicUniverse
│   └── test_pipeline_orchestrator.py           # Unit tests for PipelineOrchestrator
├── CHANGELOG.md                                # Changelog
├── environment.yml                             # Conda environment specification
├── pyproject.toml                              # PEP 517 configuration file
├── quant_pipeline.db                           # SQLite database
├── README.md                                   # This file
├── run_pipeline.sh                             # Main orchestration script
├── verify_all.py                               # Verification script for 3-Layer Arch
└── setup.py                                    # Setup script for packaging the project
```

---

## Getting Started

### Prerequisites

Python 3.11 and [Poetry](https://python-poetry.org/) (authoritative since 2026-07-31; `environment.yml` and the conda-based setup have been retired).

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/quant-pipeline.git
   cd quant-pipeline
   ```

2. **Install dependencies:**
   ```bash
   poetry install
   ```

`run_pipeline.sh` activates the Poetry environment automatically and exits with instructions if it isn't set up.

## Usage

### 1. Run Everything (Recommended)
Start the Dashboard, API, and gRPC service, plus run verification checks in one command:
```bash
./run_pipeline.sh all
```
*Press `Ctrl+C` to stop all services.*

### 2. Manual/Individual Commands

**Run Data Pipeline:**
```bash
./run_pipeline.sh
```

**Start API Server:**
```bash
./run_pipeline.sh api
```

**Start Dashboard:**
```bash
./run_pipeline.sh dashboard
```

**Start gRPC Server:**
```bash
./run_pipeline.sh grpc
```

**Run Verification:**
```bash
./run_pipeline.sh verify
```

### Workflow

1.  **Run the Pipeline**: Ensure your data is up-to-date by running `python -m cli.run_pipeline`.
2.  **Launch the Dashboard**: Start the app with `streamlit run dashboard_app/dashboard.py`.
3.  **Select a Strategy**: In the dashboard's sidebar, choose a strategy like "Moving Average Crossover" from the dropdown.
4.  **Set Parameters**: Adjust the parameters, such as the short and long window for the moving averages, and select the assets to test.
5.  **Run Backtest**: Click the "Run Backtest" button.
6.  **Analyze Results**: View the equity curve, performance statistics (Sharpe Ratio, Max Drawdown), and trade logs in the results tabs.

## 3-Layer Architecture (gRPC, GraphQL, Crypto)
We have integrated a verifiable signal generation architecture:

1.  **Core Services (gRPC)**: 
    - High-performance, typed service (`SignalService`).
    - Generates trading signals using strategies (e.g., Mean Reversion).
2.  **Gateway (GraphQL)**: 
    - Flexible query interface for clients.
    - Connects to the gRPC backend.
3.  **Crypto & Audit**:
    - **Signing**: All signals are signed using Ed25519.
    - **Hashing**: Payloads are hashed with SHA256.
    - **Audit Log**: An append-only log (`audit_log.json`) chains hashes to create a verifiable history of all generated signals.

Verify the integrity of the system at any time by running:
```bash
./run_pipeline.sh verify
```

## Detailed Workflows

This section provides step-by-step instructions for the primary features of the Quant Pipeline dashboard.

### 1. Running the Data Pipeline

The data pipeline is the foundation of the framework. You have two primary modes for running it.

#### Full Backfill (Initial Setup)

This mode downloads the entire available price history for all assets in your defined universe. It should be run the first time you set up the project or if you need to perform a complete data refresh.

#### Incremental Update (Daily Use)

This mode is much faster and only fetches data from the last recorded date in your database up to the present. This is the command you should run daily to keep your data current.

### 2. Managing Watchlists

Watchlists allow you to create and track custom groups of assets.
#### 1. Create a Watchlist: 
- In the dashboard sidebar, navigate to the "Watchlist" section.
- Click "Create New Watchlist".
- Enter a unique name (e.g., "My Favorite Tech Stocks").
- Type in the tickers you want to include (e.g., `AAPL`, `MSFT`, `GOOGL`).
- Click "Save Watchlist".

#### 2. Edit a Watchlist:
- Select an existing watchlist from the dropdown menu.
- The current list of tickers will appear.
- Add or remove tickers as needed.
- Click "Update Watchlist" to save your changes.
 
#### 3. Delete a Watchlist:
- Select the watchlist you wish to remove.
- Click the "Delete Watchlist" button and confirm the action.

`A demonstration of creating, editing, and deleting a watchlist. !`

Watchlist Management GIF

### 3. Managing Portfolios
The portfolio feature allows you to construct and track hypothetical portfolios based on 
specific asset allocations.
#### 1. Create a Portfolio:
- Navigate to the "Portfolio" tab.
- Click "Create New Portfolio".
- Provide a name and set the initial virtual capital (e.g., $100,000).
- Add assets from your universe and assign them a target weight (e.g., `SPY` at 60%, `BND` at 40%).
- Click "Save Portfolio".
#### 2. Edit or Rebalance a Portfolio:
- Load an existing portfolio from the dropdown.
- Adjust the target weights of the assets.
- Click the "Rebalance" or "Update Portfolio" button to apply the changes.
#### 3. Delete a Portfolio:
- Select the portfolio you want to remove and click the "Delete" button.

`A view of the portfolio creation and management tab. !`

Portfolio Management Screenshot

### 4. Using the Screener
Screeners help you filter the entire investment universe down to a small list of assets that meet specific criteria.
#### 1. Run a Screen:
- Go to the "Screener" tab in the dashboard.
- From the dropdown, select a screener type (e.g., "Momentum Screener").
- Choose the universe to run the screen on (e.g., "S&P 500").
- Click "Run Screener".

#### 2. Use the Results:
- The screener will output a table of assets that passed the filter.-
- You can typically save this list directly as a new watchlist, which you can then use for backtesting.

`The screener tab showing results for a momentum screen. !`

Screener Results Screenshot

### 5. Running a Backtest
This is the core analytical feature, allowing you to simulate a strategy's performance on historical data.
#### 1. Set Up the Backtest:
- Navigate to the main "Backtest" or "Analysis" tab.
- Select Assets: Choose a universe, a saved watchlist, or enter individual tickers to test on.
- Select Strategy: Pick a trading strategy from the dropdown (e.g., "Moving Average Crossover").
- Configure Parameters: Adjust the strategy's parameters, such as the moving average windows (e.g., 50 for the short window, 200 for the long window).
- Set Date Range: Define the start and end dates for the simulation.
#### 2. Execute and Analyze:
- Click the "Run Backtest" button.
- The results will be displayed across several tabs:
  - Portfolio: An equity curve chart comparing your strategy to a "Buy and Hold" benchmark.
  - Statistics: A table of key performance indicators (KPIs) like CAGR, Sharpe Ratio, Max Drawdown, and Calmar Ratio.
  - Trades: A detailed log of every trade executed during the backtest.
#### 3. Save and Load Results:
- After a run, click "Save Results" and provide a name to store the backtest configuration and results.
- You can reload any saved run from the "Load Saved Results" dropdown to instantly view its performance without re-running the simulation.

`A complete workflow showing the backtesting process from setup to analysis. !`
 
Backtesting Workflow GIF

### **Contributing**

Feel free to fork the repository and submit pull requests for improvements, bug fixes, or additional features.

### **License**

[MIT License](https://opensource.org/license/mit)
    
