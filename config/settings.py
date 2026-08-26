import os
from pathlib import Path

# --- Project Root ---
# Modern, object-oriented, and more readable
ROOT_DIR = Path(__file__).parent.parent

# --- Database Configuration ---
# Use the '/' operator for clean path joining
DB_PATH = ROOT_DIR / "quant_pipeline.db"
DB_PRICE_TABLE = "price_data_daily"
DB_PRICE_DATA_COLUMNS = [
    "Date",
    "Ticker",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "volatility_90d",
    "beta",
    "sharpe_ratio_90d",
    "rsi_14d",
]
DB_NORMALIZED_TABLE = "price_data_normalized"

# --- File-Based Configuration ---
# For simple, user-generated data like watchlists and portfolios.
WATCHLISTS_FILE_PATH = ROOT_DIR / "watchlists.json"
PORTFOLIOS_FILE_PATH = ROOT_DIR / "portfolios.json"

# --- Results Directory ---
RESULTS_DIR = ROOT_DIR / "results"

# --- API & Data Source URLs ---
# Centralizing URLs makes them easy to update if they change.
URL_SP500_WIKIPEDIA = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
# The DJIA constituents are NOT scraped from Wikipedia. That page carried a
# components table until roughly 2026-08-16, when it became a navbox with no
# table at all; the scrape returned empty every morning for ten days and those
# index-days are gone, because snapshots cannot be backdated. Kept here only so
# the next person does not rediscover the dead end.
URL_DOWJONES_WIKIPEDIA = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

# Replacement source, adopted 2026-08-25. Publishes all 30 constituents in one
# table with a clean Symbol column — no pagination, unlike the DIA ETF holdings
# pages, which truncate at 25 rows and would silently under-report.
URL_DOWJONES_CONSTITUENTS = "https://www.slickcharts.com/dowjones"

#: The DJIA is 30 stocks BY DEFINITION — that is what makes it the "Dow 30".
#: No other index here has so exact an invariant, and it is the cheapest
#: possible check that a scrape returned the index rather than some other table
#: on the page. A short read is the dangerous case: it looks like a valid
#: snapshot and quietly drops constituents from point-in-time membership.
DOWJONES_EXPECTED_COUNT = 30

#: The S&P 500 needs a RANGE, not an exact count. It targets 500 COMPANIES but
#: lists more SECURITIES, because a few companies have two share classes in the
#: index (GOOG/GOOGL, FOX/FOXA, NWS/NWSA); the scrape returns 503 today and has
#: sat at 500-505 for years. Committee changes are one or two names at a time,
#: so anything outside this band is a structural change in the source rather
#: than index turnover.
#:
#: Wide on purpose. The failure being caught is a scrape that returns a wrong
#: SHAPE — a handful of rows, or half the table — not a legitimate reconstitution.
#: A band tight enough to catch a 2% miss would false-positive on real changes,
#: and a check that cries wolf gets deleted.
SP500_EXPECTED_RANGE = (480, 520)
URL_NASDAQ100_WIKIPEDIA = "https://en.wikipedia.org/wiki/Nasdaq-100"
URL_COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"

# --- Quant Pipeline REST API (Phase 3) ---
# The dashboard reads prices through the FastAPI service by default as of the
# Phase 3 cutover (2026-08-07). Set QUANT_USE_API=0 to fall back to reading
# SQLite directly — kept as an escape hatch so the dashboard still works when
# the API or TimescaleDB is down, and as the Phase 3 rollback path.
#
# PORT 8001, NOT 8000: the GraphQL gateway owns 8000 (services/config.py
# GRAPHQL_PORT, and `./run_pipeline.sh api`). Running both on 8000 collides —
# it only went unnoticed because they were never started together.
QUANT_API_PORT = int(os.getenv("QUANT_API_PORT", "8001"))
QUANT_API_BASE_URL = os.getenv(
    "QUANT_API_BASE_URL", f"http://127.0.0.1:{QUANT_API_PORT}"
)
QUANT_USE_API = os.getenv("QUANT_USE_API", "1").lower() in {"1", "true", "yes"}
QUANT_API_TIMEOUT_SECONDS = float(os.getenv("QUANT_API_TIMEOUT_SECONDS", "30"))

# --- Pipeline Configuration ---
DEFAULT_START_DATE = "2020-01-01"
PIPELINE_SCRIPT_PATH = ROOT_DIR / "cli" / "run_pipeline.py"

# --- Caching Configuration ---
CACHE_DIR = ROOT_DIR / ".cache"
CACHE_EXPIRY_HOURS = 24  # Default cache expiry
