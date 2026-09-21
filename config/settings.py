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

# --- Crypto identity (core/crypto_identity.py) ---
#
# A TICKER IS NOT AN IDENTIFIER. CoinGecko's "mnt" is Mantle; Yahoo's MNT-USD
# is a micro-cap called MINTY. Audited 2026-09-17: 22 of 99 crypto assets held
# a different token's entire history, 27,076 bars — Uniswap served as "UNICORN
# Token", Aptos as "Apricot Finance", Sui as "Salmonation".

#: Above this ratio between the reference price and the provider's, the two are
#: not the same asset. Deliberately tight: a quote taken minutes apart moves
#: fractions of a percent, and the real substitutions are not marginal — the
#: SMALLEST wrong-asset gap measured was 2x (PEPE vs PEPEGOLD) and the largest
#: 1.4e11 (SPX6900 vs SPEXY). Nothing observed sits between 1.5 and 2.
CRYPTO_PRICE_GAP_TOLERANCE = 1.5

#: Symbols whose reference coin cannot be found by market-cap paging, mapped
#: to their stable CoinGecko id. Two distinct reasons, both measured
#: 2026-09-20 and neither fixable by asking for more pages:
#:
#:   METH-USD  `mantle-staked-ether` has market_cap_rank = None. The
#:             /coins/markets endpoint is ORDERED BY market cap, so an
#:             unranked coin appears on no page at all, ever.
#:
#:   TON-USD   Toncoin has been RENAMED. CoinGecko id `the-open-network` now
#:             carries the symbol GRAM ("Gram (prev. Toncoin)", rank 30), so
#:             a lookup keyed on "TON" finds nothing however deep it pages.
#:             A crypto ticker rename, the same shape as BK->BNY.
#:
#: An entry here asserts only WHICH COIN WE MEANT. It does not assert that the
#: price provider serves it — that is exactly what the audit then checks, and
#: both of these turned out to be wrong assets (261x and 1.6e4x gaps).
#: Pause between CoinGecko calls. The free tier rate-limits, and the failure
#: is SILENT in the way that matters: `fetch_crypto_references` stops early and
#: returns a SHORTER reference set, so symbols simply become UNVERIFIABLE
#: rather than erroring. Measured 2026-09-20 — an un-paced 4-page run returned
#: 200 coins instead of 400 and dropped both id overrides, which read as "these
#: coins do not exist" rather than "we were throttled".
COINGECKO_REQUEST_DELAY_SECONDS = 2.0

#: Retries for a 429 from CoinGecko, with exponential backoff between them.
#: Worth retrying rather than failing soft: a throttled reference lookup makes
#: a coin look ABSENT, and absent means UNVERIFIABLE — an answer that is wrong
#: in a way nobody can see. Better to wait than to record a false verdict.
COINGECKO_MAX_RETRIES = 4

#: First backoff after a 429, doubling each retry. CoinGecko's free-tier window
#: is about a minute, so starting at the 2s inter-request delay exhausts four
#: attempts in 14s and still fails. Measured 2026-09-20.
COINGECKO_THROTTLE_BACKOFF_SECONDS = 15.0

CRYPTO_ID_OVERRIDES = {
    "TON-USD": "the-open-network",
    "METH-USD": "mantle-staked-ether",
}

#: Price alone CANNOT settle a stablecoin: every stablecoin is $1, so BUIDL
#: (BlackRock) and BUIDL (DFOhub) have a gap of ~1.0 while being unrelated.
#: For those the name is the only signal, and a name mismatch there means
#: "a human must look", not "wrong" — because a legitimate alias looks
#: identical to a substitution. LEO Token really is named UNUS SED LEO.

# --- Bar plausibility (core/ingest.py) ---
#: A one-day move beyond this multiple is FLAGGED, never dropped. Yahoo's own
#: TIA-USD closes 0.0105 then 7149.41 (680,637x) on 2024-03-26 — verified
#: present at the provider, not an ingest fault.
#:
#: Flagged rather than rejected on purpose. An all-NULL bar carries no
#: information and is dropped; a 10x move carries plenty — either the provider
#: is wrong or something real happened, and crypto genuinely does 10x in a day
#: (BONK, WIF and FARTCOIN are all in this universe). Dropping the spike would
#: also leave the NEXT day's move impossible, trading one bad bar for another,
#: and would open a hole indistinguishable from a provider outage.
MAX_DAILY_MOVE_MULTIPLE = 10.0

# --- Option chain capture (scripts/capture_option_chains.py) ---
# yfinance serves only TODAY's chain, so the archive can only grow forward: a
# weekday the capture does not run is missing for good, at any price. Capture
# first, schema second — raw vendor frames go to dated Parquet with provenance
# columns only, and loading them into a table is a later, re-runnable step.

#: Small on purpose. Grow deliberately — a <=120 DTE SPY chain is ~19 expiries
#: and several thousand rows per day.
OPTION_CAPTURE_TICKERS = ("SPY", "QQQ", "IWM", "TQQQ", "AAPL", "NVDA")

#: Expiries further out than this many calendar days are not fetched.
OPTION_CAPTURE_MAX_DTE = 120

#: A ticker's whole capture below this many contracts is a failed read, not a
#: thin chain — the smallest here (TQQQ) returns hundreds. Catches the SHAPE
#: failure (Yahoo returning an empty or truncated chain), not market activity.
OPTION_CAPTURE_MIN_ROWS = 20

#: data/ is gitignored. This directory is the asset — see the as-built for the
#: fact that it currently has exactly one copy.
OPTION_CHAIN_ARCHIVE_DIR = ROOT_DIR / "data" / "option_chains"

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
