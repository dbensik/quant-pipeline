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

#: MediaWiki API, used only to read PAST revisions of the S&P 500 page for
#: scripts/reconstruct_sp500_membership.py. The daily snapshot reads the live
#: page above.
URL_WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_SP500_PAGE_TITLE = "List of S&P 500 companies"
#: First month-end reconstructed. The page's constituents table is parseable
#: and carried ~500 rows from at least here on (checked 2026-10-04).
SP500_RECONSTRUCTION_START = "2014-12-31"
#: A month-end list differing from the previous accepted one by more symbols
#: than this (added + removed) is treated as a vandalised or reshaped revision
#: and an older revision is tried instead. Real months, renames included, peak
#: at 16 (2019-12, measured against Clenow's file), so 40 catches a broken
#: page without firing on turnover.
SP500_RECONSTRUCTION_MAX_MONTHLY_CHANGE = 40
#: Seconds between MediaWiki requests. Unpaced requests were refused after
#: about a dozen calls on 2026-10-04.
WIKIPEDIA_REQUEST_DELAY_SECONDS = 1.5
URL_NASDAQ100_WIKIPEDIA = "https://en.wikipedia.org/wiki/Nasdaq-100"
URL_COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"

#: The header CoinGecko reads a Demo API key from. From 2026-09-29 the public
#: API answered 403 to keyless requests from this machine and the daily
#: top_100_crypto snapshot failed — a lost index-day that cannot be backdated.
COINGECKO_KEY_HEADER = "x-cg-demo-api-key"


def coingecko_api_key():
    """
    The CoinGecko Demo API key from the environment or the project's .env, or
    None. Read on every call so a key added to .env takes effect without a
    restart.

    NEVER put it in a URL. CoinGecko also accepts it as a query parameter, but
    request URLs appear in error logs — the 2026-09-29 403 was logged with its
    full URL — so the key travels only as a header.
    """
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _Key(BaseSettings):
        model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env", extra="ignore")
        COINGECKO_API_KEY: str = ""

    return _Key().COINGECKO_API_KEY.strip() or None


def coingecko_headers() -> dict:
    """Headers for a CoinGecko request: the key if one is set, else none.

    Pass per request, never on a shared session: the universe fetcher's
    session also scrapes Wikipedia and Slickcharts, and a session-wide header
    would hand the key to both."""
    key = coingecko_api_key()
    return {COINGECKO_KEY_HEADER: key} if key else {}

# --- Tiingo (scripts/probe_tiingo_delisted.py) ---
#
# Evaluated 2026-10-04 as a free source of DELISTED price history, which Yahoo
# does not keep. Nothing in ingest uses it; only the probe script does.
URL_TIINGO_DAILY = "https://api.tiingo.com/tiingo/daily"
#: Free-tier ceiling. The probe stops short of it rather than be refused.
TIINGO_MAX_REQUESTS_PER_HOUR = 50


def tiingo_headers() -> dict:
    """
    Headers for a Tiingo request, or {} when no key is set.

    The key comes from TIINGO_API_KEY in the environment or .env, read on
    every call. Tiingo also accepts it as a `token` query parameter; it is
    sent only as a header, for the reason given on `coingecko_api_key`.
    """
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _Key(BaseSettings):
        model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env", extra="ignore")
        TIINGO_API_KEY: str = ""

    key = _Key().TIINGO_API_KEY.strip()
    return {"Authorization": f"Token {key}"} if key else {}


# --- SEC EDGAR (core/sec.py, tier 3: research/dcf-plan-2026-10-08.md) ---
#
# The SEC refuses an undeclared client with an HTML 403 page ("Your Request
# Originates from an Undeclared Automated Tool"), measured 2026-10-08. A
# declared client sends "Name email@domain" as its User-Agent. WHOSE name and
# email is Danny's decision, so it comes only from SEC_USER_AGENT in .env and
# nothing here supplies a default.
URL_SEC_COMPANY_FACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
URL_SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
URL_SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
#: Published limit is 10 per second; stay under it.
SEC_MAX_REQUESTS_PER_SECOND = 8
SEC_TIMEOUT_SECONDS = 30


def sec_user_agent():
    """SEC_USER_AGENT from the environment or .env, or None. Read per call."""
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _UA(BaseSettings):
        model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env", extra="ignore")
        SEC_USER_AGENT: str = ""

    return _UA().SEC_USER_AGENT.strip() or None


#: The us-gaap / dei concepts stored (raw, insert-only) by core/fundamentals.py.
#: Everything a DCF's FCF drivers, the capital structure and per-share values
#: need; the ordered concept lists that turn these into line items live in
#: modeling/statements.py. Measured on AAPL 2026-10-08: each of these present.
FUNDAMENTAL_CONCEPTS = (
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
    ("us-gaap", "Revenues"),
    ("us-gaap", "SalesRevenueNet"),
    ("us-gaap", "CostOfGoodsAndServicesSold"),
    ("us-gaap", "OperatingIncomeLoss"),
    ("us-gaap", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"),
    ("us-gaap", "IncomeTaxExpenseBenefit"),
    ("us-gaap", "InterestExpense"),
    ("us-gaap", "InterestExpenseNonoperating"),
    ("us-gaap", "NetIncomeLoss"),
    ("us-gaap", "DepreciationDepletionAndAmortization"),
    ("us-gaap", "DepreciationAmortizationAndAccretionNet"),
    ("us-gaap", "ShareBasedCompensation"),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
    ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
    ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
    ("us-gaap", "MarketableSecuritiesCurrent"),
    ("us-gaap", "LongTermDebt"),
    ("us-gaap", "LongTermDebtNoncurrent"),
    ("us-gaap", "LongTermDebtCurrent"),
    ("us-gaap", "CommercialPaper"),
    ("us-gaap", "StockholdersEquity"),
    ("us-gaap", "Assets"),
    ("us-gaap", "Liabilities"),
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
    ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
    ("us-gaap", "EarningsPerShareDiluted"),
    ("dei", "EntityCommonStockSharesOutstanding"),
)

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

#: Identity verdicts a HUMAN has settled on evidence the automated check
#: cannot see. Without this the audit would recompute SUSPECT from name and
#: price on every run and silently undo the decision.
#:
#: Both entries below were resolved by `audit_crypto_bounds.py`: price at a
#: point cannot separate two $1 stablecoins, but a price HISTORY can, because
#: a coin cannot have traded before it existed.
#:
#:   BUIDL-USD  763 of 792 bars (96.3%) outside BlackRock BUIDL's all-time
#:              range, earliest violations 2020 — years before the fund.
#:              Yahoo serves DFOhub.
#:   USDS-USD   67 of 1091 bars outside USDS's range, violations dating from
#:              2020-02. Yahoo serves "Stably USD".
#:
#: Deliberately NOT a threshold rule. USDS violates on 6.1% of bars and BUIDL
#: on 96.3%; any percentage that promotes the first is arbitrary enough to
#: misfire elsewhere. What settles both is the DATE of the violations, which
#: is a judgement about each coin's history rather than a number.
CRYPTO_IDENTITY_OVERRIDES = {
    "BUIDL-USD": "wrong_asset",
    # USDS-USD was settled wrong_asset here until 2026-10-04. That verdict was
    # about Yahoo's bare `USDS-USD` ("Stably USD"). The asset is now fetched
    # as USDS33039-USD (CRYPTO_PROVIDER_SYMBOLS), whose name is USDS and whose
    # history begins 2024-09-19 with no close outside USDS's range, so the
    # entry was removed rather than left to contradict a computed MATCH.
    # --- Settled 2026-09-23, the opposite direction. ---
    # Mapping CoinGecko ids onto the unranked tokens turned three of them
    # SUSPECT, which BLOCKS INGESTION — so a mapping intended to improve
    # coverage silently stopped three legitimate assets from updating. All
    # three are the same asset written two ways, with prices agreeing to
    # within 0.5%:
    #
    #   SOLVBTC-USD  "Solv Protocol BTC"  vs "SolvBTC"          gap 1.005x
    #   WSTETH-USD   "Wrapped stETH"      vs "Lido wstETH"      gap 1.003x
    #                (wstETH IS Lido's wrapped stETH)
    #   JLP-USD      "Jupiter Perpetuals Liquidity Provider Token"
    #                                     vs "Jupiter Perps LP" gap 1.000x
    #
    # The name matcher uses containment after dropping noise words, which
    # cannot see an abbreviation ("Perps" for "Perpetuals") or a concatenation
    # ("SolvBTC" for "Solv BTC"). Loosening it was rejected: for a ~1x price
    # gap the NAME IS THE ONLY SIGNAL, and that is precisely where a looser
    # match would start waving through real substitutions like BUIDL/DFOhub.
    # Better a narrow matcher plus explicit human decisions than a broad one
    # nobody can audit.
    "SOLVBTC-USD": "match",
    "WSTETH-USD": "match",
    "JLP-USD": "match",
}

#: The ticker to ASK THE PRICE PROVIDER FOR, where it is not the asset's own
#: symbol. Yahoo files most mid-cap coins under the ticker plus a numeric id
#: (`UNI7083-USD` is Uniswap); the bare ticker belongs to whichever micro-cap
#: claimed it first (`UNI-USD` is "UNICORN Token"). That collision is what
#: left 24 assets holding another coin's history until 2026-09-24.
#:
#: The asset keeps its symbol everywhere else — registry, bars, snapshots,
#: API. Only the fetch and the identity audit use the provider ticker.
#:
#: An entry is a CLAIM, not a verdict. Ingest refuses a mapped symbol until
#: `scripts/audit_crypto_identity.py --write` has checked this exact provider
#: ticker against the coin's CoinGecko name and price and recorded it
#: (`identity_provider_symbol`), so editing a line here blocks the asset until
#: it is re-verified rather than silently importing a different coin.
#:
#: Found 2026-10-04 by searching Yahoo for each coin's name and keeping the
#: candidate whose price agreed with CoinGecko. Margins were decisive: the
#: right ticker was within 1% every time and the nearest wrong one 6.5x away
#: (SKY-USD, Skycoin), except PEPE24549-USD ("Arbi Pepe"), which sits within
#: 0.03% of Pepe's price and is rejected on its NAME.
#:
#: Each mapped history was then checked against the coin's CoinGecko all-time
#: range before a bar was stored: 0 closes outside it for every entry but
#: TAO-USD, whose first bar (2023-03-05, $0.126 against an all-time low of
#: $30.83) is excluded by `history_valid_from`.
#:
#: DELIBERATELY NOT MAPPED, so these stay empty:
#:   PEPE-USD   PEPE24478-USD is the right coin, but Yahoo rounds its prices
#:              to six decimals: 27 distinct closes in 1254 bars and 906 days
#:              of exactly zero return. A series, not a usable one.
#:   TON-USD    TON11419-USD is the right coin and serves ONE bar of history.
#:   BSC-USD, BUIDL-USD   no Yahoo ticker found for either.
CRYPTO_PROVIDER_SYMBOLS = {
    "APT-USD": "APT21794-USD",
    "ARB-USD": "ARB11841-USD",
    "CBBTC-USD": "CBBTC32994-USD",
    "HYPE-USD": "HYPE32196-USD",
    "JUP-USD": "JUP29210-USD",
    "LBTC-USD": "LBTC33652-USD",
    "METH-USD": "METH29035-USD",
    "MNT-USD": "MNT27075-USD",
    "PENGU-USD": "PENGU34466-USD",
    "PI-USD": "PI35697-USD",
    "POL-USD": "POL28321-USD",
    "PUMP-USD": "PUMP36507-USD",
    "S-USD": "S32684-USD",
    "SKY-USD": "SKY33038-USD",
    "SPX-USD": "SPX28081-USD",
    "STX-USD": "STX4847-USD",
    "SUI-USD": "SUI20947-USD",
    "TAO-USD": "TAO22974-USD",
    "TRUMP-USD": "TRUMP35336-USD",
    "UNI-USD": "UNI7083-USD",
    "USDE-USD": "USDE29470-USD",
    "USDS-USD": "USDS33039-USD",
}

CRYPTO_ID_OVERRIDES = {
    "TON-USD": "the-open-network",
    "METH-USD": "mantle-staked-ether",
    # --- Mapped 2026-09-23 from CoinGecko's full 21,382-coin index. ---
    # These are all UNRANKED (liquid-staking and wrapped tokens sit below the
    # top 400 by market cap) so /coins/markets paging reaches none of them, the
    # same structural gap that hid mantle-staked-ether.
    #
    # Where a symbol had several candidates, the largest by market cap wins,
    # and the margin was decisive every time: WBTC $10.1B vs $635M for the next
    # (an Arbitrum bridge wrapper), WETH $5.75B vs $1.4B, wstETH $12.9B vs
    # $216M. The rest of each symbol's candidates are chain-specific bridge
    # wrappers, not the canonical token.
    "BNSOL-USD": "binance-staked-sol",
    "CBBTC-USD": "coinbase-wrapped-btc",
    "EZETH-USD": "renzo-restaked-eth",
    "JITOSOL-USD": "jito-staked-sol",
    "JLP-USD": "jupiter-perpetuals-liquidity-provider-token",
    "LBTC-USD": "lombard-staked-btc",
    "OSETH-USD": "stakewise-v3-oseth",
    "RETH-USD": "rocket-pool-eth",
    "RSETH-USD": "kelp-dao-restaked-eth",
    "SOLVBTC-USD": "solv-btc",
    "STETH-USD": "staked-ether",
    "SUSDE-USD": "ethena-staked-usde",
    "USDT0-USD": "usdt0",
    "WBTC-USD": "wrapped-bitcoin",
    "WEETH-USD": "wrapped-eeth",
    "WETH-USD": "weth",
    "WSTETH-USD": "wrapped-steth",
    # BSC-USD is the case the original bug was built on. Its CoinGecko symbol
    # is literally "bsc-usd", so stripping "-USD" to get a base symbol leaves
    # "BSC" — which resolves to a $119k micro-cap called Binance Super Cycle.
    # Looked up by the FULL symbol instead.
    "BSC-USD": "binance-bridged-usdt-bnb-smart-chain",
    #
    # DELIBERATELY NOT MAPPED — an entry here asserts which coin we meant, and
    # neither of these can be asserted:
    #
    #   FTN-USD   "Fasttoken" appears NOWHERE in CoinGecko's index: zero hits
    #             on symbol, name or id across all 21,382 coins. It was in the
    #             top 100 when registered, so it has been delisted from the
    #             reference entirely. Nothing to check against.
    #
    #   IP-USD    The only lead is CoinGecko id `story-2`, now named "Data
    #             Network" with symbol DATA (rank 332). Story Protocol's ticker
    #             was IP and CoinGecko keeps an id across a rename — which is
    #             how the-open-network still holds Toncoin's history under the
    #             symbol GRAM. But that is an inference from an ID STRING, with
    #             no name or price agreeing, and asserting a coin's identity
    #             from a string is the exact mistake this whole subsystem
    #             exists to correct. Left unverifiable on purpose.
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

# --- Incremental ingest overlap (core/ingest.py) ---
#: An incremental run re-requests this many calendar days BEFORE the newest
#: stored bar, inserting only bars that are missing (existing ones are never
#: touched). Before 2026-09-25 the window began the day after the newest bar,
#: so a day lost while a later one landed was never requested again: 463 of
#: 527 equities lost 2026-08-28 around a four-morning DB outage, and DXCM/MGM
#: lost 2026-09-22 when Yahoo errored and then served nothing usable for that
#: day for two more runs. 14 days covers a week-long outage plus that lag.
INGEST_OVERLAP_DAYS = 14

# --- Read-time price adjustment (db/repositories/market_data.fetch_range) ---
#: THE cutover switch of research/dividend-drift-plan-2026-09-27.md. When True,
#: assets with price_basis = 'served' are read from their served values and
#: corporate_actions, adjusted at read time; when False every read returns the
#: stored columns exactly as before. price_basis was written in phase 4, so
#: without this switch the new read path would go live the moment any process
#: reloaded — the API, or the 06:00 job — before phase 6 verified it.
#: Turned ON 2026-09-28 (phase 6), after: 516 symbols gated, returns matching
#: Yahoo to 0.00006pp through fetch_range, whole bars checked, and the fresh-
#: return check going from 250 mismatched days (stored) to the result recorded
#: in the plan. Set False to return every read to the stored columns.
SERVED_PRICES_ENABLED = True

# --- Risk-free rate (core/rates.py, tier 2 phase 0) ---
# Plan: research/option-pricing-plan-2026-10-08.md, decision 1. Replaces the
# hardcoded 0.02 for new code; optimize.py and data_enricher.py still carry it.
#
# ^IRX is the 13-week T-bill rate QUOTED ON A BANK-DISCOUNT BASIS, in percent.
# Checked 2026-10-08 against Treasury's Daily Treasury Bill Rates: ^IRX
# 3.982-4.037 against the 13-week bank-discount close 4.00-4.05 over
# 2026-10-01..07 (within ~0.02, an intraday snapshot), and 0.11 below the
# coupon-equivalent column. Stored exactly as served; converted on read.
RISK_FREE_RATE_SERIES = "^IRX"
#: Days to maturity of a 13-week bill, for the discount-to-yield conversion.
RISK_FREE_RATE_TENOR_DAYS = 91
#: First date fetched on an empty table: SPY's first stored bar.
RISK_FREE_RATE_HISTORY_START = "2015-01-02"
#: A read whose newest observation is older than this many calendar days is
#: reported stale. Bond-market holidays (Columbus Day, Veterans Day) are stock
#: sessions with no bill print, so a one-day gap is normal; a week is not.
RISK_FREE_RATE_MAX_AGE_DAYS = 7

# --- Option pricing kernels (pricing/, tier 2 phase 1) ---
# Plan: research/option-pricing-plan-2026-10-08.md. Pricing time is ACT/365;
# realised volatility is annualised over trading days. Never mixed in one function.
#: Implied-vol search interval for brentq, as annual sigma.
OPTIONS_IV_BOUNDS = (1e-4, 5.0)
#: Cox-Ross-Rubinstein steps for the American price.
BINOMIAL_STEPS = 500
#: Risk-neutral Monte Carlo paths (antithetic pairs count as two).
MC_PRICER_PATHS = 200_000
#: Rolling windows, in trading days, for realised vol and the vol cone.
REALISED_VOL_WINDOWS = (10, 20, 60, 120)
#: Trading days per year, for annualising realised volatility only.
REALISED_VOL_DAYS_PER_YEAR = 252

# --- Option chains and the surface (pricing/chains.py, tier 2 phase 2) ---
#: A bid below this is "no bid": its mid is not a price.
OPTIONS_MIN_BID = 0.01
#: Expiries closer than this many calendar days are flagged zero_dte and kept
#: out of the surface: T is minutes, and any timestamp skew dominates the IV.
OPTIONS_EXCLUDE_DTE_BELOW = 1
#: Near-the-money call/put pairs whose parity forwards are medianed per expiry.
OPTIONS_FORWARD_PAIRS = 6
#: The forward/premium joint solve stops when F moves less than this (relative).
OPTIONS_FORWARD_TOL = 1e-7
OPTIONS_FORWARD_MAX_PASSES = 10
#: An expiry within this many days of a PROJECTED ex-date is flagged: the
#: projection is the last ex-date plus the median gap, and can miss by days.
OPTIONS_DIVIDEND_DATE_UNCERTAIN_DAYS = 5
#: A last trade older than this many days before the capture is flagged stale.
OPTIONS_STALE_TRADE_DAYS = 5

# --- Monte Carlo simulation (api/routers/simulate.py) ---
# Plan: research/monte-carlo-plan-2026-10-07.md. The kernels in simulation/
# take every parameter explicitly; these are the router's defaults and caps.
#: `returns` mode resamples the strategy's own daily returns — instant.
SIM_DEFAULT_PATHS = 2_000
#: paths x horizon x 8 bytes, with three arrays alive at once: 5,000 paths over
#: a 3,000-bar history is ~360 MB. 20,000 would be 1.4 GB.
SIM_MAX_PATHS = 5_000
#: `prices` mode re-runs the strategy once per path (~5 ms a run since the
#: 2026-10-07 backtester change), so the cap is time, not memory.
SIM_DEFAULT_RERUN_PATHS = 200
SIM_MAX_RERUN_PATHS = 1_000
#: Stationary-bootstrap mean block, about one trading month: long enough to
#: keep volatility clustering, short enough that a 10-year series still mixes.
SIM_BLOCK_LENGTH_DAYS = 20.0
#: P(ruin) = share of paths that ever fall below this fraction of start equity.
SIM_RUIN_THRESHOLD = 0.5
#: A forward horizon may be at most 5 trading years, or the history length if
#: that is longer (the default horizon IS the history length).
SIM_MAX_HORIZON_DAYS = 1_260
SIM_VAR_HORIZONS_DAYS = (1, 10, 21)
#: Sample paths a client may ask for beside the bands, for a spaghetti overlay.
SIM_MAX_RETURNED_PATHS = 200

# --- Reassigned tickers (core/corporate_actions.detect_reassignment) ---
#: A ticker reassigned to another company shows up as a collapse in DOLLAR
#: volume (close x volume): a split leaves it roughly unchanged, a crash usually
#: raises it. PARA's reassignment collapsed it ~450x ($196M/day to ~$440k).
#: Calibrated 2026-09-24 over all 516 equities and 11 ETFs, full history: the
#: largest genuine collapse was JNJ at 8.0x (Kenvue exchange offer) and the
#: 99th percentile 4.6x, so 20x has a wide margin on both sides.
REASSIGNMENT_WINDOW_BARS = 20
REASSIGNMENT_MIN_COLLAPSE = 20.0
#: The collapse must be FROM a real market. Without this floor AMCR fired at
#: 81x in 2019 — placeholder pre-listing bars trading 0 or 20 shares a day,
#: where a single 3,000-share print moves the median by two orders. A ratio
#: between two near-zero numbers is noise. PARA was ~$196M/day before.
REASSIGNMENT_MIN_PRIOR_DOLLAR_VOLUME = 1_000_000.0

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

#: Priced surfaces, one JSON file per ticker-day (tier 2 phase 3). data/ is
#: gitignored; every file is derivable from the archive and can be deleted.
OPTIONS_SURFACE_CACHE_DIR = ROOT_DIR / "data" / "surfaces"
#: Dividends are projected this far past the capture date: the longest expiry
#: captured plus a quarter, so every expiry's schedule is complete.
OPTIONS_DIVIDEND_HORIZON_DAYS = OPTION_CAPTURE_MAX_DTE + 92
#: Surfaces computed at once. A miss costs 10-35 s of CPU; more in parallel
#: only contend for the interpreter lock.
OPTIONS_SURFACE_MAX_CONCURRENT = 1

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
