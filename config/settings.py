import os
from pathlib import Path

# --- Project Root ---
# Modern, object-oriented, and more readable
ROOT_DIR = Path(__file__).parent.parent

# --- Database Configuration ---
# Use the '/' operator for clean path joining
DB_PATH = ROOT_DIR / "quant_pipeline.db"

#: Same-CIK S&P 500 ticker renames, 2014-2026 (FB -> META). Read by CIK
#: resolution and by the screener when a RECONSTRUCTED member list, whose
#: symbols are the tickers of their day, is priced (decision 42).
SP500_TICKER_RENAMES_CSV = ROOT_DIR / "research" / "sp500-ticker-renames-2026-10-04.csv"

#: Snapshotted indexes whose new members the 06:00 job registers (decision 44,
#: scripts/register_new_members.py): index -> (asset class, metadata label).
#: Crypto members are listed, never registered: each needs the identity audit.
NEW_MEMBER_INDEXES = {
    "sp500": ("equity", "S&P 500"),
    "dow_jones": ("equity", "Dow Jones"),
    "top_100_crypto": ("crypto", None),
}
#: First date a newly registered equity is backfilled from, insert-only: the
#: start of every other served equity's history (decision 26).
NEW_MEMBER_BACKFILL_START = "2010-01-01"
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
#: One coin's detail, for its categories (decision 46, core/crypto_class.py).
URL_COINGECKO_COIN = "https://api.coingecko.com/api/v3/coins/{coin_id}"

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


def quant_private_key():
    """
    The signal service's Ed25519 private key (base64 of its 32 raw bytes) from
    the environment or the project's .env, or None. services/config.py read only
    os.environ, so a key written to .env never reached the service and every
    gRPC restart signed with a new ephemeral key (set up 2026-10-15). A secret:
    .env is gitignored and the commit guard refuses it; never log or print it.
    """
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _Key(BaseSettings):
        model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env", extra="ignore")
        QUANT_PRIVATE_KEY: str = ""

    return _Key().QUANT_PRIVATE_KEY.strip() or None


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
#: EDGAR's daily form index: one fixed-width file per business day listing
#: every filing. The quarter's directory listing (JSON) names the days that
#: exist, so a weekend or holiday is never requested (its URL answers 403
#: AccessDenied, which must not be mistaken for a block).
URL_SEC_DAILY_INDEX_DIR = (
    "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/index.json"
)
URL_SEC_DAILY_FORM_INDEX = (
    "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/{name}"
)
#: Forms whose filing changes a company's XBRL facts (amendments included).
SEC_FACT_FORMS = (
    "10-K",
    "10-K/A",
    "10-Q",
    "10-Q/A",
    "10-KT",
    "10-KT/A",
    "10-QT",
    "10-QT/A",
    "20-F",
    "20-F/A",
    "40-F",
    "40-F/A",
)
#: The daily step looks back this many days, so mornings the Mac slept
#: through are caught up (the job has gone seven days without running).
SEC_REFRESH_LOOKBACK_DAYS = 7
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


# --- Backups (scripts/backup.py, last step of the 06:00 job) ---
# Added 2026-10-12. Until then nothing was backed up: no Time Machine
# destination, `data/` gitignored, the database in one Docker volume. The
# option-chain archive and the universe snapshots cannot be re-fetched (yfinance
# serves only today's chain; a missed index-day cannot be backdated), and
# neither can the prices as Yahoo served them, the legacy names it no longer
# serves, or the human decisions recorded on each asset.
#: The Postgres container; its OWN pg_dump makes the dump, so the file restores
#: with any pg_restore of that major version or newer.
BACKUP_DB_CONTAINER = "quant_timescaledb"
#: Project files copied beside the dump (relative to ROOT_DIR). Everything else
#: under data/ is derivable (data/surfaces is a cache).
BACKUP_FILES = (
    "data/option_chains",
    "data/option_chains_postclose",
    "portfolios.json",
    "watchlists.json",
    "audit_log.json",
)
#: Newest daily backups kept, and monthly ones (the first of each month).
BACKUP_KEEP_DAILY = 14
BACKUP_KEEP_MONTHLY = 12


def backup_dir():
    """
    QUANT_BACKUP_DIR from the environment or .env, as a Path, or None. Read
    per call. It lives in .env, not here: the repository is public and the
    path names the Google Drive account.
    """
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _Backup(BaseSettings):
        model_config = SettingsConfigDict(env_file=ROOT_DIR / ".env", extra="ignore")
        QUANT_BACKUP_DIR: str = ""

    value = _Backup().QUANT_BACKUP_DIR.strip()
    return Path(value).expanduser() if value else None


#: A ticker whose filer the ticker map gets WRONG, with the evidence. An entry
#: wins over the map and the rename CSV (core/cik_resolution.py).
CIK_OVERRIDES = {
    # The rename CSV mapped WBA to 104207 'WALGREEN CO', which filed its last
    # 10-Q 2014-12-30 and deregistered (15-12B) 2014-12-31. Walgreens Boots
    # Alliance files as 1618921 from 2015 (41 10-K/10-Qs to 2025-06-26).
    # Checked against SEC submissions 2026-10-11.
    "WBA": 1618921,
}

#: Earlier filers whose history belongs to the same business, read together
#: with the asset's own CIK (latest filed wins per period, as for one CIK).
#: A holding-company reorganisation moves the ticker to a NEW CIK and leaves
#: the history under the old one; a ticker->CIK map cannot express that.
CIK_PREDECESSORS = {
    # 'ExxonMobil Holdings Corp' (2115436) registered as successor issuer by
    # 8-K12B on 2026-07-01; 'EXXON MOBIL CORP' (34088) filed Form 25-NSE on
    # 2026-07-02. 2115436 had 49 facts; 34088 holds the history (10-Ks back
    # to 2009 here). Checked against SEC submissions 2026-10-11.
    "XOM": (34088,),
    # Walgreen Co, the pre-2015 filer of the same business (see CIK_OVERRIDES).
    "WBA": (104207,),
    # Found by the resolver's history-gap check 2026-10-11, each confirmed in
    # SEC submissions (successor 8-K12B / 8-K12G3, predecessor 25-NSE or 15-12):
    "APA": (6769,),  # APACHE CORP -> APA Corp, 8-K12B 2021-03-01
    "APO": (1411494,),  # old Apollo Global Management -> new, 8-K12B 2022-01-03
    "BLK": (1364742,),  # old BlackRock (now 'BlackRock Finance') -> BlackRock, Inc. 2024-10
    "MRVL": (
        1058057,
    ),  # MARVELL TECHNOLOGY GROUP LTD -> Marvell Technology, Inc., 8-K12B 2021-04-20
    "TPL": (97517,),  # TEXAS PACIFIC LAND TRUST -> Texas Pacific Land Corp, 15-12B 2021-01-25
    "BG": (1144519,),  # BUNGE LTD -> Bunge Global SA, 8-K12G3 2023-11-01
}

#: History gaps the check found and a person accepted, with why. A merger of
#: equals has no single predecessor whose statements continue the company's.
CIK_HISTORY_GAP_ACCEPTED = {
    "DD": "DowDuPont 2017 merger of Dow and DuPont: no single predecessor",
    "TKO": "formed 2023 from WWE and UFC: no single predecessor",
    # Same CIK throughout; checked 2026-10-11: ifrs-full facts 2018-08-30..
    # 2022-08-19, us-gaap from 2022-11-04 (US redomicile). The allow-list is
    # us-gaap, so TEAM's history before 2022 is not stored.
    "TEAM": "filed 20-F under IFRS until its 2022 redomicile; us-gaap starts 2022-11-04",
}

#: The resolver flags an equity whose filer's first 10-K/10-Q comes more than
#: this many days after its first stored price bar: the signature of history
#: left under a predecessor CIK (XOM: 2026 filer, bars from 2015).
CIK_HISTORY_GAP_DAYS = 400

#: The us-gaap / dei concepts stored (raw, insert-only) by core/fundamentals.py.
#: Everything a DCF's FCF drivers, the capital structure and per-share values
#: need; the ordered concept lists that turn these into line items live in
#: modeling/statements.py. Measured on AAPL 2026-10-08: each of these present.
FUNDAMENTAL_CONCEPTS = (
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
    # Added 2026-10-11: KHC and SJM tag revenue only under this variant.
    ("us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax"),
    ("us-gaap", "Revenues"),
    ("us-gaap", "SalesRevenueNet"),
    ("us-gaap", "CostOfGoodsAndServicesSold"),
    ("us-gaap", "OperatingIncomeLoss"),
    (
        "us-gaap",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    ),
    ("us-gaap", "IncomeTaxExpenseBenefit"),
    ("us-gaap", "InterestExpense"),
    ("us-gaap", "InterestExpenseNonoperating"),
    # Added 2026-10-11 from a census of the 132 non-financial equities with no
    # interest expense ending in 2026: 19 file InterestExpenseDebt, 16
    # InterestAndDebtExpense, 54 only cash InterestPaidNet (used as a labelled
    # PROXY), 49 nothing.
    ("us-gaap", "InterestExpenseDebt"),
    ("us-gaap", "InterestAndDebtExpense"),
    ("us-gaap", "InterestPaidNet"),
    ("us-gaap", "InterestPaid"),
    ("us-gaap", "NetIncomeLoss"),
    ("us-gaap", "DepreciationDepletionAndAmortization"),
    ("us-gaap", "DepreciationAmortizationAndAccretionNet"),
    # Added 2026-10-11 from a census of the 104 non-financial equities with no
    # D&A within 460 days (so no EBITDA and no exit-multiple cross-check; MSFT
    # among them): 47 tag DepreciationAndAmortization, 77 Depreciation and 72
    # AmortizationOfIntangibleAssets (summed when there is no total).
    ("us-gaap", "DepreciationAndAmortization"),
    ("us-gaap", "Depreciation"),
    ("us-gaap", "AmortizationOfIntangibleAssets"),
    ("us-gaap", "ShareBasedCompensation"),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
    # Added 2026-10-11: Apple filed CFO under this variant 2014-04..2017-02 (its
    # history had a hole there), and DLTR, DOV, DRI, DD, JCI, NWS, NWSA file it
    # today — 7 of the 12 refused for "no operating cash flow".
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
    # Added 2026-10-11 from a census of the 126 equities refused for missing
    # capex: 82 file PaymentsToAcquireProductiveAssets; the oil-and-gas names
    # file exploration/development concepts.
    ("us-gaap", "PaymentsToAcquireProductiveAssets"),
    ("us-gaap", "PaymentsToAcquireOilAndGasPropertyAndEquipment"),
    ("us-gaap", "PaymentsToExploreAndDevelopOilAndGasProperties"),
    ("us-gaap", "PaymentsToAcquireOilAndGasProperty"),
    # Added 2026-10-11 from a census of the 26 refused for capex; see the
    # capex list in modeling/statements.py for who files which.
    ("us-gaap", "PaymentsToAcquireOtherPropertyPlantAndEquipment"),
    ("us-gaap", "PaymentsToAcquireOtherProductiveAssets"),
    ("us-gaap", "PaymentsForConstructionInProcess"),
    ("us-gaap", "PaymentsForCapitalImprovements"),
    ("us-gaap", "PaymentsToAcquireMachineryAndEquipment"),
    ("us-gaap", "PaymentsForProceedsFromProductiveAssets"),
    # Added 2026-10-13: capitalised software and other intangibles are paid
    # for beside capex, outside PP&E (CMCSA "Cash paid for intangible assets"
    # $2,658M in FY2025 beside $11,750M of capital expenditures).
    ("us-gaap", "PaymentsToAcquireIntangibleAssets"),
    # Added 2026-10-14 (decision 23): VST resells part of the emission
    # allowances it buys ("Proceeds from sales of environmental allowances"
    # $275M beside $1,189M of purchases in FY2025); its capex is the net.
    ("us-gaap", "ProceedsFromSaleOfIntangibleAssets"),
    ("us-gaap", "PaymentsToDevelopSoftware"),
    ("us-gaap", "PaymentsForSoftware"),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
    ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
    ("us-gaap", "MarketableSecuritiesCurrent"),
    # Added 2026-10-09: Apple holds $77.7B here, more than its cash; leaving it
    # out overstated net debt by that much.
    ("us-gaap", "MarketableSecuritiesNoncurrent"),
    ("us-gaap", "LongTermDebt"),
    ("us-gaap", "LongTermDebtNoncurrent"),
    ("us-gaap", "LongTermDebtCurrent"),
    ("us-gaap", "CommercialPaper"),
    # Added 2026-10-11 from a census of the 97 non-financial equities that
    # report interest but tagged no LongTermDebt[Noncurrent] within 400 days
    # (125 of all equities; LDOS's last LongTermDebt was 2015, CMCSA had none):
    # 67 tag LongTermDebtAndCapitalLeaseObligations, 49 its Current, 32 the
    # IncludingCurrentMaturities total, 33 DebtCurrent, 26 ShortTermBorrowings,
    # 19 DebtAndCapitalLeaseObligations, 15 DebtLongtermAndShorttermCombinedAmount.
    ("us-gaap", "LongTermDebtAndCapitalLeaseObligations"),
    ("us-gaap", "LongTermDebtAndCapitalLeaseObligationsCurrent"),
    ("us-gaap", "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities"),
    ("us-gaap", "DebtAndCapitalLeaseObligations"),
    ("us-gaap", "DebtLongtermAndShorttermCombinedAmount"),
    ("us-gaap", "DebtCurrent"),
    ("us-gaap", "ShortTermBorrowings"),
    # Added 2026-10-12 from a census of the 19 refused for interest with no
    # debt balance: AKAM, DDOG, DXCM and ZS tag only convertible notes, CDNS
    # only UnsecuredLongTermDebt, VRSN only SeniorLongTermNotes. See the debt
    # lists in modeling/statements.py for which shape each is.
    ("us-gaap", "ConvertibleLongTermNotesPayable"),
    ("us-gaap", "ConvertibleNotesPayableCurrent"),
    ("us-gaap", "UnsecuredLongTermDebt"),
    ("us-gaap", "SeniorLongTermNotes"),
    # Added 2026-10-13 (decision 17): noncontrolling interests. CFO and
    # EBITDA are consolidated, so the partners' share is subtracted (DVA's
    # clinic partners earned $332M of $1,079M in FY2025). Seven valued filers
    # (ACN, EL, ICE, MPC, SPGI, SRE, WBD) tag only the redeemable and
    # nonredeemable parts; the balances say whether NCI exists at all.
    ("us-gaap", "NetIncomeLossAttributableToNoncontrollingInterest"),
    ("us-gaap", "NetIncomeLossAttributableToRedeemableNoncontrollingInterest"),
    ("us-gaap", "NetIncomeLossAttributableToNonredeemableNoncontrollingInterest"),
    ("us-gaap", "ProfitLoss"),
    ("us-gaap", "MinorityInterest"),
    ("us-gaap", "RedeemableNoncontrollingInterestEquityCarryingAmount"),
    # Added 2026-10-13 (decision 18): preferred stock is a claim ahead of the
    # common (OXY's $8.5B, ALB's $2.2B mandatory convertible), subtracted
    # like debt; equity-method stakes are noted, not added. Temporary equity
    # attributable to the parent is NOT preferred: J's $1,093M was redeemable
    # NCI in PA Consulting.
    ("us-gaap", "PreferredStockValue"),
    ("us-gaap", "PreferredStockValueOutstanding"),
    ("us-gaap", "PreferredStockLiquidationPreferenceValue"),
    ("us-gaap", "EquityMethodInvestments"),
    # Added 2026-10-14 (decision 19): unfunded pensions and retiree benefits,
    # NOTED, never subtracted (contributions are inside CFO; the totals are
    # missing for filers that tag per plan, GE and T). The balance-sheet
    # lines, net of overfunded plans' assets: GEHC $5,453M, 18% of market cap.
    ("us-gaap", "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent"),
    ("us-gaap", "PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent"),
    ("us-gaap", "DefinedBenefitPensionPlanLiabilitiesNoncurrent"),
    ("us-gaap", "OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent"),
    ("us-gaap", "PensionAndOtherPostretirementDefinedBenefitPlansCurrentLiabilities"),
    ("us-gaap", "DefinedBenefitPensionPlanLiabilitiesCurrent"),
    ("us-gaap", "DefinedBenefitPlanAssetsForPlanBenefitsNoncurrent"),
    ("us-gaap", "StockholdersEquity"),
    ("us-gaap", "Assets"),
    ("us-gaap", "Liabilities"),
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
    ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
    ("us-gaap", "EarningsPerShareDiluted"),
    ("dei", "EntityCommonStockSharesOutstanding"),
)


#: Research concepts (decision 40b, 2026-10-16): stored beside the DCF's for
#: the Equity Research Desk's pack (scripts/desk_pack.py) and nothing else —
#: the DCF reads its own concept set (core.valuation_service.ALL_CONCEPTS), so
#: storing these changes no valuation. SEC company facts carry ~560 us-gaap
#: concepts for a large filer (MSFT, 2026-10-16); these are the ones the desk's
#: lenses use. Every name was checked present in at least one of MSFT, JPM,
#: WMT, TRV, CAT and PFE before it was added. Insert-only, like the rest.
RESEARCH_CONCEPTS = (
    # Unit economics: margins and the cost of a revenue dollar
    ("us-gaap", "GrossProfit"),
    ("us-gaap", "CostOfRevenue"),
    ("us-gaap", "CostOfGoodsSold"),
    ("us-gaap", "ResearchAndDevelopmentExpense"),
    ("us-gaap", "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"),
    ("us-gaap", "SellingGeneralAndAdministrativeExpense"),
    ("us-gaap", "GeneralAndAdministrativeExpense"),
    ("us-gaap", "SellingAndMarketingExpense"),
    ("us-gaap", "MarketingAndAdvertisingExpense"),
    ("us-gaap", "OperatingExpenses"),
    ("us-gaap", "CostsAndExpenses"),
    ("us-gaap", "RestructuringCharges"),
    ("us-gaap", "GoodwillImpairmentLoss"),
    ("us-gaap", "ImpairmentOfIntangibleAssetsExcludingGoodwill"),
    ("us-gaap", "AssetImpairmentCharges"),
    ("us-gaap", "OtherNonoperatingIncomeExpense"),
    ("us-gaap", "InvestmentIncomeInterest"),
    ("us-gaap", "EarningsPerShareBasic"),
    ("us-gaap", "EffectiveIncomeTaxRateContinuingOperations"),
    ("us-gaap", "AllocatedShareBasedCompensationExpense"),
    # Balance sheet and working capital
    ("us-gaap", "AssetsCurrent"),
    ("us-gaap", "LiabilitiesCurrent"),
    ("us-gaap", "LiabilitiesAndStockholdersEquity"),
    ("us-gaap", "AccountsReceivableNetCurrent"),
    ("us-gaap", "InventoryNet"),
    ("us-gaap", "AccountsPayableCurrent"),
    ("us-gaap", "PropertyPlantAndEquipmentNet"),
    ("us-gaap", "PropertyPlantAndEquipmentGross"),
    ("us-gaap", "Goodwill"),
    ("us-gaap", "IntangibleAssetsNetExcludingGoodwill"),
    ("us-gaap", "OperatingLeaseLiability"),
    ("us-gaap", "OperatingLeaseRightOfUseAsset"),
    ("us-gaap", "FinanceLeaseLiability"),
    ("us-gaap", "RetainedEarningsAccumulatedDeficit"),
    ("us-gaap", "TreasuryStockValue"),
    ("us-gaap", "CommonStockSharesOutstanding"),
    # Revenue quality: deferred revenue and the contracted backlog
    ("us-gaap", "ContractWithCustomerLiability"),
    ("us-gaap", "ContractWithCustomerLiabilityCurrent"),
    ("us-gaap", "DeferredRevenue"),
    ("us-gaap", "DeferredRevenueCurrent"),
    ("us-gaap", "RevenueRemainingPerformanceObligation"),
    ("us-gaap", "IncreaseDecreaseInContractWithCustomerLiability"),
    # Capital allocation: buybacks, dividends, issuance, acquisitions
    ("us-gaap", "PaymentsForRepurchaseOfCommonStock"),
    ("us-gaap", "PaymentsOfDividends"),
    ("us-gaap", "PaymentsOfDividendsCommonStock"),
    ("us-gaap", "ProceedsFromIssuanceOfCommonStock"),
    ("us-gaap", "ProceedsFromIssuanceOfLongTermDebt"),
    ("us-gaap", "RepaymentsOfLongTermDebt"),
    ("us-gaap", "PaymentsToAcquireBusinessesNetOfCashAcquired"),
    ("us-gaap", "CommonStockDividendsPerShareDeclared"),
    ("us-gaap", "CommonStockDividendsPerShareCashPaid"),
    ("us-gaap", "StockRepurchasedDuringPeriodShares"),
    # Cash flow detail and accrual quality
    ("us-gaap", "NetCashProvidedByUsedInInvestingActivities"),
    ("us-gaap", "NetCashProvidedByUsedInFinancingActivities"),
    ("us-gaap", "IncreaseDecreaseInAccountsReceivable"),
    ("us-gaap", "IncreaseDecreaseInInventories"),
    ("us-gaap", "IncreaseDecreaseInAccountsPayable"),
    ("us-gaap", "IncomeTaxesPaidNet"),
    ("us-gaap", "DeferredIncomeTaxExpenseBenefit"),
    # Banks and insurers
    ("us-gaap", "InterestAndDividendIncomeOperating"),
    ("us-gaap", "InterestIncomeExpenseNet"),
    ("us-gaap", "NoninterestIncome"),
    ("us-gaap", "NoninterestExpense"),
    ("us-gaap", "ProvisionForLoanLeaseAndOtherLosses"),
    ("us-gaap", "ProvisionForLoanAndLeaseLosses"),
    ("us-gaap", "Deposits"),
    ("us-gaap", "LoansAndLeasesReceivableNetReportedAmount"),
    ("us-gaap", "PremiumsEarnedNet"),
    ("us-gaap", "PolicyholderBenefitsAndClaimsIncurredNet"),
)

#: Everything core/fundamentals.py stores.
STORED_CONCEPTS = FUNDAMENTAL_CONCEPTS + RESEARCH_CONCEPTS

# --- DCF (modeling/dcf.py, modeling/valuation.py; tier 3 phase 3) ---
# Decisions 4 and 5 of research/dcf-plan-2026-10-08.md, approved 2026-10-09.
DCF_PROJECTION_YEARS = 5
DCF_TERMINAL_GROWTH = 0.025
#: A constant, documented as the assumption it is; the sensitivity grid shows
#: what it moves.
DCF_EQUITY_RISK_PREMIUM = 0.045
#: Beta: daily total-return returns regressed on SPY over this many sessions
#: (decision 4's beta; still computed and reported).
DCF_BETA_WINDOW_DAYS = 756
DCF_BETA_BENCHMARK = "SPY"
#: Decision 11 (approved 2026-10-11): the beta USED is weekly over 5 years,
#: Blume-adjusted. The phase 6 sweep: 24 names above 3x price against 48 on
#: the daily 3-year beta, whose near-zero values for staples (CPB -0.01, MO
#: -0.07) put the cost of equity at the T-bill rate.
DCF_BETA_METHOD = "weekly_5y_blume"
DCF_BETA_WEEKLY_WINDOW = 260
#: Cost of debt when the filer reports no interest expense (Apple stopped
#: tagging InterestExpense after mid-2023): risk-free + this spread, labelled
#: as an estimate wherever it is used.
DCF_DEBT_SPREAD = 0.01
#: Decision 14 (approved 2026-10-11): the WACC used is at least this, applied
#: and labelled (the CAPM figure is kept beside it); a WACC override on the
#: page replaces it. Measured on the 13 names over 3x price on a low WACC
#: (CAG 4.9%, CPB 4.6%): market cost of debt, a 5.5% ERP or 2% terminal growth
#: each moved value/price 5-30%; only a floor moved the list. A CAPM beta of
#: 0.4 says a stock moved little with SPY, not that its cash flows are nearly
#: riskless — the floor states that judgement openly.
DCF_WACC_FLOOR = 0.07
#: Decision 15 (approved 2026-10-11): the default first-year growth is the
#: LOWER of the 3- and 5-year revenue CAGR, capped here. Revenue CAGR counts
#: acquisitions and recoveries as growth (LVS 46.9% after reopening, AMCR
#: 19.9% with Berry, FANG 16.2% with Endeavor); the input stays editable.
DCF_DEFAULT_GROWTH_CAP = 0.10
#: Stock-based compensation is added back inside operating cash flow; True
#: subtracts it again, treating it as the real cost it is. Shown as its own
#: line either way. Apple FY2025: $12.9B of $98.8B FCF.
DCF_SUBTRACT_SBC = True
#: Decision 17 (approved 2026-10-13): noncontrolling interests' share of
#: income is subtracted from base FCF. It is a proxy for the partners' cash,
#: and it breaks when consolidated FCF is small beside earnings: FCX's is
#: 99% of base FCF (TTM to 2026-06-30; capex at PT-FI). Above this share of
#: a positive base FCF the valuation is refused rather than printed; a
#: negative base FCF (INTC) only falls further.
DCF_NCI_MAX_FCF_SHARE = 0.5
#: An NCI balance with no NCI income on file refuses the valuation (missing
#: is never zero) unless the balance is below this share of market cap: ITW's
#: $1M and VLTO's $1M of NCI are immaterial and are only noted.
DCF_NCI_IMMATERIAL_BALANCE_SHARE = 0.01
#: Decision 18: an equity-method stake this large a share of market cap is
#: named in the notes (it is valued only through dividends inside CFO).
DCF_EQUITY_METHOD_NOTE_SHARE = 0.05
#: Decision 19: an unfunded pension and retiree-benefit balance this large a
#: share of market cap is named in the notes; it is never subtracted.
DCF_PENSION_NOTE_SHARE = 0.05
DCF_EXIT_MULTIPLE = 15.0
#: Sensitivity grid half-widths around the base case.
DCF_GRID_WACC_STEP = 0.005
DCF_GRID_GROWTH_STEP = 0.005
DCF_GRID_POINTS = 5
#: Reverse DCF searches first-year growth in this bracket.
DCF_REVERSE_GROWTH_BOUNDS = (-0.5, 1.0)
#: Above this share of EV the valuation is mostly the terminal assumption.
DCF_TERMINAL_SHARE_WARN = 0.8
#: Sectors refused, with the reason shown. Financials: decision 2's reason.
#: Real Estate: decision 12 (approved 2026-10-11) — REIT capex is property
#: acquisition and development (13 file PaymentsToAcquireRealEstate, 12
#: PaymentsToDevelopRealEstateAssets) and REITs are valued on FFO/AFFO; refused
#: until there is such a model.
DCF_REFUSED_SECTORS = {
    "Financials": "free cash flow is not meaningful for banks and insurers",
    "Real Estate": "REIT capital spending is property acquisition and development; "
    "REITs need an FFO/AFFO model, not a free-cash-flow DCF",
}

#: Symbols refused whatever their sector, with why. Decision 13 (approved
#: 2026-10-11): their operating cash flow or debt carries a financial book
#: that free cash flow cannot separate — the reason Financials are refused.
#: Each recurred above 3x price in the phase 6 sweep.
_MANAGED_CARE = "managed care: insurance float runs through operating cash flow"
_CAPTIVE_FINANCE = (
    "captive finance: finance receivables and the debt funding them distort FCF and net debt"
)
DCF_REFUSED_SYMBOLS = {
    "CNC": _MANAGED_CARE,
    "ELV": _MANAGED_CARE,
    "UNH": _MANAGED_CARE,
    "CVS": _MANAGED_CARE,
    "HUM": _MANAGED_CARE,
    "MOH": _MANAGED_CARE,
    "F": _CAPTIVE_FINANCE,
    "GM": _CAPTIVE_FINANCE,
    "KMX": _CAPTIVE_FINANCE,
    # Added 2026-10-11 (approved by Danny): John Deere Financial and PACCAR
    # Financial. Both were refused anyway for "interest but no debt balance"
    # (DE tags $17.1B DebtCurrent and $6.1B SecuredDebt, no standard total);
    # this is the reason that would remain once the debt tags were read.
    "DE": _CAPTIVE_FINANCE,
    "PCAR": _CAPTIVE_FINANCE,
    # Added 2026-10-13 (approved by Danny). Missing is never zero: its FY2025
    # cash-flow statement shows "Additions to instruments" $300.5M beside the
    # $224.5M of "other property, plant and equipment" the DCF can read, and
    # the instruments line is tagged only in ZBH's own extension concept,
    # which SEC companyfacts never serves.
    "ZBH": (
        "capex incomplete: $300.5M of instruments (FY2025) is filed only in the "
        "company's own XBRL concept, which SEC company facts does not serve"
    ),
}

#: Symbols in a refused SECTOR that are valued anyway, with why. Decision 16
#: (approved 2026-10-12, option B): fee businesses whose free cash flow is an
#: ordinary company's. Measured with the sector gate lifted (2026-10-12):
#: operating cash flow a stable 0.17-0.59 of revenue over 6 years, values
#: 0.19-3.75x price; banks and insurers alongside ran 3-32x revenue. A list by
#: name, not by GICS sub-industry, on purpose: a stale list fails as a visible
#: refusal, a misread classification as a wrong number, and only these names
#: were measured. Add a new index member only after the same check.
_EXCHANGE_DATA = "exchange or data provider: fee revenue, no balance-sheet lending"
_BROKER = "insurance broker: commissions and fees, no underwriting float"
_PROCESSOR = "payment processor: transaction fees, cash conversion stable over 6 years"
_RE_SERVICES = "real estate services: fees, not a REIT's property capital spending"
DCF_ALLOWED_SYMBOLS = {
    **dict.fromkeys(("SPGI", "MCO", "MSCI", "FDS", "ICE", "CME", "NDAQ", "CBOE"), _EXCHANGE_DATA),
    **dict.fromkeys(("AJG", "AON", "BRO", "MRSH", "WTW"), _BROKER),
    **dict.fromkeys(("MA", "GPN", "CPAY", "JKHY", "PYPL", "FIS", "FISV"), _PROCESSOR),
    **dict.fromkeys(("CBRE", "CSGP"), _RE_SERVICES),
}


#: Per-filer line concepts, {symbol: {line item: concepts}}: the line read as
#: the SUM of these concepts per period instead of the first of the item's
#: list (modeling.statements, line_concepts). A period missing a part is
#: dropped, never zero. Every entry was read off the filer's latest 10-K. No
#: rule picks the right line, so add a filer only after reading its statement.
#:
#: Capex (2026-10-12): 27 of 381 valued companies tag capex under more than
#: one concept, 11 materially, and only AEP, LNT and EOG were wrong. The
#: others' second concept is an acquisition (CNP, PWR, MLM, FANG), a component
#: already inside the total (DAL: flight $3,521M + ground $978M = the $4,499M
#: it tags), or not on the statement at all (MCD, MCHP, WMB's accrual figure).
#: "The largest" would take FANG's acquisitions; "the sum" would double DAL.
#:
#: Revenue (2026-10-13): it feeds only the growth rate. 26 valued filers tag
#: the latest fiscal year under two revenue concepts with different values;
#: these 7 report Revenues as the income statement's total and the contract
#: concept first in the list as a part of it (commodity derivatives, leases
#: and other non-contract revenue are outside ASC 606). The reverse also
#: happens, so Revenues cannot simply go first: NTAP's total is the contract
#: figure ($6,925M) and its Revenues fact ($6,237M) is not on the statement.
_TOTAL_REVENUES = ("Revenues",)
_INTANGIBLES = ("PaymentsToAcquireIntangibleAssets",)
LINE_CONCEPTS_BY_SYMBOL = {
    # "Construction Expenditures" $8,453M. The concept first in the list,
    # PaymentsToAcquireProductiveAssets ($3,453M), is "Acquisitions of
    # Generation Facilities": buying power plants, not capex.
    "AEP": {"capex": ("PaymentsForConstructionInProcess",)},
    # "Utility construction and acquisition expenditures" $2,277M plus "Other
    # construction and acquisition expenditures" $206M.
    "LNT": {
        "capex": (
            "PaymentsToAcquireOtherPropertyPlantAndEquipment",
            "PaymentsToAcquireOtherProductiveAssets",
        )
    },
    # "Additions to Oil and Gas Properties" $6,115M plus "Additions to Other
    # Property, Plant and Equipment" $479M.
    "EOG": {
        "capex": (
            "PaymentsToAcquireOilAndGasPropertyAndEquipment",
            "PaymentsToAcquireOtherPropertyPlantAndEquipment",
        )
    },
    # "REVENUES" $54,774M; the IncludingAssessedTax fact ($889M) is a part.
    # Growth had been -6.85% (that part shrinking) for revenue that was flat.
    "CHTR": {"revenue": _TOTAL_REVENUES},
    # "Revenues" $80,269M against $24,956M of contract revenue: most of a
    # grain merchant's sales are commodity contracts outside ASC 606.
    "ADM": {"revenue": _TOTAL_REVENUES},
    # "Net sales" $70,329M against $16,944M of contract revenue, likewise.
    "BG": {"revenue": _TOTAL_REVENUES},
    # "Revenues" $16,937M against $15,202M of contract revenue.
    "KMI": {"revenue": _TOTAL_REVENUES},
    # "Total revenues" $13,702M against $12,418M.
    "SRE": {"revenue": _TOTAL_REVENUES},
    # "Total revenues" $17,188M against "Oil, gas and NGL sales" $16,786M.
    "DVN": {"revenue": _TOTAL_REVENUES},
    # "Revenues" $11,950M, net of commodity derivative losses; the contract
    # figure ($14,899M) is larger than the total.
    "WMB": {"revenue": _TOTAL_REVENUES},
    # Intangible capex (2026-10-13), PaymentsToAcquireIntangibleAssets where
    # the statement shows it is recurring capital spending: CMCSA "Cash paid
    # for intangible assets" $2,658M (mostly software) beside $11,750M of
    # capital expenditures; ADP "Additions to intangibles" $468.5M beside
    # $196.6M; SLB "Exploration data capitalized" $252M (its seismic data
    # library) beside $1,694M. NOT mapped: TMUS and VZ spectrum licences
    # (lumpy: VZ $450M in 2025, $5,796M in 2023), SNPS's $16,681M (the Ansys
    # acquisition), CPB's $56M (not on its statement).
    # VST (decision 23): "Purchases of environmental allowances" $1,189M less
    # "Proceeds from sales of environmental allowances" $275M (FY2025, in
    # investing). Allowances are retired against the plants' emissions every
    # year, an operating cost the statement puts below CFO; resales net.
    "CMCSA": {"intangible_capex": _INTANGIBLES},
    "ADP": {"intangible_capex": _INTANGIBLES},
    "SLB": {"intangible_capex": _INTANGIBLES},
    "VST": {
        "intangible_capex": (
            "PaymentsToAcquireIntangibleAssets",
            "-ProceedsFromSaleOfIntangibleAssets",
        )
    },
}


#: scripts/check_line_concepts.py (06:00 job): another concept for a line's
#: newest fiscal year is flagged when valuing on it moves value/price by at
#: least this much, absolute AND relative. The 2026-10-13 census used these.
LINE_CHECK_MIN_MOVE = 0.05
LINE_CHECK_MIN_REL = 0.05

#: An alternative under this share of the chosen figure is a component, not
#: the line, and is not flagged (core/line_concept_check.alternatives).
LINE_CHECK_MIN_SHARE = 0.10

#: (symbol, line item, alternative concept) read off the filing and found to
#: be right as chosen, with what the statement shows. Clears that flag only;
#: a mapped line needs no entry (its LINE_CONCEPTS_BY_SYMBOL entry is the
#: review). Read 2026-10-12/13 from each filer's latest 10-K.
_CONTINUING_CFO = (
    "capex is in continuing investing and discontinued operations are separate "
    "lines, so continuing CFO is the figure to pair with it"
)
LINE_CONCEPTS_REVIEWED = {
    ("CARR", "cfo", "NetCashProvidedByUsedInOperatingActivities"): _CONTINUING_CFO,
    ("OXY", "cfo", "NetCashProvidedByUsedInOperatingActivities"): _CONTINUING_CFO,
    ("DAL", "capex", "PaymentsToAcquireOtherProductiveAssets"): (
        "ground equipment $978M is a component: flight $3,521M + ground = the $4,499M chosen"
    ),
    ("FANG", "capex", "PaymentsToAcquireOilAndGasProperty"): (
        "$5,938M is acquisitions of oil and gas properties, not capex"
    ),
    ("MCD", "capex", "PaymentsToAcquireOtherProductiveAssets"): (
        "not a line of the cash-flow statement; capital expenditures is the $3,365M chosen"
    ),
    ("PAYX", "capex", "PaymentsToAcquireOtherProductiveAssets"): (
        "not a line of the cash-flow statement; the $235M chosen is"
    ),
    ("HII", "capex", "PaymentsToAcquireProductiveAssets"): (
        '"Capital expenditure additions" $402M (chosen); $396M nets $6M of grant proceeds'
    ),
    ("HPE", "interest_expense", "InterestExpenseDebt"): (
        "$1,098M is interest on all borrowings, matching the total debt netted; $598M is a part"
    ),
}


# --- Spin-offs (decision 58, research/spinoff-basis-2026-10-23.md) ---
#
# Yahoo serves a spin-off as a "split" whose ratio is the price adjustment
# (HON 2025-10-30 1.061 for Solstice). That is right for adjusting returns
# and wrong for a share count: a spin leaves the count alone, and the DCF had
# multiplied counts filed before it by the ratio (CTVA on 2026-10-23: 667M
# shares x 6.665 = 4,447M, $3.07 a share against a $12.86 price).
#
# Every equity split row whose ratio is not a multiple of 1/4 either way
# (`odd_split_ratio`), classified 2026-10-23: (symbol, ex-date) -> (kind,
# the ratio share counts move by, what it was). "cover shares" is the
# cover-page count just after over just before, from the stored facts: about
# 1.0 for a spin, the reverse split's ratio when one came with it. Rows with
# no count either side are named from the separation alone. Kinds:
#   spin-off  the share ratio is 1.0, or the reverse split done with it;
#             the DCF refuses across it (valuation.build_inputs)
#   split     a real share change with an odd ratio: Yahoo's ratio is kept
#   payment / other  not a share change: ratio 1.0, no refusal
# A new odd row not listed here is flagged by scripts/check_line_concepts.py
# (SPLIT?), and until it is read the DCF treats it as a split, as before.
SPLIT_ROWS_REVIEWED = {
    ("VZ", "2010-07-02"): ("spin-off", 1.0, "Frontier (wireline lines); cover shares 1.000x"),
    ("MSI", "2011-01-04"): (
        "spin-off",
        1 / 7,
        "Motorola Mobility, with a 1-for-7 reverse split; cover shares 0.143x",
    ),
    ("NOC", "2011-03-31"): ("spin-off", 1.0, "Huntington Ingalls; cover shares 1.005x"),
    ("MAR", "2011-11-22"): ("spin-off", 1.0, "Marriott Vacations Worldwide; cover shares 0.996x"),
    ("WMB", "2012-01-03"): ("spin-off", 1.0, "WPX Energy; cover shares 1.005x"),
    ("COP", "2012-05-01"): ("spin-off", 1.0, "Phillips 66; cover shares 0.960x"),
    ("JCI", "2012-10-01"): (
        "spin-off",
        1.0,
        "ADT, and Tyco Flow Control to Pentair (Tyco is JCI's filer); cover shares 1.013x",
    ),
    ("ABT", "2013-01-02"): ("spin-off", 1.0, "AbbVie; cover shares 0.994x"),
    ("VLO", "2013-05-02"): ("spin-off", 1.0, "CST Brands; cover shares 0.994x"),
    ("LDOS", "2013-09-30"): (
        "spin-off",
        0.25,
        "new SAIC, with a 1-for-4 reverse split; cover shares 0.250x",
    ),
    ("OKE", "2014-02-03"): ("spin-off", 1.0, "ONE Gas; cover shares 1.007x"),
    ("DOV", "2014-03-03"): ("spin-off", 1.0, "Knowles; cover shares 0.979x"),
    ("SPG", "2014-05-29"): ("spin-off", 1.0, "Washington Prime; no cover count stored either side"),
    ("IP", "2014-07-02"): ("spin-off", 1.0, "xpedx (Veritiv); no cover count stored either side"),
    ("IRM", "2014-09-26"): (
        "split",
        1.0820,
        "REIT-conversion distribution paid in stock: cover shares 193.7M -> 210.1M (1.085x)",
    ),
    ("ADP", "2014-10-01"): ("spin-off", 1.0, "CDK Global; cover shares 1.002x"),
    ("A", "2014-11-03"): ("spin-off", 1.0, "Keysight; cover shares 1.005x"),
    ("KMB", "2014-11-03"): ("spin-off", 1.0, "Halyard Health; cover shares 0.981x"),
    ("OXY", "2014-12-01"): ("spin-off", 1.0, "California Resources; cover shares 0.994x"),
    ("GOOG", "2015-04-27"): ("payment", 1.0, "Class C adjustment payment; not a share change"),
    ("PPL", "2015-06-02"): ("spin-off", 1.0, "Talen Energy; cover shares 1.003x"),
    ("MAS", "2015-07-01"): ("spin-off", 1.0, "TopBuild; cover shares 0.978x"),
    ("BAX", "2015-07-01"): ("spin-off", 1.0, "Baxalta; cover shares 1.002x"),
    ("NI", "2015-07-02"): ("spin-off", 1.0, "Columbia Pipeline; cover shares 1.002x"),
    ("EBAY", "2015-07-20"): ("spin-off", 1.0, "PayPal; cover shares 0.986x"),
    ("VTR", "2015-08-18"): ("spin-off", 1.0, "Care Capital Properties; cover shares 1.001x"),
    ("HPQ", "2015-11-02"): ("spin-off", 1.0, "Hewlett Packard Enterprise; cover shares 0.996x"),
    ("DRI", "2015-11-10"): ("spin-off", 1.0, "Four Corners Property Trust; cover shares 1.001x"),
    ("WTW", "2016-01-05"): (
        "split",
        0.3775,
        "Willis 2.649-to-1 share consolidation at the Towers Watson merger (cover shares 0.767x, merger issuance inside)",
    ),
    ("DHR", "2016-07-05"): ("spin-off", 1.0, "Fortive; cover shares 1.003x"),
    ("JCI", "2016-09-06"): (
        "split",
        0.9550,
        "Tyco 0.955-for-1 consolidation at the JCI merger (cover shares 2.198x, merger issuance inside)",
    ),
    ("HON", "2016-10-03"): ("spin-off", 1.0, "AdvanSix; cover shares 0.999x"),
    ("APD", "2016-10-03"): ("spin-off", 1.0, "Versum Materials; cover shares 1.004x"),
    ("YUM", "2016-11-01"): ("spin-off", 1.0, "Yum China; cover shares 0.000x"),
    ("DOC", "2016-11-01"): ("spin-off", 1.0, "Quality Care Properties; cover shares 1.000x"),
    ("CAG", "2016-11-10"): ("spin-off", 1.0, "Lamb Weston; cover shares 0.994x"),
    ("HLT", "2017-01-04"): (
        "spin-off",
        1 / 3,
        "Park Hotels and Hilton Grand Vacations, with a 1-for-3 reverse split; cover shares 0.333x",
    ),
    ("MET", "2017-08-07"): ("spin-off", 1.0, "Brighthouse Financial; cover shares 0.990x"),
    ("HPE", "2017-09-01"): ("spin-off", 1.0, "Enterprise Services to DXC; cover shares 0.984x"),
    ("LEN", "2017-11-09"): (
        "split",
        1.0170,
        "Class B stock dividend, 1 per 50 Class A; no cover count stored either side",
    ),
    ("APTV", "2017-12-05"): ("spin-off", 1.0, "Delphi Technologies; cover shares 1.000x"),
    ("PNR", "2018-05-01"): ("spin-off", 1.0, "nVent; cover shares 0.983x"),
    ("DOV", "2018-05-09"): ("spin-off", 1.0, "Apergy; cover shares 0.955x"),
    ("HON", "2018-10-01"): ("spin-off", 1.0, "Garrett Motion; cover shares 0.985x"),
    ("HON", "2018-10-29"): ("spin-off", 1.0, "Resideo; cover shares 0.985x"),
    ("EQT", "2018-11-13"): ("spin-off", 1.0, "Equitrans Midstream; cover shares 1.001x"),
    ("DELL", "2018-12-28"): (
        "other",
        1.0,
        "Class V exchange and Class C listing; no cover count stored either side",
    ),
    ("HSIC", "2019-02-08"): ("spin-off", 1.0, "Covetrus; cover shares 0.993x"),
    ("GE", "2019-02-26"): ("spin-off", 1.0, "GE Transportation to Wabtec; cover shares 1.002x"),
    ("DD", "2019-04-02"): ("spin-off", 1.0, "Dow; cover shares 0.996x"),
    ("DD", "2019-06-03"): (
        "spin-off",
        1 / 3,
        "Corteva, with a 1-for-3 reverse split; cover shares 0.332x",
    ),
    ("ECHO", "2019-09-11"): (
        "spin-off",
        1.0,
        "BSS business to DISH; no cover count stored either side",
    ),
    ("TT", "2020-03-02"): (
        "spin-off",
        1.0,
        "Industrial segment to Gardner Denver (Ingersoll Rand); cover shares 1.003x",
    ),
    ("RTX", "2020-04-03"): (
        "spin-off",
        1.0,
        "Carrier and Otis (shares then issued in the Raytheon merger); cover shares 1.764x",
    ),
    ("FTV", "2020-10-09"): ("spin-off", 1.0, "Vontier; cover shares 1.000x"),
    ("PFE", "2020-11-17"): ("spin-off", 1.0, "Upjohn to Viatris; cover shares 1.003x"),
    ("MRK", "2021-06-03"): ("spin-off", 1.0, "Organon; cover shares 1.000x"),
    ("DTE", "2021-07-01"): ("spin-off", 1.0, "DT Midstream; cover shares 1.000x"),
    ("IP", "2021-10-01"): ("spin-off", 1.0, "Sylvamo; no cover count stored either side"),
    ("DELL", "2021-11-02"): ("spin-off", 1.0, "VMware; no cover count stored either side"),
    ("IBM", "2021-11-04"): ("spin-off", 1.0, "Kyndryl; cover shares 1.003x"),
    ("O", "2021-11-15"): (
        "spin-off",
        1.0,
        "Orion Office REIT (shares then issued in the VEREIT merger); cover shares 1.463x",
    ),
    ("EXC", "2022-02-02"): ("spin-off", 1.0, "Constellation Energy; cover shares 1.000x"),
    ("ZBH", "2022-03-01"): ("spin-off", 1.0, "ZimVie; cover shares 1.002x"),
    ("BDX", "2022-04-01"): ("spin-off", 1.0, "Embecta; cover shares 1.000x"),
    ("T", "2022-04-11"): (
        "spin-off",
        1.0,
        "WarnerMedia to Warner Bros. Discovery; cover shares 1.002x",
    ),
    ("GE", "2023-01-04"): ("spin-off", 1.0, "GE HealthCare; cover shares 0.997x"),
    ("LH", "2023-07-03"): ("spin-off", 1.0, "Fortrea; cover shares 1.000x"),
    ("DHR", "2023-10-02"): ("spin-off", 1.0, "Veralto; cover shares 1.001x"),
    ("FLEX", "2024-01-03"): ("spin-off", 1.0, "Nextracker; no cover count stored either side"),
    ("MMM", "2024-04-01"): ("spin-off", 1.0, "Solventum; cover shares 0.993x"),
    ("GE", "2024-04-02"): ("spin-off", 1.0, "GE Vernova; cover shares 0.991x"),
    ("ILMN", "2024-06-25"): ("spin-off", 1.0, "GRAIL; no cover count stored either side"),
    ("J", "2024-09-30"): ("spin-off", 1.0, "Amentum; cover shares 0.998x"),
    ("LEN", "2025-01-21"): (
        "spin-off",
        1.0,
        "Millrose Properties; no cover count stored either side",
    ),
    ("WDC", "2025-02-24"): ("spin-off", 1.0, "Sandisk; cover shares 1.003x"),
    ("J", "2025-05-16"): (
        "other",
        1.0,
        "unidentified; cover shares 0.995x across it, so not a share change",
    ),
    ("FTV", "2025-06-30"): ("spin-off", 1.0, "Ralliant; cover shares 0.995x"),
    ("HON", "2025-10-30"): ("spin-off", 1.0, "Solstice Advanced Materials; cover shares 1.001x"),
    ("DD", "2025-11-03"): ("spin-off", 1.0, "Qnity Electronics; cover shares 1.001x"),
    ("CMCSA", "2026-01-05"): ("spin-off", 1.0, "Versant Media; no cover count stored either side"),
    ("BDX", "2026-02-10"): (
        "spin-off",
        1.0,
        "Biosciences and Diagnostics to Waters; cover shares 0.968x",
    ),
    ("FDX", "2026-06-01"): ("spin-off", 1.0, "FedEx Freight; cover shares 0.992x"),
    ("HON", "2026-06-29"): (
        "spin-off",
        0.5,
        "Honeywell Aerospace (HONA), with a 1-for-2 reverse split; cover shares 0.500x",
    ),
    ("SPGI", "2026-07-01"): ("spin-off", 1.0, "Mobility; no cover count stored either side"),
    ("AVB", "2026-08-17"): (
        "other",
        1.0,
        "after AVB's delisting (Form 25-NSE); legacy asset, REIT, never valued; not read",
    ),
    ("CTVA", "2026-10-01"): (
        "spin-off",
        1.0,
        "the Corteva separation; no cover count stored either side",
    ),
}


def odd_split_ratio(ratio: float) -> bool:
    """Neither the ratio nor its inverse is a multiple of 1/4: not a 2:1, 3:2, 5:4 or 1:10."""
    return all(abs(r * 4 - round(r * 4)) > 0.01 for r in (ratio, 1 / ratio))


#: A spin's ex-date to the first periodic filing whose period ends at least
#: this many days after it: before then the price is the company after the
#: spin and every statement the company before it. The days because filers
#: date quarters nominally: HON's Q2 10-Q is dated 2026-06-30, but the
#: 2026-06-29 distribution fell in its third quarter (debt $34.0B at 06-30,
#: $16.6B at 09-30).
SPINOFF_FILING_MIN_DAYS = 7

#: A trailing year bridges two bases when the prior-year YTD it subtracts
#: carries more discontinued operating cash than the fiscal year it is
#: subtracted from, by this share of that year's total CFO: a business moved
#: into discontinued operations after the 10-K (HON 2026-10-23: 9M 2025
#: $2,594M against FY2025 $333M, 35%). Measured 2026-10-23 on 501 filers at
#: each filing since 2012: 98 points above 5% (50 filers; the largest are
#: known separations: BAX 2015 Baxalta, EBAY 2015 PayPal, MRK 2021 Organon,
#: FIS 2024 Worldpay); HON's Q2 2026 (Solstice's 6M $474M vs FY $333M, 2.2%,
#: one basis) is below it. A filer that tags only total CFO re-presents
#: with no signal here (CMCSA, FDX): build_inputs refuses those by date (58b).
DCF_DISCONTINUED_BREAK_SHARE = 0.05


def dcf_sector_refusal(symbol: str | None, sector: str | None) -> str | None:
    """Why `symbol`'s sector refuses a DCF, or None (not refused, or allowed by name)."""
    if sector not in DCF_REFUSED_SECTORS:
        return None
    if symbol is not None and symbol.upper() in DCF_ALLOWED_SYMBOLS:
        return None
    return DCF_REFUSED_SECTORS[sector]


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
#:   GRAM-USD  Toncoin was RENAMED. CoinGecko id `the-open-network` now
#:             carries the symbol GRAM ("Gram (prev. Toncoin)", rank 30), so
#:             a lookup keyed on "TON" found nothing however deep it paged.
#:             A crypto ticker rename, the same shape as BK->BNY: the asset
#:             row was re-keyed TON-USD -> GRAM-USD on 2026-10-19 (decision 46).
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
    # --- Settled 2026-10-14 (Danny). The same coin across a rename: MATIC
    # migrated to POL in September 2024, and both names say so. CoinGecko
    # "POL (ex-MATIC)" vs Yahoo POL28321-USD "Polygon (prev. MATIC)", price
    # gap 1.001x. Mapped since 2026-10-04 and blocked as SUSPECT until now.
    "POL-USD": "match",
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
#:              (Re-keyed GRAM-USD 2026-10-19 and priced from Yahoo's GRAM-USD,
#:              floored at 2026-06-12: before that the key served another
#:              token at ~$0.008, 786 of 916 bars outside Toncoin's range.)
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
    # --- Added 2026-10-19 (decision 46). Found by searching Yahoo for each
    # coin's NAME; bare tickers AERO-USD, M-USD and WLFI-USD are other tokens,
    # and bare NIGHT-USD is "Midnight (midnight.vip)", a lookalike within 0.4%
    # of the real price that the name matcher passed. Every history checked
    # against CoinGecko's all-time range before a bar was stored.
    "AERO-USD": "AERO29270-USD",
    "BTW-USD": "BTW39158-USD",
    "CC-USD": "CC37263-USD",
    "LIT-USD": "LIT39125-USD",
    "M-USD": "M35491-USD",
    "MORPHO-USD": "MORPHO34104-USD",
    "NIGHT-USD": "NIGHT39064-USD",
    "VVV-USD": "VVV35509-USD",
    "WLFI-USD": "WLFI33251-USD",
    "ZRO-USD": "ZRO26997-USD",
    # --- Added 2026-10-22 (index joiner flagged 2026-10-21). Bare PEARL-USD
    # is empty on Yahoo; Pearl (CoinGecko pearl-2) is "Pearl
    # (pearlresearch.ai)", from 2026-08-20, inside CoinGecko's range.
    "PEARL-USD": "PRL41876-USD",
}

#: Registered assets with NO usable price source, and why: the "deliberately
#: not mapped" list above as data (2026-10-19). They stay registered because
#: the daily snapshot really records them as index members. Deleting them
#: would only have tomorrow's snapshot register them again, and
#: point-in-time membership would lose real members. The freshness surface
#: lists them apart from the stale ones: counted as stale they kept the
#: header badge red every day for something nobody can act on, the noise that
#: hides a real stoppage. Only while they have no fresh bar: a coin that
#: gains a source shows up as an ordinary asset again, and its line here
#: should go.
UNPRICED_ASSETS = {
    "PEPE-USD": (
        "Yahoo's PEPE24478-USD is the right coin but rounds to six decimals: 27 distinct "
        "closes in 1,254 bars"
    ),
    "BSC-USD": "No Yahoo ticker found for Binance Bridged USDT (BNB Smart Chain)",
    "BUIDL-USD": (
        "No Yahoo ticker found for BlackRock BUIDL; the bare key is DFOhub (wrong_asset)"
    ),
}

#: top_100_crypto members deliberately NOT registered, and why (decision 46,
#: research/crypto-universe-pegged-2026-10-19.md). The 06:00 new-member step
#: reports these as known and FLAGS any other unregistered member: a coin
#: that needs classifying and an identity audit. Pegged assets (CoinGecko
#: "Stablecoins", "Tokenized Assets") are not crypto risk; the rest have no
#: usable Yahoo series.
CRYPTO_MEMBERS_NOT_REGISTERED = {
    **{
        s: "stablecoin"
        for s in (
            "BFUSD-USD",
            "GHO-USD",
            "OUSD-USD",
            "PYUSD-USD",
            "RLUSD-USD",
            "U-USD",
            "USDD-USD",
            "USDF-USD",
            "USDG-USD",
            "USDGO-USD",
        )
    },
    **{
        s: "tokenized"
        for s in (
            "BCAP-USD",
            "EURSAFO-USD",
            "EUTBL-USD",
            "FIGR_HELOC-USD",
            "PAXG-USD",
            "USDY-USD",
            "USTB-USD",
            "USYC-USD",
            "XAUT-USD",
        )
    },
    "HTX-USD": "unpriceable: Yahoo's HTX-USD has 5 distinct closes in 1,000 bars",
    "ASTER-USD": "unpriceable: no Yahoo ticker found",
    "PRL-USD": "unpriceable: no Yahoo ticker found (bare PRL-USD is The Parallel)",
    "RAIN-USD": "unpriceable: no Yahoo ticker found",
    "STABLE-USD": "unpriceable: no Yahoo ticker found",
}

CRYPTO_ID_OVERRIDES = {
    "GRAM-USD": "the-open-network",
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
# hardcoded 0.02. Every reader uses it now; data_enricher.py, the last 0.02,
# was deleted 2026-10-12 with the SQLite-era pipelines that alone called it.
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

# --- Backtest lot sizes ---
# The smallest quantity a backtest may trade, by asset class. Equities and
# ETFs trade whole shares. Crypto trades to 1e-8 (one satoshi; exchanges'
# real minimums vary by coin, but none is near a whole unit). Until 2026-10-12
# every backtest traded whole units, so once BTC passed the starting cash a
# BTC backtest could not buy at all and reported a flat line. A class with no
# entry is refused, not defaulted: an option or future traded in whole units
# would be the same silent error.
BACKTEST_LOT_SIZES = {"equity": 1.0, "etf": 1.0, "crypto": 1e-8}


def lot_size_for(asset_class: str) -> float:
    """The backtest lot size for `asset_class`; ValueError if it has none."""
    try:
        return BACKTEST_LOT_SIZES[asset_class]
    except KeyError:
        raise ValueError(f"No backtest lot size for asset class {asset_class!r}.") from None


# --- Paper trader (decision 50, research/paper-trader-2026-10-20.md) ---
# A basket symbol with no bar for a session holds the trader (sessions are
# traded in order); this many held sessions are flagged by the 06:00 job.
PAPER_TRADER_HELD_SESSIONS_FLAGGED = 3


# --- Benchmark and regime report (decision 53, research/benchmark-regime-report-2026-10-20.md) ---
#: The benchmark a backtest is regressed on unless the request names one: a
#: run whose assets are all of one class listed here takes that class's,
#: anything else (equities, ETFs, a basket mixing crypto with them) the default.
BENCHMARK_DEFAULT = "SPY"
BENCHMARK_BY_ASSET_CLASS = {"crypto": "BTC-USD"}
#: Calendar days of benchmark bars read before a run's start, so its first
#: days can be ranked (252 earlier volatility readings plus the window).
BENCHMARK_HISTORY_DAYS = 730
#: Bars in the trailing volatility a day's regime is ranked on.
BENCHMARK_REGIME_WINDOW = 21
#: Earlier readings a day is ranked against before it gets a regime.
BENCHMARK_REGIME_MIN_HISTORY = 252
#: Bars in the rolling Sharpe.
BENCHMARK_ROLLING_SHARPE_WINDOW = 252
#: Newey-West lags for alpha's standard error.
BENCHMARK_NEWEY_WEST_LAGS = 5
#: Fewer common bars than this and there is no regression.
BENCHMARK_MIN_BARS = 30


# --- Short positions (decision 48, research/short-engine-2026-10-19.md) ---
# Facts about a broker the repo cannot verify, so unset (None) until Danny sets
# them: a portfolio backtest whose strategy shorts is 422 unless each is set
# here or passed in the request (core/ledger.ShortTerms). Reference points,
# not defaults: Reg T initial margin 50% (max gross 2), FINRA maintenance on a
# short at least 30%, borrow on easy-to-borrow ETFs usually well under 1%.
SHORT_BORROW_RATE = None  # annual, on the shorts' value
SHORT_MAX_GROSS = None  # gross exposure cap, as a multiple of equity
SHORT_MAINTENANCE = None  # equity / gross exposure below which positions are cut
#: Asset classes a backtest may short. Crypto is not: borrow and margin on it
#: differ by venue, and nothing here models them.
SHORTABLE_ASSET_CLASSES = frozenset({"equity", "etf"})


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

#: Bars a year for annualising backtest metrics (volatility, Sharpe, Sortino),
#: the portfolio optimizer and alpha — decided by the series' own calendar
#: (decision 38, research/crypto-strategies-2026-10-15.md). A series with
#: weekend bars (crypto, or a basket holding crypto) has 365 a year; one
#: without has 252. Until 2026-10-15 every series used 252, so every crypto
#: Sharpe was sqrt(365/252) = 1.2035x too high. A SHARE of weekend bars, not
#: "any", so one stray weekend print cannot flip an equity series: equities
#: had 0 of 2,073,293 bars on a weekend, crypto 2 in 7.
TRADING_DAYS_PER_YEAR = 252
CALENDAR_DAYS_PER_YEAR = 365
CALENDAR_DAYS_WEEKEND_SHARE = 0.10

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

# --- SVI fits and arbitrage checks (pricing/svi.py, decision 56) ---
# research/svi-surface-2026-10-20.md. None of these is in the surface's
# fingerprint: an SVI change never recomputes a cached surface.
#: An expiry is fitted only with this many smile points (five parameters)...
SVI_MIN_POINTS = 8
#: ...and at least this many calendar days to run.
SVI_MIN_DAYS = 2
#: Starting points per fit. One was never worse on the 81 latest slices;
#: three keep a margin at ~0.1 s a slice.
SVI_STARTS = 3
#: The butterfly check runs this far in log-moneyness past the quoted range.
SVI_BUTTERFLY_MARGIN = 0.1
#: Points on which g(k) and the calendar check are evaluated.
SVI_CHECK_POINTS = 801
#: Points of each fitted curve the API returns for the chart.
SVI_CURVE_POINTS = 101

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
#: Capture days known lost, {YYYY-MM-DD: why}: scripts/precompute_surfaces.py
#: --days stops naming them. yfinance serves only today's chain, so these are
#: permanent gaps in the archive.
OPTION_CAPTURE_DAYS_LOST = {
    "2026-10-09": (
        "both runs crashed writing the first ticker: Yahoo sent inTheMoney mixing "
        "False and 0.0 (fixed 2026-10-13)"
    ),
    "2026-10-21": (
        "both runs failed every ticker at the trading-day check: yfinance's "
        "history(period='5d') raised TypeError ('NoneType' object is not "
        "subscriptable); 2026-10-20 and 2026-10-22 captured normally"
    ),
}

#: Expiries further out than this many calendar days are not fetched.
OPTION_CAPTURE_MAX_DTE = 120

#: A ticker's whole capture below this many contracts is a failed read, not a
#: thin chain — the smallest here (TQQQ) returns hundreds. Catches the SHAPE
#: failure (Yahoo returning an empty or truncated chain), not market activity.
OPTION_CAPTURE_MIN_ROWS = 20

#: data/ is gitignored. This directory is the asset — see the as-built for the
#: fact that it currently has exactly one copy.
OPTION_CHAIN_ARCHIVE_DIR = ROOT_DIR / "data" / "option_chains"
#: The payoff page's default expiry (decision 54): the one nearest this many
#: calendar days after the capture, as the pricer's default.
OPTION_QUOTES_TARGET_DAYS = 30
#: The curve before expiry (decision 55): spots on the grid between the
#: request's low and high, before the strikes and the reference spot are added.
OPTION_POSITION_CURVE_POINTS = 121
#: Most legs one position-curve request may carry.
OPTION_POSITION_MAX_LEGS = 12

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
QUANT_API_BASE_URL = os.getenv("QUANT_API_BASE_URL", f"http://127.0.0.1:{QUANT_API_PORT}")
QUANT_USE_API = os.getenv("QUANT_USE_API", "1").lower() in {"1", "true", "yes"}
QUANT_API_TIMEOUT_SECONDS = float(os.getenv("QUANT_API_TIMEOUT_SECONDS", "30"))

# --- Pipeline Configuration ---
DEFAULT_START_DATE = "2020-01-01"
PIPELINE_SCRIPT_PATH = ROOT_DIR / "cli" / "run_pipeline.py"

# --- Caching Configuration ---
CACHE_DIR = ROOT_DIR / ".cache"
CACHE_EXPIRY_HOURS = 24  # Default cache expiry

# --- Headline archive (decision 57 B, research/news-archive-2026-10-22.md) ---
#: Stories asked per search. 50 is Yahoo's ceiling, and asking for more is
#: worse than capped: AMCR returned 50 at 50 and NOTHING at 100 or 300.
NEWS_ARCHIVE_PER_SYMBOL = 50
#: A search returning at least this many is a full page: older stories
#: exist that it did not return. Yahoo often returns 49 of 50 (AAPL, NVDA).
NEWS_ARCHIVE_FULL_PAGE = 45
#: At 06:00 an empty answer is retried this many times, a pause apart: 42 of
#: 51 empties on 2026-10-22 had stories a minute later. The hourly pass does
#: not retry within a run; an empty page is due the next hour (step 2: later
#: that day 0 of 23 changed within a minute, 5 recovered within the hour).
NEWS_ARCHIVE_EMPTY_RETRIES = 2
NEWS_ARCHIVE_RETRY_PAUSE_SECONDS = 60
NEWS_ARCHIVE_WORKERS = 5
#: The hourly pass's cadence. A symbol is refetched when its last page would
#: be outrun before the NEXT pass: NVDA's 50 stories spanned 1.8 hours.
NEWS_ARCHIVE_HOURLY_INTERVAL_HOURS = 1.0
#: A page counts as FULL only when at least this share of it is tagged with
#: the symbol (decision 57 B step 2). A term matching no quote returns
#: Yahoo's generic feed, ~48 stories of other tickers from the last ~3 hours;
#: KEY's ticker page had 1 of 50. Refetching such a page hourly stores noise,
#: and calling it truncated claims a loss that did not happen.
NEWS_ARCHIVE_OWN_SHARE = 0.5
#: Equities whose ticker is a word are ALSO searched by a name, each term
#: MEASURED to return the company's own tagged stories (2026-10-22,
#: research/news-archive-gaps-2026-10-22.md: ticker page -> this term, tagged
#: of returned). No automatic rule from the SEC title works: "HASBRO, INC"
#: lands on the generic feed, and stripping "Corp" turns "POOL CORP" back into
#: a word. A claim, like CRYPTO_PROVIDER_SYMBOLS: the run prints any listed
#: term that comes back empty or mostly someone else's.
NEWS_SEARCH_TERMS = {
    "ALL": "ALLSTATE CORP",  # 2/50 -> 50/50
    "AMP": "AMERIPRISE FINANCIAL INC",  # 2/50 -> 50/50
    "APP": "AppLovin Corp",  # 16/48 -> 50/50
    "CMS": "CMS ENERGY CORP",  # 21/50 -> 50/50
    "COST": "COSTCO WHOLESALE CORP",  # 10/49 -> 48/48
    "DE": "DEERE & CO",  # 0/48 -> 50/50
    "DOW": "DOW INC",  # 16/50 -> 42/50
    "EQT": "EQT Corp",  # 23/50 -> 50/50
    "ESS": "ESSEX PROPERTY",  # empty -> 44/44
    "FAST": "FASTENAL CO",  # 8/50 -> 50/50
    "FIX": "COMFORT SYSTEMS USA INC",  # 4/50 -> 50/50
    "FOX": "Fox Corp",  # 10/49 -> 20/48
    "GEN": "Gen Digital Inc",  # 0/50 -> 34/42
    "HAS": "HASBRO",  # 2/50 -> 50/50 ("HASBRO, INC": generic feed)
    "KEY": "KEYCORP",  # 1/50 -> 50/50
    "KKR": "KKR & Co. Inc",  # 17/50 -> 50/50
    "LOW": "LOWES COMPANIES INC",  # 3/50 -> 49/49
    "NOW": "ServiceNow",  # 5/49 -> 49/49 ("ServiceNow, Inc": generic feed)
    "POOL": "POOL CORP",  # 13/47 -> 30/30
    "RMD": "RESMED INC",  # 15/50 -> 50/50
    "TAP": "MOLSON COORS BEVERAGE CO",  # 15/48 -> 45/50
    "TEAM": "Atlassian Corp",  # 4/50 -> 50/50
    "TECH": "BIO-TECHNE Corp",  # 0/48 -> 36/36
    "WELL": "WELLTOWER",  # 11/48 -> 50/50
}
#: Ticker pages mostly about others, reviewed and left without a term; the
#: run's WORD TICKER? line skips them. ARE, COO and KIM are NOT here: no term
#: found yet, so they keep being reported.
NEWS_TICKER_PAGES_REVIEWED = {
    "GOOGL": "Yahoo tags Alphabet's stories GOOG, archived under GOOG",
    "NWSA": "Yahoo tags News Corp's stories NWS, archived under NWS",
    "VYLR": "the name 'Vylor' found the same 5 stories as the ticker",
}
