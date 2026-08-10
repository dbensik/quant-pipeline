# Delisting errors — investigation findings (2026-08-09)

## Verdict

The 11 ERROR symbols split **8 genuinely delisted / 3 wrongly flagged renames**.
`looks_delisted` is right about 8 and wrong about 3. The docstring at
`core/corporate_actions.py:117` calls all eleven "acquired or taken-private" —
that claim is false for BK, FI, MMC.

| Ticker | Reality | Evidence (per-ticker, all read directly) | Flag correct? |
|---|---|---|---|
| ANSS | delisted — Synopsys, completed **2025-07-17** | Form 25-NSE 2025-07-17; Form 15-12G 2025-07-29; absent from nasdaqlisted.txt (2026-08-07) | yes |
| HES  | delisted — Chevron, completed **2025-07-18** | Form 25-NSE acc. 0000876661-25-000523, filed+effective 2025-07-18; last trade 2025-07-17; Nasdaq API "Symbol not exists" while controls CVX + HESM serve live quotes | yes |
| WBA  | delisted, **2025-08-28** | Form 25-NSE acc. 0001354457-25-000854 (filed *by* Nasdaq ⇒ executed); Form 15-12G 2025-09-08; no filings since | yes |
| IPG  | delisted — closing **2025-11-26** | Form 8-K acc. 0001193125-25-300704, Item 2.01 Introductory Note read directly | yes |
| K    | delisted, **2025-12-11** | Form 25-NSE acc. 0000876661-25-000958, accepted 2025-12-11 by NYSE; absent from both otherlisted.txt and nasdaqlisted.txt | yes |
| DAY  | delisted, effective **2026-02-04** | Form 25-NSE (NYSE's own filing, CIK 1725057); six S-8 POS on 2026-02-04 | yes |
| HOLX | delisted — merger closed **2026-04-07** | Nasdaq Trader Equity Corporate Actions Alert ECA2026-222 "UPDATED: Merger closed" | yes |
| CTRA | delisted — Devon, **2026-05-19** | Form 15-12G 2026-05-19, **1 holder of record** ⇒ no float, closes the OTC/grey-market gap | yes |
| **BK**  | **RENAMED → BNY**, NYSE, eff. 2026-05-21 | 10-Q filed 2026-07-31 lists "BNY \| NYSE"; same CIK 0001390777, same file no. 001-35651, "Former name: Not Applicable" | **NO** |
| **FI**  | **RENAMED → FISV**, NYSE→Nasdaq, eff. 2025-11-11 | Nasdaq Trader DTN2025-32; **CUSIP unchanged 337738108** | **NO** |
| **MMC** | **RENAMED → MRSH**, NYSE | Yahoo MMC 404s; MRSH live; vendors alias MMC→MRSH | **NO** |

**Consistency check that costs nothing and catches pattern-matching.** All 11
hold 1390 gapless bars through 2025-07-15, so no ticker in this cohort can have
died *before* that date. Every completion date above is later — HES by three
days. Any verdict citing an earlier close would be self-refuting. (My own first
recall put Chevron/Hess in 2024; that was the 2023-10-22 merger *agreement*,
and the verifier flagged the announcement-vs-completion trap explicitly.)

All 11 adversarial verifiers agreed with their researcher, and each retrieved
SEC filings independently via curl with a compliant User-Agent — sec.gov 403s
the WebFetch user agent, which had blocked several first-pass researchers.
Caveat on the agreement rate: researcher and verifier can share one failure
mode (name search finds no successor → conclude delisted), so the primary
filings above, not the 11/11 tally, are what carries the eight.

Renames independently
confirmed by a constant-ratio test on the price series (rename ⇒ constant
multiplicative offset = dividend re-adjustment):

| pair | ratio spread over 20d | verdict |
|---|---|---|
| BK↔BNY | 1.65e-07 (ratio 1.022846) | same instrument |
| FI↔FISV | 0.00e+00 (ratio 1.000000) | same instrument (Fiserv pays no dividend) |
| MMC↔MRSH | 1.67e-07 (ratio 1.024923) | same instrument |
| BK↔JPM (control) | 2.16e-02 | different |
| MMC↔AON (control) | 1.28e-02 | different |

Five orders of magnitude separation.

## Live damage

`BNY`, `FISV`, `MRSH` are **not in the `assets` registry**. BK/FI/MMC hold 1390
bars ending 2025-07-15 and carry `delisted_at`. So three live S&P 500
constituents have **13 months of missing bars and are marked dead.** Any
screen or backtest over that window silently omits them — a survivorship
bias of exactly the kind the point-in-time membership work exists to prevent.

## Why everything froze on one date

2025-07-15 is the **bulk-load date**, not 11 delisting dates. The old crontab
entry pointed at a Google Drive path that no longer exists, so no ingest ran
between 2025-07-15 and today. Today's first working run advanced every
still-resolvable symbol to 2026-08-07 and failed on the 11 that died or were
renamed during the 13-month gap. `DEFAULT_BACKFILL_START` is 2015-01-01, so the
shared `2025-07-16` resume point means `newest_stored = 2025-07-15` for all 11.

## What an empty fetch actually proves

**Only that Yahoo has no record keyed to that exact string today.** Both my
initial inferences were wrong. The control experiment:

| Symbol | Reality | yfinance 5y | Yahoo /v1/search exact hit |
|---|---|---|---|
| FRCB | seized 2023, $200→$0.0004 shell | **1255 rows** | yes (OTCPK) |
| BK | live, current S&P 500 member | **0 rows** | **no** |
| TWTR, ATVI, VMW, SIVBQ | long gone | 0 rows | no |
| AAPL, MSFT, JPM | live | 1255 rows | yes |

FRCB proves Yahoo does *not* purge history when a company dies. BK proves an
empty result does *not* mean the company died. SIVBQ vs FRCB — same event
class, opposite outcomes. Availability tracks whether the **symbol key**
exists, not what happened to the issuer. `looks_delisted` infers a corporate
fact from a string-lookup miss; those are different things.

Yahoo `/v8/finance/chart/BK` returns HTTP 404 byte-identical to TWTR/ATVI, so
the resolution failure is upstream at Yahoo, not a yfinance bug.

## Secondary findings

**No skip for already-dead assets.** `cli/run_pipeline.py:56` does an
unfiltered `SELECT symbol FROM assets` (616 rows). `ingest_symbols`
(`core/ingest.py:167-198`) has only two skip gates — asset-not-found (172-177)
and already-current (193). `delisted_at` never reaches the function: the domain
`Asset` (`core/models/__init__.py:23-28`) has no such field. So all 14 flagged
assets are re-fetched daily. Note the run still **exits 0** — `outcome.error`
stays None, so these never enter `report.failed`. The ERROR lines come from
yfinance's own logger.

Two constraints on any fix: `core/ingest.py:240` rewrites `delisted_at` to
`now()` every run (so it means "last seen dead", not "first detected"), and
`mark_full_refresh` clears it to NULL (`db/repositories/market_data.py:309`).

**`BSC-USD-USD` is a stale registry row, not a runtime transform.** `assets`
holds both `BSC-USD` (id 87, 2412 bars) and `BSC-USD-USD` (id 88, 0 bars).
Origin: `data_pipeline/crypto_pipeline.py:144` appends `-USD` unconditionally,
while the same class guards correctly at `:44-49`. Fed raw CoinGecko symbols,
only Binance-Peg `bsc-usd` already ended in `-usd`. That path is dead code now
(it takes a `sqlite3.Connection`). Fix is `DELETE FROM assets WHERE symbol =
'BSC-USD-USD'` — 0 references in market_data, universe_membership,
watchlist_symbols, portfolio_trades.

**`PUMP-USD` / `SUSDS-USD`** are correctly formed; Yahoo just has no listing.
They have zero bars, so `looks_delisted` returns False (`newest_bar is None`)
and a `delisted_at IS NULL` filter would *not* silence them.

**Current S&P 500 scrape is clean** — `DynamicUniverse().get_tickers('sp500')`
returns 503 names containing BNY, FISV, MRSH and none of the 11 old tickers.
The stale names survive only in `assets`.

## Recommended fixes (none applied)

1. **Un-flag and remap the three renames** — highest value. Register BNY/FISV/MRSH,
   stitch the pre-rename bars (CUSIP unchanged ⇒ one continuous series, not a
   gap), clear `delisted_at`. Do NOT treat as new listings.
2. **Add a rename map** so this is detectable, not manual. Memory already
   records renames as the open gap in corporate-actions handling.
3. **Skip `delisted_at IS NOT NULL` in `_registered_symbols`** — needs
   `delisted_at` plumbed into the domain `Asset` first.
4. `DELETE FROM assets WHERE symbol = 'BSC-USD-USD'`.
5. Tighten `looks_delisted` — an empty fetch plus a stale bar cannot distinguish
   rename from delisting. A Yahoo `/v1/finance/search` miss combined with a
   successor-ratio test can.
