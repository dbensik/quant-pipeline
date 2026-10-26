# Headline archive gaps: empty answers and word tickers (decision 57 B, step 2)

**Status: measured 2026-10-22 about 15:00–15:50 New York time; built the same day on Danny's word ("go on 57 B step 2"). See Built at the end.**

There are two gaps in the archive built earlier today (`research/news-archive-2026-10-22.md`):

- **(A)** symbols that came back empty;
- **(B)** tickers that are ordinary words, whose search fills with other companies' stories.

Both changes touch the same code (`due()`, `last_fetches`, and what a run searches), so they are proposed together and would be built once.

## (A) Empty answers: Yahoo's answer sticks for minutes, not seconds

**Measured:**
- The 23 symbols empty in the 14:55 run were searched again 3 times, 30 s apart, at about 15:45.
- Within those three rounds, **no answer changed**. A term's answer stays the same for at least a minute.
- Between 14:55 and 15:45, **5 of the 23 had recovered**:

  | symbol | stories now (tagged with the symbol) |
  |---|---|
  | COP | 50 (49) |
  | NWSA | 50 (19) |
  | O | 50 (50) |
  | OMC | 46 (46) |
  | SBAC | 50 (50) |

- **Still empty:** CASY, NWS, PLD and 15 coins. Most of the coins are liquid-staking or wrapped tokens (BNSOL, EZETH, JITOSOL, RETH, WEETH…) that likely have no news at all. VET returns 48 stories, none tagged VET-USD (see the generic feed, below).
- **Answers also flip the other way.** At 14:51, `BTC` returned 47 stories (43 tagged BTC-USD) and `ADA` returned 50 (26). At about 15:47 both returned **0**.
- **Not reconciled:** the morning measurement recovered 42 of 51 empties with a retry a minute later, but today none recovered after 30 s. The 06:00 retries are left as they are.

**The rule I offered is refuted.** "Retry empties that have returned stories before" would skip CASY, NWS and PLD: each has only one fetch in the archive, and it was empty.

**Proposed (A):**
- The hourly pass refetches every symbol whose newest successful fetch was empty. That is up to about 23 searches an hour, about 5 s.
- The hourly pass makes **no in-run retries** (`retries=0` on `--due`). Otherwise each hour would wait 2 × 60 s pauses that don't help, turning a 10 s job into 2.5 min.

## (B) Word tickers: the company name finds the stories, but not by any automatic rule

### Equities

The candidates are the 30 listed equities whose stored stories carry their own ticker less than half the time. DAY, EA and K are delisted, so the archive skips them. Each was searched by its ticker, by its SEC title (`metadata.cik_sec_title`, with "/NEW/" removed), and by that title with "Inc", "Corp", "Co" and similar removed.

| symbol | ticker: tagged / returned, span | best name term: tagged / returned, span |
|---|---|---|
| DE | 0/48, 4 h | DEERE & CO: 50/50, 391 h |
| KEY | 1/50, 7 h | KEYCORP: 50/50, 534 h |
| ALL | 2/50, 22 h | ALLSTATE CORP: 50/50, 670 h |
| AMP | 2/50, 53 h | AMERIPRISE FINANCIAL: 50/50, 831 h |
| LOW | 3/50, 30 h | LOWES COMPANIES INC: 49/49, 412 h |
| HAS | 2/50, 10 h | HASBRO: 50/50, 89 h |
| NOW | 5/49, 7 h | ServiceNow: 49/49, 169 h |
| GEN | 0/50, 171 h | Gen Digital Inc: 34/42 |
| TECH | 0/48, 9 h | BIO-TECHNE: 36/41 |
| FIX | 4/50 | COMFORT SYSTEMS USA: 50/50 |
| TEAM | 4/50 | Atlassian: 50/50 |
| FAST | 8/50 | FASTENAL: 49/50 |
| COST | 10/49, 17 h | COSTCO WHOLESALE: 48/48, 145 h |
| WELL | 11/48 | WELLTOWER: 50/50 |
| ESS | (empty) | ESSEX PROPERTY: 44/44 |
| APP, CMS, RMD, TAP | 15–21 of 48–50 | full name: 45–50 of 50 |
| KKR, EQT, DOW, POOL | 13–23 of 47–50 | the FULL title (KKR & Co. Inc, EQT Corp, DOW INC, POOL CORP): 30–50 |
| FOX | 10/49 | Fox Corp: 20/48 |
| ARE, COO, KIM | | no term found today (empty, or the generic feed) |
| VYLR | 5/32 | the name adds nothing |
| GOOGL | | not a word ticker: its stories are tagged GOOG, which is archived |

**About 25 names go from a handful of their own stories to a full page,** and the page reaches back weeks instead of hours.

**No automatic rule from the SEC title works:**
- **The full title** can land on the generic feed (below): "HASBRO, INC", "ServiceNow, Inc", "COOPER COMPANIES, INC" and "ESSEX PROPERTY TRUST, INC" each returned about 48 untagged stories spanning about 3 h.
- **Removing the suffix** turns KKR, DOW, POOL, EQT and Fox back into the ticker or a word, and loses what the full title found.

### Coins

Each of 110 coins was searched by its base symbol (as today) and by `metadata.verified_name`. That field is filled for 109 of them; the morning measurement used the empty `metadata.name`, so that result was void.

| search | coins with tagged news | tagged stories |
|---|---|---|
| base symbol (today) | 34 | 349 |
| verified name | 38 | 635 |
| **both, combined** | **43** | **749** |

- **The name helps most for coins whose symbol is a word:** Bitcoin 43 (BTC 0 at that moment), Cardano 50 (ADA 0), Solana 48 (SOL 6), Chainlink 40 (LINK 5), Litecoin 11 (LTC 0), Dogecoin 44 (DOGE 14).
- **The two searches only partly overlap:** for ETH, the name found 38 stories the base symbol did not. So both searches are kept, not one replacing the other.

### The generic feed

A term that matches nothing returns about 48 stories tagged with other tickers, all from the last ~3 hours. It is NOT empty, so today it counts as a full page:

- `due()` sends it back to the hourly pass;
- the archive records it as truncated, though no story about the company was missed.

Today's full pages that the hourly pass is due to refetch, by how long a full page lasts:

| span | symbol | tagged with the symbol |
|---|---|---|
| 1.9 h | NVDA | 49 |
| 3.3 h | META | 48 |
| 3.4 h | DE | 1 |
| 5.6 h | ARE | 0 |
| 6.3 h | NOW | 6 |
| 6.4 h | KEY | 1 |
| 7.8 h | AMZN, GOOG, MSFT | 48–50 |
| 7.8 h | GOOGL | 3 |
| 8.4 h | FDS | 50 |
| 9.1 h | IBM | 47 |
| 10.7 h | HAS | 3 |
| 12.0 h | LRCX | 49 |

DE, ARE, NOW, KEY and HAS are full of other companies' news, so refetching them hourly stores mostly noise. GOOGL is not a problem: its stories arrive under GOOG.

## Proposed (B)

1. **Coins: always search both terms,** the base symbol and `verified_name`. That is 110 more searches per run, about 10 s at 06:00. A coin is due hourly if either page would be outrun.
2. **Equities: an explicit list in settings, `NEWS_SEARCH_TERMS = {symbol: term}`.** Each entry is a term measured to return the company's own tagged stories, as `CRYPTO_PROVIDER_SYMBOLS` records measured tickers.
   - The list starts with the ~25 names above, each term the best one from this measurement.
   - A listed symbol is searched by its ticker AND its term, so nothing found today is lost.
   - ARE, COO and KIM stay off the list until a term is found.
     2026-10-25: found. 'Alexandria Real Estate' 50/50, 'Cooper Companies'
     50/50, 'Kimco Realty' 45/50. FOX's 'Fox Corp' stays: 32 of its 48 stories
     are tagged FOXA, the same company, so its WEAK TERM line is expected.
3. **A page mostly about others is not full.** Store the number of stories tagged with the symbol (`own_tagged`) on each `news_fetches` row; this is the only schema change, a new nullable column.
   - `due()` and `window_truncated` count a page as full only when at least half of it is the symbol's own stories.
   - This stops the hourly refetches of DE, ARE, NOW, KEY and HAS, and the false truncation records on them.
4. **Each search term gets its own coverage history.** `last_fetches` keys by (symbol, search term), using `news_fetches.search_term`, which already exists. A ticker page and a name page then each get their own hourly decision.
5. **New word tickers are reported, not fixed.** The run prints `WORD TICKER?` for an unlisted equity whose full page has fewer than half its own stories. It is a printed line, not a flag that pages anyone. Adding a name to the list stays your call.

**Not proposed:**
- deriving terms automatically from SEC titles (refuted above);
- changing the 06:00 retries;
- FinBERT scoring.

## Estimate

About 2.5–3 h of agent time for (A) and (B) together. That covers:

- one migration (`own_tagged`);
- the keying change and the new list;
- unit tests, plus integration tests on the ZZNEWS symbols;
- mutants on a copy;
- a full run at the end.

It assumes the measured terms still return the same stories when the run is checked. Yahoo's answers moved within the hour, so the run will name any term that comes back empty.

## Built (2026-10-22, about 16:15–16:25)

Built as proposed, (A) and (B) items 1–5.

- **Code:**
  - `core/news_archive.py`:
    - `search_terms` lists every term a symbol is searched by.
    - `own_tagged` and `is_full` are new. `window_truncated` and `due()` judge fullness with `is_full`, and `due()` also returns an empty last answer.
    - `last_fetches` is keyed by (symbol, term).
  - `scripts/archive_news.py`:
    - one search per term;
    - no in-run retries on `--due`;
    - prints `WORD TICKER?` and `WEAK TERM` lines.
  - Settings: `NEWS_ARCHIVE_OWN_SHARE`, `NEWS_SEARCH_TERMS` (24 names), and `NEWS_TICKER_PAGES_REVIEWED` (GOOGL, NWSA, VYLR).
  - Migration `0013_news_fetch_own_tagged`: one nullable column, plus an index on (symbol, term, time). Rows written before it have `own_tagged` NULL, which `is_full` reads as "judge by size", as before.
- **First run, `--due` at about 16:18:**
  - 138 searches in 13 s, with no retry pauses; 0 failed; 1,842 new symbol–story rows.
  - **Name searches:**
    - equities: 24 searches, 938 of the 951 stories tagged with their symbol;
    - coins: 91 searches, 436 of 1,013 tagged.
  - **Empty pages retried:** CASY, COP, O and OMC came back with 46–50 stories each.
  - **67 empty**, including:
    - COST, DOW, FIX and FOX's terms (each returned stories 40 minutes earlier, so they are printed as WEAK TERM);
    - NWS, NWSA, PLD and SBAC, again.

    All of them are due the next hour, which is this step's point.
- **Found and fixed while building: an empty answer hid losses.**
  - The baseline for truncation was the newest fetch that didn't fail, and an empty answer counts as one.
  - Example: stories at 05:00, empty at 06:00, then a full page at 07:00 reaching back only to 05:12. That page was compared with 06:00, so the 05:00–05:12 loss was not recorded.
  - The problem existed before this step, but hourly empty answers make it common.
  - Now `LastFetch.stories_fetched_at` carries the newest fetch that returned stories, and truncation is measured from it. `due()` still treats an empty answer as the newest.
- **Beyond the proposal:**
  - an index on `news_fetches` (symbol, term, time), for the per-term lookup;
  - `NEWS_TICKER_PAGES_REVIEWED`, which stops GOOGL, NWSA and VYLR being reported every day;
  - the `WEAK TERM` line.
- **Cost, corrected:** the proposal said up to ~23 searches an hour; the real number is about 70.
  - At 16:23, 68 of 795 pages were due for the 17:01 pass, 67 of them empty. About 45 of those are coin names that likely never return (e.g. 'Binance Staked SOL'); the rest are base symbols and tickers.
  - That is about 7 s an hour, and about 1,700 `news_fetches` rows a day recording an empty answer, roughly 3x the log's growth.
  - Still cheap. If the rows matter, the next step would be to stop refetching a page that has never returned stories after N days.
- **Tests:**
  - 39 unit tests and 6 integration tests for the archive. The full suite: 1,948 passed; integration: 90 passed.
  - **Mutants:** 13, on a copy of the repo, all caught. These 10, plus 3 on the truncation baseline:
    - fullness ignoring the own-tag share;
    - an empty answer not due;
    - no second term;
    - last fetch keyed by symbol only;
    - every story counted as the symbol's own;
    - `own_tagged` not stored;
    - truncation ignoring the own-tag share;
    - an equity searched by any name;
    - a listed word ticker still reported;
    - an empty term not reported weak.
- **Not tested by a unit test:** the `--due` run's `retries=0`. The 13 s run above, with 67 empty answers, shows no 60 s pauses were taken.
