# A headline archive (decision 57 B, step 1): measured and built 2026-10-22

**Status: built 2026-10-22 on Danny's word ("go on archive with hourly"); see Built at the end.**

## Why now

Yahoo's search only returns recent stories: the newest 50 for a ticker, and
for the busiest names those 50 cover a few hours. Whatever we don't store
goes out of that window and can't be fetched again, just like a missed
universe snapshot.

Weighting sources and authors by how accurate they have been needs months of
stored headlines. Sentiment scoring (FinBERT) can be run later on the stored
text, so this step stores the stories only and scores nothing.

## Measured

There were three passes over the registry on 2026-10-22, at about 14:40 New
York time:

- **Depth:** 5 symbols, each asked for 10, 50, 100 and 300 stories.
- **Full registry:** all 695 registered symbols (546 equities, 35 ETFs, 114
  crypto), 50 stories each, 5 threads. The empty answers were then retried
  twice, a minute apart.
- **Crypto:** each coin searched by its base symbol and by its stored name.

### 50 is the ceiling, and asking for more returns nothing

| symbol | asked 10 | asked 50 | asked 100 | asked 300 |
|---|---|---|---|---|
| AAPL | 10 | 49 (1.0 d) | 49 | 49 |
| SPY | 10 | 50 (1.2 d) | 50 | 50 |
| KO | 10 | 50 (2.3 d) | 50 | 50 |
| AMCR | 10 | 50 (71 d) | **0** | **0** |
| BTC-USD | 10 | 12 (73 d) | 12 | 12 |

The number in brackets is the age of the oldest story returned.

Asking for more than 50 is not just capped: for AMCR it returned an empty
answer. So the archive must ask for exactly 50, and a test should pin that.

### The full registry, 50 stories each

- **Speed:** 66 s wall clock for all 695 symbols. No errors. A request took
  0.51 s at the median and 0.96 s at most.
- **Order and keys:** all 531 symbols with two or more dated stories returned
  them newest first. Every story had a Yahoo id and a publish time.
- **Volume:** 18,676 unique stories carried the ticker they were found under,
  in 23,089 symbol–story pairs (one story often names several tickers). Of
  those, 2,184 unique stories were published in the last 24 hours, which is
  about 800k stories a year.
- **History on the first run:** the oldest story returned per symbol was
  2.7 days old at the 10th percentile, 22 days at the median and 83 days at
  the 90th. So the first run gets roughly three weeks of history for the
  median name, once.

| class | symbols | empty on the 1st pass | empty after 2 retries | carrying their own ticker |
|---|---|---|---|---|
| equity | 546 | 49 | 9 | 22,169 |
| etf | 35 | 2 | (included above) | 909 |
| crypto | 114 | 111 | — | 11 |

**Empty answers are mostly transient.** 42 of the 51 empty equity and ETF
answers came back with stories on a retry. The 9 that stayed empty are ATO,
CTRA, EPAM, HOLX, NWSA, PKG, PLD, WAT and WBA. NWSA's stories arrive under
NWS. This is why an empty answer is never cached today.

### The 50 newest cover less than a day for the busiest names

The window is the age of the oldest story among the 50 returned. It is
counted only for symbols that returned 45 or more stories, since Yahoo often
returns 49.

| window under | symbols |
|---|---|
| 6 h | 4 |
| 12 h | 14 |
| 24 h | 18 |
| 48 h | 35 |

The 18 symbols under 24 hours:

| symbol | window (h) | symbol | window (h) |
|---|---|---|---|
| NVDA | 1.8 | FDS | 8.2 |
| DE | 3.1 | IBM | 8.9 |
| META | 3.2 | HAS | 11.3 |
| ARE | 5.6 | LRCX | 11.8 |
| NOW | 6.1 | TECH | 12.3 |
| KEY | 6.2 | COST | 21.5 |
| AMZN | 7.6 | ALL | 22.3 |
| GOOG / GOOGL | 7.6 | AAPL | 23.8 |
| MSFT | 7.6 | | |

About 11 of these are genuinely busy names: NVDA, META, AMZN, GOOG/GOOGL,
MSFT, FDS, IBM, LRCX, COST and AAPL. The others are short only because their
ticker is an ordinary word (see the next section).

**A fetch once a day at 06:00 would keep only a few hours of the mega-caps'
news,** and those are the names with the most coverage to score. NVDA's
tagged stories arrived at roughly 27 an hour.

### Tickers that are ordinary words get crowded out

Search matches text, so a ticker that is also a word fills its 50 results
with unrelated stories. These symbols returned 50 stories, but only a few
carried their own ticker:

| symbol | stories carrying the ticker (of 50) |
|---|---|
| DAY | 0 |
| EA | 0 |
| GEN | 0 |
| K | 0 |
| KEY | 1 |
| LOW | 1 |
| ALL | 2 |
| AMP | 2 |
| HAS | 3 |
| FIX | 4 |
| NOW | 6 |
| FAST | 8 |

The archive would hold almost nothing for these names. The fix would be to
also search by company name: `metadata.verified_name` or `cik_sec_title` is
on the equity rows. That is not part of this proposal.

### Crypto needs the base symbol

- **Searching `ETH-USD`:** 0 stories.
- **Searching `ETH`:** 50 stories, 49 of them tagged `ETH-USD`.

Across the 114 coins:

| search term | coins with stories | tagged stories |
|---|---|---|
| `X-USD` (as today) | 3 | 11 |
| base symbol (`ETH`) | 34 | 406 |

Yahoo still tags these stories with the `-USD` symbol, so the existing
relevance rule works unchanged.

The search by coin name measured nothing. `metadata.name` is empty for all
114 coins, so the "0 by name" result is void, not negative. A one-off manual
check found that "Ethereum" and "Solana" do return stories tagged `ETH-USD`
and `SOL-USD`.

## Proposed (approved and built as below)

1. **Tables** (one migration, nothing ever overwritten):
   - `news_stories`: Yahoo's story id is the key. It stores the title,
     summary, URL, publisher, `published_at` (Yahoo's time), `first_seen_at`
     (our first fetch) and Yahoo's related-ticker tags as served.
     `first_seen_at` matters for scoring accuracy: it is when we could have
     known the story, which can differ from the published time.
   - `news_story_symbols`: one row per story and symbol it was found under,
     with the search term used (`ETH` for `ETH-USD`) and the first time it
     was seen there. Unique on (story, symbol).
   - `news_fetches`: one row per symbol per run, as the coverage log. It
     records how many stories were returned, the oldest one's time, the
     number of empty attempts, and `window_truncated`.
     - `window_truncated` means the oldest story returned is newer than this
       symbol's previous fetch, so stories in between were missed.
     - Loss is counted there, never trimmed silently.
2. **Stored unfiltered; the relevance rule is applied when reading.** The rule
   already has two unbuilt amendments (a title match, and the crypto base
   symbol for the Research page). Storing every story as served, with its
   tags, lets a later rule apply to the past too. This is the same idea as
   storing prices as served and adjusting them on read. It costs about 10%
   more rows: 24,175 stories returned against 22,169 that carry their own
   ticker.
3. **Fetching:**
   - exactly 50 stories per request, 5 threads;
   - crypto searched by its base symbol;
   - an empty answer retried twice, a minute apart;
   - a symbol still empty after that is logged as no news, which is a flag,
     not a failure.
4. **When it runs:**
   - **(a)** A full pass of all 695 symbols as a step in the 06:00 job,
     before the backup, so the new tables go into the proven dump. It takes
     about 70 s, not counting the retry pauses (196 s in all on the first run).
   - **(b) Recommended:** also an hourly pass, through a new launchd job.
     Each hour it refetches only the symbols whose last window was shorter
     than the time since their last fetch: about 15–20 symbols, around 10 s
     an hour.
     - Without (b), the busy names keep a few hours of each day and
       `window_truncated` records the loss.
     - (b) adds a second scheduled job, which is persistent configuration,
       so it needs your yes.
5. **Failures:** if Yahoo fails for every symbol (no `news` field), the step
   exits non-zero and posts a notification, like the other checks. It never
   stops the backup.
6. **Not in this step:** FinBERT scores, the company-name search for word
   tickers, and any change to the Research page.

**Build estimate:**
- (1)–(5) with the 06:00 step only: about 3–4 h of agent time, including
  tests that run without the network and mutants on a copy.
- The hourly job (4b): about another 45 min.

Both estimates assume no surprises in the migration.

## Built (2026-10-22)

- **Code:**
  - `core/news_archive.py`: the rules and the writes. The rules (search term,
    retries, window truncation, which symbols the hourly pass picks) are pure
    functions.
  - `core/news.py`: Yahoo's item shape, moved unchanged out of
    `api/upstream.py` so core does not import api.
  - `scripts/archive_news.py`: the command-line entry point.
  - Migration `0012_news_archive`: `news_stories`, `news_story_symbols`,
    `news_fetches`.
  - Settings `NEWS_ARCHIVE_*` in `config/settings.py`.
- **Yahoo serves no summary on search,** so none is stored. `story_type` (STORY
  or VIDEO) is stored instead.
- **06:00:** a "headline archive" step just before the backup.
  - Some symbols failing after retries is the flag `news-archive`.
  - Every search failing is a failure.
- **Hourly:** launchd job `com.dbensik.quant-pipeline.news-hourly`, wrapper
  `scripts/cron/news_hourly.sh`, log `logs/news_hourly.log`. It runs
  `archive_news.py --due` and notifies only when a run stores nothing (exit 2).
- **First full run, 2026-10-22 ~14:55 New York time:**
  - 680 symbols searched (delisted ones are skipped) in 196 s, including two
    60 s retry pauses.
  - 0 failed.
  - 23 still empty after retries, including CASY, COP, NWS, NWSA, O, OMC,
    PLD, SBAC and 15 coins.
  - 23,681 stories and 29,371 symbol–story rows; 25,095 of those rows carry
    the symbol's own tag. 2,623 rows are crypto.
  - 15 MB for the three tables.
  - An immediate `--due` pass found no symbol due, which is correct: NVDA's
    1.9 h span had not yet come within an hour of being outrun.
- **Tests:**
  - 24 unit tests and 3 integration tests (real database).
  - 7 mutants run on a copy of the repo, all caught: truncation ignoring page
    size; the hourly rule ignoring the next pass; no retries; errors not
    retried; crypto searched as `X-USD`; a stored story overwritten; a short
    page made due.
  - The short-page test first let its mutant survive (its span was too long
    to be due anyway) and was rewritten.
- **Growth:** about 1.4 MB a day at this rate (~0.5 GB a year) before the dump
  compresses it. It sits in the daily backup and its restore row-count check.
- **Known gap:** a symbol still empty after its retries waits for the next
  06:00 run, because the hourly pass refetches only full pages. On the first
  run that was 23 of 680 symbols, against 9 of 695 in the measurement. NWS
  returned stories in the measurement 20 minutes earlier, which suggests
  Yahoo is flaky rather than these symbols having no news. Unbuilt picks: a
  third retry or a longer pause, or an hourly retry of empties that have
  returned stories before.
